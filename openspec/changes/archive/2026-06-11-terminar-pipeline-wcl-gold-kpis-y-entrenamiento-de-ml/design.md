# Design: Pipeline WCL → Gold KPIs + ML Training

## Technical Approach

Wrap existing pipeline scripts as thin Dagster `@asset` wrappers. Replace Gold `mode("overwrite")` with merge-by-key. Add dead-letter queue, WCL pagination, and Parquet lookup tables. Build ML feature engineering and training as downstream assets. Ordered as: PR1 (Dagster+incremental), PR3 (hardening), PR2 (ML).

## Architecture Decisions

| Decision | Choice | Rejected | Rationale |
|----------|--------|----------|-----------|
| Orchestrator | Dagster | Airflow, Prefect | Asset graph model matches Medallion; `dagster dev` in seconds; built-in AssetChecks; Python-native |
| Incremental write | Merge by `run_id`/`keystone_run_id` | Overwrite, Append+dedup | Preserves existing data; idempotent; no data loss on partial runs |
| ML model | Linear Regression (ols) | Random Forest, XGBoost | Proposal initially said RF; Linear is interpretable baseline, faster to iterate, coefficient analysis gives direct feature importance |
| ML tracking | MLflow local (file URI) | MLflow server, no tracking | Zero infra; file-based for MVP; migrate to server later |
| Dead-letter storage | Parquet on MinIO `silver/dead_letter/` | DB table, in-memory | Consistent with existing pipeline; queryable via Spark |
| Lookup tables | Parquet in `bronze/lookups/` + hardcoded fallback | DB-only, hardcoded-only | Evolvable: version-controlled Parquet, fallback for bootstrap |
| WCL pagination | Loop `nextPageTimestamp` until null | Single query (current) | WCL GraphQL `events` returns cursor; loop to get all events |

## Data Flow

```
bronze_rio ──→ silver_raiderio ──┐
                                  ├── silver_dungeon_runs ──→ gold_kpis ──→ gold_features ──→ ml_model
bronze_wcl ──→ match_manifest ──┘       │
       │                               └── silver_player_performance ──┘
       └── silver/dead_letter (on WCL errors)

check_minio_state ──→ (validates connectivity before all assets)

bronze/lookups/* ──→ gold_dim_tables (dim_dungeon, dim_affix, dim_spec_role)
```

## PR #1 — Dagster Orchestration + Incremental

### Asset Graph

| Asset | Source | Key | Dependencies |
|-------|--------|-----|-------------|
| `check_minio_state` | MinIO connectivity check | N/A | None |
| `bronze_rio` | Raider.IO API | `keystone_run_id` | `check_minio_state` |
| `bronze_wcl` | WCL API + match manifest | `report_code`+`fight_id` | `check_minio_state`, `match_manifest` |
| `match_manifest` | Fuzzy join script | `rio_run_id` | `bronze_rio` |
| `silver_raiderio` | `SilverPipeline.clean_raiderio()` | `keystone_run_id` | `bronze_rio` |
| `silver_dungeon_runs` | `SilverPipeline.apply_fuzzy_join()` output 1 | `run_id` | `silver_raiderio`, `match_manifest`, `bronze_wcl` |
| `silver_player_performance` | `SilverPipeline.apply_fuzzy_join()` output 2 | `run_id`+`player_name` | same |
| `gold_kpi_death_clock` | `GoldPipeline.compute_kpi_death_clock()` | `run_id`+`tank_name` | `silver_player_performance` |
| `gold_kpi_healer_deficit` | `GoldPipeline.compute_kpi_healer_deficit()` | `run_id`+`healer_name` | `silver_player_performance` |
| `gold_kpi_interrupt_rate` | `GoldPipeline.compute_kpi_interrupt_rate()` | `run_id`+`player_name` | `silver_player_performance` |
| `gold_kpi_synergy` | `GoldPipeline.compute_kpi_synergy()` | `dungeon_id`+`key_level`+`affix_ids`+`comp_signature` | `silver_raiderio` |
| `gold_dim_*` (4 assets) | `GoldPipeline.build_dim_*()` | natural key | `silver_raiderio` or lookup |
| `gold_features` | ML feature view | `run_id` | all `gold_kpi_*` + `silver_dungeon_runs` |
| `ml_model` | scikit-learn train | N/A | `gold_features` |

### Asset Wrappers

Each asset is a thin wrapper calling existing pipeline code. Pattern:

```python
@asset(key_prefix=["orakel"])
def bronze_rio(context: AssetExecutionContext, check_minio_state) -> MetadataValue:
    spark = get_spark_session("bronze_rio")
    try:
        count = ingest_raiderio_runs(RaiderIOClient(), spark, settings.SEASON)
        return MetadataValue.int(count)
    finally:
        spark.stop()
```

### Incremental Merge Pattern

Replace `.write.mode("overwrite").parquet(path)` with:

```python
def merge_write(spark, df, path, merge_key: list[str]):
    if path_exists(spark, path):
        existing = spark.read.parquet(path)
        # Upsert by key: new data wins on conflict
        merged = existing.join(df, on=merge_key, how="full_outer") \
            .select(*[coalesce(df[c], existing[c]).alias(c) for c in df.columns])
        merged.write.mode("overwrite").parquet(path)
    else:
        df.write.mode("append").parquet(path)
```

Match manifest: incremental resumes per tank (existing checkpoint logic in `scripts/match_reports.py`).

### Schedule & Execution

- `daily_schedule`: cron `0 2 * * *`, season from settings
- `check_minio_state`: sensor/asset that verifies MinIO connectivity + bucket exists before downstream
- Partial materialization: WCL assets fail gracefully — `try/except` logs error, writes empty DataFrame, downstream uses Rio-only fallback (already implemented in `gold.py`)

### AssetChecks (Data Quality)

| Asset | Check | Threshold |
|-------|-------|-----------|
| `bronze_rio` | row_count ≥ 1, null % on `keystone_run_id` < 1% | min_rows=1 |
| `silver_raiderio` | row_count ≥ 1, dedup ≤ 10% reduction | max_dedup_ratio=0.1 |
| `silver_dungeon_runs` | row_count ≥ 1, no null `dungeon_id` | min_rows=1 |
| `silver_player_performance` | row_count ≥ 5 | min_rows=5 |
| `gold_kpi_*` | row_count ≥ 1, null % on primary metric < 50% | max_null_ratio=0.5 |
| `gold_features` | row_count ≥ 10, no null `clear_time_seconds` | min_rows=10 |

### File Structure

```
orakel/
  pipeline/
    definitions.py     # Dagster Definitions, assets, schedules, checks
    assets/
      __init__.py
      bronze.py         # bronze_rio, bronze_wcl assets
      silver.py         # silver_* assets
      gold.py           # gold_kpi_*, gold_dim_* assets
      checks.py         # AssetCheck definitions
    io_managers.py      # MinIO S3A IO manager, merge logic
  ml/
    __init__.py
    features.py         # feature engineering
    trainer.py          # model training + evaluation
    predict.py          # prediction function
    schemas.py          # ML feature/target schemas
```

## PR #3 — Operational Hardening

### Dead-Letter Queue

Schema (Parquet in `silver/dead_letter/`):

| Column | Type | Description |
|--------|------|-------------|
| `entity_type` | string | `match_report`, `wcl_events`, `wcl_table` |
| `entity_key` | string | Report code, run ID, etc. |
| `error_type` | string | `auth_error`, `rate_limit`, `timeout`, `parse_error` |
| `error_message` | string | Full traceback/message |
| `payload_snapshot` | string (JSON) | Input that failed |
| `occurred_at` | timestamp | When it failed |
| `retried` | boolean | Whether it was retried |
| `season` | string | Partition column |

On WCL error, asset catches exception, writes to dead-letter, continues. Re-run reads dead-letter, retries once, moves to `retried=true` on success.

### WCL Event Pagination

Current `get_events()` makes one call. New:

```python
def get_events(self, report_code, fight_ids, data_type, start_ts=None):
    # ... query with $startTime filter
    all_events = []
    next_ts = start_ts
    while True:
        result = self.query(query, vars_with_timestamp)
        events = result["data"]["reportData"]["report"]["events"]
        all_events.extend(events["data"])
        next_ts = events.get("nextPageTimestamp")
        if not next_ts:
            break
    return all_events
```

Point budget: ~5 pts per page. Budget check per page. If budget exhausted mid-pagination, return partial + log.

### Lookup Tables

Extract on boot:
- `bronze/lookups/dim_dungeon_timer/` ← `_TWW3_DUNGEONS`
- `bronze/lookups/dim_affix/` ← `_TWW3_AFFIXES`
- `bronze/lookups/dim_spec_role/` ← `_WOW_SPEC_ROLE_MAP`

Loading strategy: `GoldPipeline.build_dim_*` tries Parquet first, falls back to hardcoded dict. Bootstrapped by `scripts/seed_lookups.py`.

## PR #2 — ML Training

### Feature Engineering (`gold_features` asset)

| Feature | Source | Transform |
|---------|--------|-----------|
| `key_level` | `silver_dungeon_runs.key_level` | As-is (int) |
| `dungeon_id` | `silver_dungeon_runs.dungeon_id` | One-hot (8 dungeons) |
| `death_clock_seconds` | `gold_kpi_death_clock` | Log1p (skewed) |
| `deficit_ratio` | `gold_kpi_healer_deficit` | As-is |
| `interrupts_per_minute` | `gold_kpi_interrupt_rate` | As-is |
| `synergy_score` | `gold_kpi_synergy` | As-is |
| `affix_*` (binary flags) | `silver_dungeon_runs.weekly_modifiers` | Multi-hot (16 affixes) |
| `comp_signature` | `silver_dungeon_runs.roster` | Count encoding by role combo |
| **Target**: `clear_time_seconds` | `silver_dungeon_runs.clear_time_ms / 1000` | — |

Join all KPIs + dungeon_runs on `run_id`. Filter null targets. Output: single flat Parquet in `gold/features/`.

### Model Training (`ml_model` asset)

- `sklearn.linear_model.LinearRegression` with `fit_intercept=True`
- Split: temporal 80/20 by `completed_at` (no shuffle — avoid leakage)
- Metrics: MAE, RMSE, R²
- Baseline: `DummyRegressor(strategy="mean")` — compare MAE
- MLflow: `mlflow.log_params()`, `mlflow.log_metrics()`, `mlflow.sklearn.log_model()`
- Tracking URI: `mlflow.set_tracking_uri(f"file://{settings.MINIO_BUCKET}/mlruns")` (local file)

### File Structure

```
orakel/ml/
  features.py    # build_feature_view(spark, season) -> DataFrame
  trainer.py      # train_model(df) -> (model, metrics_dict)
  predict.py      # predict(model, features_df) -> predictions_df
  schemas.py      # feature_columns list, target_column constant
```

## File Changes

| File | Action | Description |
|------|--------|-------------|
| `orakel/pipeline/definitions.py` | Create | Dagster Definitions with asset graph, schedule, checks |
| `orakel/pipeline/assets/bronze.py` | Create | bronze_rio, bronze_wcl, check_minio_state |
| `orakel/pipeline/assets/silver.py` | Create | silver_raiderio, silver_dungeon_runs, silver_player_performance |
| `orakel/pipeline/assets/gold.py` | Create | gold_kpi_*, gold_dim_*, gold_features |
| `orakel/pipeline/assets/checks.py` | Create | AssetCheck definitions |
| `orakel/pipeline/io_managers.py` | Create | merge_write() incremental logic |
| `orakel/pipeline/gold.py` | Modify | Add `merge_write` option, lookup fallback |
| `orakel/clients/warcraftlogs.py` | Modify | Paginate `get_events()` via `nextPageTimestamp` |
| `orakel/models/schemas.py` | Modify | Add dead_letter_schema, ML feature schemas |
| `orakel/ml/features.py` | Create | Feature engineering from Gold |
| `orakel/ml/trainer.py` | Create | Linear Regression training + MLflow logging |
| `orakel/ml/predict.py` | Create | Prediction function |
| `orakel/ml/schemas.py` | Create | Feature column definitions |
| `scripts/seed_lookups.py` | Create | Bootstrap Parquet lookups from hardcoded dicts |
| `pyproject.toml` | Modify | Add dagster, dagster-webserver, mlflow, pandas |

## Testing Strategy

| Layer | What | Approach |
|-------|------|----------|
| Unit | `merge_write()`, dead-letter schema, feature transforms | PySpark fixtures from conftest |
| Unit | Asset wrapper logic (mock Spark, mock API) | Mock `get_spark_session` |
| Unit | Linear Regression trainer | Small pandas DF, assert MAE < baseline |
| Integration | Dagster materialize single asset | `dagster materialize` in-process |
| Integration | End-to-end Bronze→Gold with sample data | MinIO localstack fixtures |

## Migration / Rollout

No data migration required. Gold tables currently use `overwrite` — first Dagster run writes full dataset via merge (equivalent to `overwrite` on first run). Lookup Parquet tables bootstrapped by `scripts/seed_lookups.py` (once). Scripts in `scripts/` remain untouched — backward compatible.

## Open Questions

- [ ] Should `ml_model` asset use `sklearn.linear_model.LinearRegression` or `Ridge` (L2 regularization) as default?
- [ ] Partition strategy for `gold_features` — by `season` only, or also by `key_level`?
- [ ] Dagster IO manager custom class vs. inline `merge_write` in asset body — which is cleaner for MVP?