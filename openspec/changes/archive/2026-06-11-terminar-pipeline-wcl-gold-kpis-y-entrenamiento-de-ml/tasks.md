# Tasks: Terminar Pipeline WCL → Gold KPIs y Entrenamiento de ML

## Review Workload Forecast

| Field | Value |
|-------|-------|
| Estimated changed lines | ~850-1100 |
| 400-line budget risk | High |
| Chained PRs recommended | Yes |
| Suggested split | PR1 → PR3 → PR2 |
| Delivery strategy | ask-on-risk |
| Chain strategy | pending |

Decision needed before apply: Yes
Chained PRs recommended: Yes
Chain strategy: pending
400-line budget risk: High

### Suggested Work Units

| Unit | Goal | Likely PR | Notes |
|------|------|-----------|-------|
| 1 | Dagster orchestration + incremental write | PR1 | ~280-320 lines; foundation for all |
| 2 | Operational hardening: DLQ, WCL pagination, lookups | PR3 | ~250-300 lines; depends on PR1 |
| 3 | ML training: features, trainer, predict | PR2 | ~320-400 lines; depends on PR1+PR3 |

---

## PR1: Dagster Orchestration + Incremental

### Phase 1: IO Manager Foundation

- [x] 1.1 Crear `orakel/pipeline/io_managers.py` con `merge_write()`: lee path existente, hace full_outer join por merge_key, nueva data gana en conflicto. Si path no existe, append. Usar coalesce para nulls.
- [x] 1.2 Crear `MinIOIOManager` (Dagster IO Manager) que envuelve `merge_write()` y usa key_prefix para paths. Define `load_input()` y `handle_output()`.

### Phase 2: Asset Definitions — Bronze

- [x] 2.1 Crear `orakel/pipeline/assets/__init__.py` exportando assets
- [x] 2.2 Crear `orakel/pipeline/assets/bronze.py`: asset `check_minio_state` — usa `settings.MINIO_ENDPOINT`, `settings.MINIO_BUCKET`, `settings.MINIO_ACCESS_KEY`. Lanza `Failure` con mensaje claro si bucket no accesible.
- [x] 2.3 Asset `bronze_rio` — wrapper alrededor de `scripts/ingest_raiderio.py` (llama función existente). Ejecuta dentro de SparkSession con `get_spark_session("bronze_rio")`.
- [x] 2.4 Asset `bronze_wcl` — wrapper alrededor de `scripts/ingest_warcraftlogs.py`. Si falla, retorna DataFrame vacío (no lanza). Loguea warning.
- [x] 2.5 Asset `match_manifest` — wrapper alrededor de `scripts/match_reports.py`. Depende de `bronze_rio`.

### Phase 3: Asset Definitions — Silver

- [x] 3.1 Crear `orakel/pipeline/assets/silver.py`
- [x] 3.2 Asset `silver_raiderio` — wrapper `SilverPipeline.clean_raiderio()` desde `orakel/pipeline/silver.py`
- [x] 3.3 Asset `silver_dungeon_runs` — wrapper `SilverPipeline.apply_fuzzy_join()` output 1 (dungeon_runs). Key: `run_id`. Usa `merge_write`.
- [x] 3.4 Asset `silver_player_performance` — wrapper `SilverPipeline.apply_fuzzy_join()` output 2 (player_performance). Key: `run_id`+`player_name`. Usa `merge_write`.

### Phase 4: Asset Definitions — Gold

- [x] 4.1 Crear `orakel/pipeline/assets/gold.py`
- [x] 4.2 Asset `gold_dim_dungeon` — wrapper `GoldPipeline.build_dim_dungeon()`. Añadir lookup fallback (ver PR3).
- [x] 4.3 Asset `gold_dim_player` — wrapper `GoldPipeline.build_dim_player()`
- [x] 4.4 Asset `gold_dim_affix` — wrapper `GoldPipeline.build_dim_affix()`. Añadir lookup fallback (ver PR3).
- [x] 4.5 Asset `gold_dim_spec` — wrapper `GoldPipeline.build_dim_spec()`. Añadir lookup fallback (ver PR3).
- [x] 4.6 Asset `gold_kpi_death_clock` — wrapper `GoldPipeline.compute_kpi_death_clock()`. Key: `run_id`+`tank_name`. Usa `merge_write`.
- [x] 4.7 Asset `gold_kpi_healer_deficit` — wrapper `GoldPipeline.compute_kpi_healer_deficit()`. Key: `run_id`+`healer_name`. Usa `merge_write`.
- [x] 4.8 Asset `gold_kpi_interrupt_rate` — wrapper `GoldPipeline.compute_kpi_interrupt_rate()`. Key: `run_id`+`player_name`. Usa `merge_write`.
- [x] 4.9 Asset `gold_kpi_synergy` — wrapper `GoldPipeline.compute_kpi_synergy()`. Key: `(dungeon_id, key_level, affix_ids, comp_signature)`. Usa `merge_write`.

### Phase 5: AssetChecks

- [x] 5.1 Crear `orakel/pipeline/assets/checks.py`
- [x] 5.2 AssetCheck para `bronze_rio`: row_count ≥ 1, null % on `keystone_run_id` < 1%
- [x] 5.3 AssetCheck para `silver_raiderio`: row_count ≥ 1
- [x] 5.4 AssetCheck para `silver_dungeon_runs`: row_count ≥ 1, no null `dungeon_id`
- [x] 5.5 AssetCheck para `silver_player_performance`: row_count ≥ 5
- [x] 5.6 AssetChecks para `gold_kpi_*`: row_count ≥ 1, null % on primary metric < 50%
- [x] 5.7 AssetCheck para `gold_features`: row_count ≥ 10, no null `clear_time_seconds`

### Phase 6: Dagster Definitions + Schedule

- [x] 6.1 Crear `orakel/pipeline/definitions.py`: `Definitions` con todos los assets, asset_checks, y schedule `daily_pipeline` (cron `0 2 * * *`). Saison desde `settings.SEASON`.
- [x] 6.2 Modificar `pyproject.toml`: agregar `dagster>=1.10`, `dagster-webserver>=1.10` en dependencies.

### Files Affected by PR1

| File | Action |
|------|--------|
| `orakel/pipeline/definitions.py` | Create |
| `orakel/pipeline/assets/__init__.py` | Create |
| `orakel/pipeline/assets/bronze.py` | Create |
| `orakel/pipeline/assets/silver.py` | Create |
| `orakel/pipeline/assets/gold.py` | Create |
| `orakel/pipeline/assets/checks.py` | Create |
| `orakel/pipeline/io_managers.py` | Create |
| `orakel/pipeline/gold.py` | Modify (add `write` param to all methods) |
| `pyproject.toml` | Modify |

---

## PR3: Operational Hardening

### Phase 1: Dead Letter Queue

- [x] 1.1 Agregar `dead_letter_schema` en `orakel/models/schemas.py`: columnas `entity_type`, `entity_key`, `error_type`, `error_message`, `payload_snapshot`, `occurred_at`, `retried`, `season`.
- [x] 1.2 Modificar `scripts/ingest_warcraftlogs.py`: wrap `WarcraftLogsClient.get_events()` en try/except. En caso de `WCLRateLimitError`, `HTTPError`, timeout: escribir a `silver/dead_letter/wcl_events/` como Parquet. Continuar con siguiente fight/report.
- [x] 1.3 Modificar `scripts/match_reports.py`: wrap fuzzy join en try/except. En caso de error, escribir a `silver/dead_letter/wcl_matches/`. Continuar con siguiente tank.
- [x] 1.4 Agregar logging al final del pipeline: "DLQ: N registros en source={name}" con conteo total de registros DLQ.

### Phase 2: WCL Pagination

- [x] 2.1 Modificar `WarcraftLogsClient.get_events()` en `orakel/clients/warcraftlogs.py`: loop `nextPageTimestamp` hasta null. Por cada página, hacer query con `startTime` filter. Acumular eventos. Check budget de puntos por página (~5 pts/página). Si budget agotado mid-pagination, retornar parcial + log warning.

### Phase 3: Lookup Tables

- [x] 3.1 Crear `scripts/seed_lookups.py`: extrae `_TWW3_DUNGEONS`, `_TWW3_AFFIXES`, `_WOW_SPEC_ROLE_MAP` desde `orakel/pipeline/gold.py` y los escribe a `bronze/lookups/dungeon_timers/`, `bronze/lookups/affixes/`, `bronze/lookups/spec_roles/` como Parquet.
- [x] 3.2 Modificar `GoldPipeline.build_dim_dungeon()`: intentar leer `bronze/lookups/dungeon_timers/` primero. Si no existe o falla, usar `_TWW3_DUNGEONS` como fallback. Loguear "Lookup table dungeon_timers no encontrada, usando fallback hardcodeado" si aplica.
- [x] 3.3 Modificar `GoldPipeline.build_dim_affix()`: intentar leer `bronze/lookups/affixes/` primero. Si falla, usar `_TWW3_AFFIXES` como fallback. Log warning si fallback.
- [x] 3.4 Modificar `GoldPipeline.build_dim_spec()`: intentar leer `bronze/lookups/spec_roles/` primero. Si falla, usar `_WOW_SPEC_ROLE_MAP` como fallback. Log warning si fallback.

### Files Affected by PR3

| File | Action |
|------|--------|
| `orakel/models/schemas.py` | Modify (add dead_letter_schema) |
| `orakel/clients/warcraftlogs.py` | Modify (add pagination) |
| `scripts/ingest_warcraftlogs.py` | Modify (add DLQ) |
| `scripts/match_reports.py` | Modify (add DLQ) |
| `scripts/seed_lookups.py` | Create |
| `orakel/pipeline/gold.py` | Modify (add lookup fallback) |

---

## PR2: ML Training

### Phase 1: Feature Engineering

- [x] 1.1 Crear `orakel/ml/schemas.py`: definir `FEATURE_COLUMNS`, `TARGET_COLUMN`, `AFFIX_COLUMNS`, `DUNGEON_COLUMNS`, `ROLE_COUNT_FEATURES`. Comp signature usa role count encoding (num_tanks, num_healers, num_dps) en vez de one-hot top-20 (decisión del usuario).
- [x] 1.2 Crear `orakel/ml/features.py`: función `build_feature_view(spark, season) -> DataFrame`. Join todos los KPIs + dungeon_runs por `run_id`. Filter null targets. Feature engineering:
  - Role count encoding (num_tanks, num_healers, num_dps) from roster
  - Binary flags para `affix_id` (16 affixes)
  - Log1p transform on death_clock_seconds
  - Dungeon one-hot (8 TWW3 dungeons)
  - Z-score normalization deferred to trainer (avoids data leakage)
  - Output: `gold/features/` partitioned by `season` + `key_level`

### Phase 2: Model Training

- [x] 2.1 Crear `orakel/ml/trainer.py`: función `train_model(df) -> (model, metrics_dict)`.
- [x] 2.2 Train/test split temporal 80/20: ordenar por `completed_at`, primer 80% train, ultimo 20% test. No shuffle.
- [x] 2.3 Entrenar `sklearn.linear_model.Ridge` con `alpha=1.0, fit_intercept=True` (decisión del usuario: Ridge sobre LinearRegression). Target: `clear_time_seconds = clear_time_ms / 1000.0`.
- [x] 2.4 Baseline: `sklearn.dummy.DummyRegressor(strategy="mean")`. Calcular MAE_baseline, R²_baseline.
- [x] 2.5 Calcular métricas en test: MAE, RMSE, R². Loguear warning si R² negativo o MAE > baseline.
- [x] 2.6 MLflow tracking: `mlflow.log_params()` (features, train_size, test_size), `mlflow.log_metrics()` (MAE, RMSE, R², baseline_MAE, baseline_R²). Serializar modelo con `mlflow.sklearn.log_model()`.
- [x] 2.7 Feature importance: extraer coeficientes, reportar top-5 positivos y top-5 negativos. Escribir `feature_importance.csv` a run folder.

### Phase 3: Asset Integration

- [x] 3.1 Asset `gold_features` en `orakel/pipeline/assets/gold.py` — wrapper `build_feature_view()` desde `orakel/ml/features.py`
- [x] 3.2 Asset `ml_model` en `orakel/pipeline/assets/gold.py` — wrapper `train_model()` desde `orakel/ml/trainer.py`. Si < 10 runs, marcar skipped con mensaje "Datos insuficientes: N < 10 runs".
- [x] 3.3 Serializar modelo a MinIO `ml_models/{run_id}/` como artifact MLflow. Path registrado en metadata Parquet.

### Phase 4: Prediction (referencia para futuro)

- [x] 4.1 Crear `orakel/ml/predict.py`: función `predict(model, features_df) -> predictions_df`. Carga modelo de MinIO, aplica predict, retorna DataFrame con columns `run_id`, `predicted_clear_time_seconds`.

### Also completed (deferred from PR1)

- [x] 5.7 AssetCheck para `gold_features`: row_count ≥ 10, no null `clear_time_seconds`

### Files Affected by PR2

| File | Action |
|------|--------|
| `orakel/ml/__init__.py` | Create |
| `orakel/ml/features.py` | Create |
| `orakel/ml/trainer.py` | Create |
| `orakel/ml/predict.py` | Create |
| `orakel/ml/schemas.py` | Create |
| `orakel/pipeline/assets/gold.py` | Modify (add gold_features, ml_model assets) |
| `pyproject.toml` | Modify (add mlflow, scikit-learn, pandas) |

---

## Dependency Graph

```
PR1 (Dagster Orchestration)
  └── PO-1..PO-6
  └── PR1 completa → PR3 puede empezar

PR3 (Operational Hardening)
  ├── Depende de: PR1 (Dagster definitions base)
  └── OH-1..OH-4
  └── PR3 completa → PR2 puede empezar

PR2 (ML Training)
  ├── Depende de: PR1 (assets base) + PR3 (lookup fallback, no null features)
  └── ML-1..ML-4
```

## Verification Criteria

| PR | Asset/Function | Check |
|----|----------------|-------|
| PR1 | `check_minio_state` | Fallback si MinIO down; downstream no corre |
| PR1 | `bronze_rio` | Materializa con row_count ≥ 1 |
| PR1 | `merge_write` | Segunda ejecución no duplica run_ids |
| PR3 | DLQ | `spark.read.parquet("silver/dead_letter/...").count()` > 0 si hubo errores |
| PR3 | WCL pagination | >3000 eventos → múltiples páginas capturadas |
| PR3 | Lookup fallback | Log warning cuando lookup no existe |
| PR2 | `gold_features` | row_count ≥ 10, no null `clear_time_seconds` |
| PR2 | `ml_model` | MAE, RMSE, R² logged en MLflow; modelo en MinIO |

## Acceptance Criteria References

| PR | Spec Requirement |
|----|------------------|
| PR1 | PO-1, PO-2, PO-3, PO-4, PO-5, PO-6, PO-7, PO-8, PO-9 |
| PR3 | OH-1, OH-2, OH-3, OH-4, OH-5, OH-6, OH-7, OH-8, OH-9 |
| PR2 | ML-1, ML-2, ML-3, ML-4, ML-5, ML-6, ML-7, ML-8, ML-9, ML-10 |