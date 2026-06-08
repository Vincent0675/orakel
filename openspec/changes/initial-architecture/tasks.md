# Tasks: initial-architecture

## Review Workload Forecast

| Field | Value |
|-------|-------|
| Estimated changed lines | ~2800–3200 (30 new files, configs, scripts, pipeline modules) |
| 400-line budget risk | High |
| Chained PRs recommended | Yes |
| Suggested split | PR 1 (infra+bronze) → PR 2 (silver+kpi4) → PR 3 (wcl+fuzzy) → PR 4 (ml+bi) |
| Delivery strategy | ask-on-risk |
| Chain strategy | pending |

Decision needed before apply: Yes
Chained PRs recommended: Yes
Chain strategy: pending
400-line budget risk: High

### Suggested Work Units

| Unit | Goal | Likely PR | Notes |
|------|------|-----------|-------|
| 1 | Infrastructure + Bronze (Raider.IO) | PR 1 | pyproject, docker-compose, .env, config, clients, schemas, ingest scripts; smoke test bronze |
| 2 | Silver + KPI 4 (Composition Synergy) | PR 2 | pipeline modules, bronze→silver, KPI functions, silver→gold, verify KPI 4 |
| 3 | WCL Integration + KPIs 1–3 | PR 3 | WCL client, rate limiter, match_reports.py, fuzzy join, update silver+gold; full 4-KPI run |
| 4 | ML + BI + Orchestrator | PR 4 | train_model.py, BI notebook, run_pipeline.py, e2e smoke test |

---

## Phase 1: Infrastructure + Bronze (Raider.IO)

- [x] 1.1 Initialize `pyproject.toml` with `pyspark`, `minio`, `requests`, `python-dotenv`, `scikit-learn` via `uv add`
  - **Files**: `pyproject.toml`
  - **Deps**: None
  - **Effort**: S
  - **Acceptance**: `uv run pyspark --version` succeeds; `import pyspark` works in `uv run python`

- [x] 1.2 Create `docker-compose.yml` with MinIO service (ports 9000/9001, root user/password from env)
  - **Files**: `docker-compose.yml`
  - **Deps**: None
  - **Effort**: S
  - **Acceptance**: `docker compose up -d` starts MinIO; MinIO console reachable at localhost:9001

- [x] 1.3 Create `.env` template and `orakel/config.py` with `python-dotenv` loading
  - **Files**: `.env`, `orakel/config.py`, `.gitignore` (add `.env`)
  - **Deps**: None
  - **Effort**: S
  - **Acceptance**: `from orakel.config import settings; settings.RAIDERIO_API_KEY` returns env value or raises clear error

- [x] 1.4 Create `orakel/utils/minio.py` — SparkSession factory with S3A config pointing to MinIO
  - **Files**: `orakel/utils/minio.py`
  - **Deps**: 1.1, 1.2, 1.3
  - **Effort**: M
  - **Acceptance**: `get_spark_session()` returns SparkSession with `local[*]`, 4g driver, S3A endpoint; `spark.read.parquet("s3a://orakel/")` works

- [x] 1.5 Create `orakel/models/schemas.py` — all PySpark StructType definitions (Bronze/Silver/Gold)
  - **Files**: `orakel/models/schemas.py`
  - **Deps**: 1.4
  - **Effort**: M
  - **Acceptance**: All 4 schemas defined: `bronze_raiderio_schema`, `bronze_wcl_reports_schema`, `bronze_wcl_events_schema`, `silver_dungeon_runs_schema`, `silver_player_performance_schema`, plus Gold KPI schemas and dim tables

- [x] 1.6 Create `orakel/clients/raiderio.py` — REST client with pagination + 429 backoff
  - **Files**: `orakel/clients/raiderio.py`
  - **Deps**: 1.3
  - **Effort**: M
  - **Acceptance**: `RaiderIOClient.fetch_runs(season, page)` returns parsed JSON; HTTP 429 triggers exponential backoff; stops on empty page

- [x] 1.7 Create `scripts/ingest_raiderio.py` — fetch runs → Bronze Parquet on MinIO
  - **Files**: `scripts/ingest_raiderio.py`
  - **Deps**: 1.4, 1.5, 1.6
  - **Effort**: M
  - **Acceptance**: `uv run python scripts/ingest_raiderio.py --season season-tww-3 --limit 5` writes Parquet to `bronze/raiderio/runs/`

- [x] 1.8 Smoke test: run ingest → verify Bronze files in MinIO
  - **Files**: None (verification only)
  - **Deps**: 1.7
  - **Effort**: S
  - **Acceptance**: `spark.read.parquet("s3a://orakel/bronze/raiderio/runs/")` returns DataFrame with rows; schema matches `bronze_raiderio_schema`

---

## Phase 2: Silver + KPI 4 (Composition Synergy)

- [x] 2.1 Create `orakel/pipeline/bronze.py` — refactor ingest logic into pipeline module
  - **Files**: `orakel/pipeline/bronze.py`
  - **Deps**: 1.4, 1.5, 1.6
  - **Effort**: M
  - **Acceptance**: `BronzePipeline.ingest_raiderio()` produces same output as ingest_raiderio.py; callable from other scripts

- [x] 2.2 Create `orakel/pipeline/silver.py` — clean, dedup, type casting (no WCL join yet)
  - **Files**: `orakel/pipeline/silver.py`
  - **Deps**: 2.1
  - **Effort**: M
  - **Acceptance**: `SilverPipeline.clean_raiderio()` reads Bronze, dedupes by `keystone_run_id`, enforces types, writes `silver/raiderio_runs/`

- [x] 2.3 Create `scripts/bronze_to_silver.py` — Spark job: Bronze → Silver
  - **Files**: `scripts/bronze_to_silver.py`
  - **Deps**: 2.2, 1.4
  - **Effort**: S
  - **Acceptance**: `uv run python scripts/bronze_to_silver.py --season season-tww-3` produces Silver Parquet; dedup verified by row count reduction

- [x] 2.4 Create `orakel/models/kpi.py` — pure Python KPI functions (all 4)
  - **Files**: `orakel/models/kpi.py`
  - **Deps**: None
  - **Effort**: M
  - **Acceptance**: All 4 functions work: `compute_death_clock(dtps, hps, max_hp)`, `compute_healer_deficit(tank_dtps, healer_hps)`, `compute_interrupt_rate(successful, total)`, `compute_synergy_score(comp_avg, overall_avg)`; edge cases return correct sentinel/NULL values

- [x] 2.5 Create `orakel/pipeline/gold.py` — KPI aggregations + dimension tables
  - **Files**: `orakel/pipeline/gold.py`
  - **Deps**: 2.3, 2.4
  - **Effort**: L
  - **Acceptance**: `GoldPipeline.compute_synergy()` writes `gold/kpi_composition_synergy/`; `GoldPipeline.compute_kpi4_from_raiderio()` groups by (dungeon, key_level, affixes, comp) and computes ratio; NULL for sample_count=1

- [x] 2.6 Create `scripts/silver_to_gold.py` — Spark job: Silver → Gold
  - **Files**: `scripts/silver_to_gold.py`
  - **Deps**: 2.5, 1.4
  - **Effort**: S
  - **Acceptance**: `uv run python scripts/silver_to_gold.py --season season-tww-3` produces Gold Parquet files under `gold/`

- [x] 2.7 Verify KPI 4 (Composition Synergy) computed from Raider.IO-only data
  - **Files**: None (verification only)
  - **Deps**: 2.6
  - **Effort**: S
  - **Acceptance**: Query `gold/kpi_composition_synergy/` — synergy_score values are non-null for comps with ≥2 samples; sample_count matches group sizes

---

## Phase 3: WCL Integration + KPIs 1–3

- [x] 3.1 Create `orakel/clients/warcraftlogs.py` — GraphQL+OAuth client with point tracking
  - **Files**: `orakel/clients/warcraftlogs.py`
  - **Deps**: 1.3
  - **Effort**: L
  - **Acceptance**: `WarcraftLogsClient.get_token()` returns access token; `WarcraftLogsClient.fetch_fights(report_code)` returns parsed fights; point budget tracked and resumable

- [x] 3.2 Create `orakel/utils/rate_limiter.py` — token-bucket for WCL
  - **Files**: `orakel/utils/rate_limiter.py`
  - **Deps**: None
  - **Effort**: S
  - **Acceptance**: `TokenBucketRateLimiter.consume(cost)` sleeps when budget exhausted; `Retry-After` header respected; 3 retries then abort with logged error

- [x] 3.3 Create `scripts/match_reports.py` — fuzzy join engine with stratified tank sampling
  - **Files**: `scripts/match_reports.py`
  - **Deps**: 1.6, 3.1, 3.2, 1.4
  - **Effort**: XL
  - **Acceptance**: Groups Raider.IO tanks by class (6 classes); samples evenly; calls WCL API per tank; produces `silver/matches/` Parquet with columns `rio_run_id, wcl_report_code, wcl_fight_id, confidence, match_method`; Layer 1 = challenge_mode_id + keystoneLevel + affixes, Layer 2 = 30s window, Layer 3 = 3/5 roster overlap

- [ ] 3.4 Run `match_reports.py` → produce match manifest → verify matches
  - **Files**: None (verification only)
  - **Deps**: 3.3
  - **Effort**: M
  - **Acceptance**: `silver/matches/` contains ≥1 match; confidence scores distributed (not all 0.2); match_method values include `"full_3_layer"` or `"rio_only"`

- [x] 3.5 Create `scripts/ingest_warcraftlogs.py` — fetch WCL events for matched runs
  - **Files**: `scripts/ingest_warcraftlogs.py`
  - **Deps**: 3.1, 3.2, 3.4, 1.4, 1.5
  - **Effort**: L
  - **Acceptance**: Reads match manifest; fetches DamageTaken, HealingDone, Interrupts events per fight; writes to `bronze/warcraftlogs/events/` partitioned by `report_code/fight_id`

- [x] 3.6 Update `orakel/pipeline/silver.py` — add fuzzy join integration using match manifest
  - **Files**: `orakel/pipeline/silver.py`
  - **Deps**: 2.2, 3.4
  - **Effort**: M
  - **Acceptance**: `SilverPipeline.apply_fuzzy_join()` reads match manifest; joins WCL events to Raider.IO runs; produces `silver/dungeon_runs/` and `silver/player_performance/`

- [x] 3.7 Update `scripts/bronze_to_silver.py` — add WCL data processing
  - **Files**: `scripts/bronze_to_silver.py`
  - **Deps**: 3.5, 3.6
  - **Effort**: S
  - **Acceptance**: Script processes both Raider.IO runs AND WCL events; both land in Silver

- [x] 3.8 Update `orakel/pipeline/gold.py` — compute KPIs 1–3 (Death Clock, Healer Deficit, Interrupt Rate)
  - **Files**: `orakel/pipeline/gold.py`
  - **Deps**: 2.5, 3.6
  - **Effort**: L
  - **Acceptance**: `compute_death_clock_kpi()` → `gold/kpi_tank_death_clock/`; `compute_healer_deficit_kpi()` → `gold/kpi_healer_deficit/`; `compute_interrupt_rate_kpi()` → `gold/kpi_interrupt_rate/`; edge cases handled per spec (sentinel for infinite survival, NULL for zero casts)

- [ ] 3.9 Run full pipeline → verify all 4 KPIs computed
  - **Files**: None (verification only)
  - **Deps**: 3.8
  - **Effort**: M
  - **Acceptance**: All 4 KPI folders exist in `gold/` with non-empty Parquet; death_clock_categories distributed across `safe/moderate/critical`; deficit_ratio range matches expected thresholds

- [ ] 3.10 Manual audit scenario V2 — verify 10 fuzzy join matches
  - **Files**: None (verification only)
  - **Deps**: 3.9
  - **Effort**: S
  - **Acceptance**: 10 random matches inspected manually; Layer 1 IDs match; timestamps within 30s; roster overlap ≥3; confidence score matches formula

---

## Phase 4: ML + BI + Orchestrator

- [ ] 4.1 Create `scripts/train_model.py` — feature assembly, train/test split, scikit-learn models
  - **Files**: `scripts/train_model.py`
  - **Deps**: 3.9, 1.4, 1.3
  - **Effort**: L
  - **Acceptance**: Reads Gold data; assembles 8 features (per spec); 80/20 chronological split by `completed_at`; trains Linear Regression + Random Forest; logs RMSE, MAE, R²; saves model to `ml_models/dungeon_predictor/`

- [ ] 4.2 Run ML training → verify model metrics (RMSE, MAE, R²)
  - **Files**: None (verification only)
  - **Deps**: 4.1
  - **Effort**: S
  - **Acceptance**: Training completes without error; RMSE and R² logged; R² > 0 (baseline); predictions fall within valid time range per scenario V4

- [ ] 4.3 Build BI dashboard or notebook connected to Gold layer
  - **Files**: `notebooks/bi_dashboard.py` or similar
  - **Deps**: 3.9
  - **Effort**: M
  - **Acceptance**: 4 charts exist (Death Clock histogram, Healer Deficit scatter, Interrupt Rate bar, Comp Synergy heatmap); 3 filters work (key level slider, dungeon multi-select, affix multi-select)

- [ ] 4.4 Create `scripts/run_pipeline.py` — CLI orchestrator (`--steps`, `--season`, `--limit`)
  - **Files**: `scripts/run_pipeline.py`
  - **Deps**: 1.7, 2.3, 2.6, 3.3, 3.5, 4.1
  - **Effort**: M
  - **Acceptance**: `uv run python scripts/run_pipeline.py --steps bronze,silver,gold,ml --season season-tww-3 --limit 100` runs all steps in sequence; `--steps bronze,silver` runs subset; `--limit` caps API pages

- [ ] 4.5 End-to-end pipeline smoke test with `--limit 10`
  - **Files**: None (verification only)
  - **Deps**: 4.4
  - **Effort**: S
  - **Acceptance**: Full pipeline completes in <15 minutes with `--limit 10`; Bronze→Silver→Gold→ML all succeed; ML model saved; BI charts renderable

---

## Effort Summary

| Phase | Tasks | Total Effort | Dependencies |
|-------|-------|-------------|--------------|
| 1 — Infrastructure + Bronze | 8 tasks | ~4–5 hrs | Docker, API keys |
| 2 — Silver + KPI 4 | 7 tasks | ~5–6 hrs | Phase 1 |
| 3 — WCL Integration | 10 tasks | ~8–10 hrs | Phase 2, WCL OAuth keys |
| 4 — ML + BI + Orchestrator | 5 tasks | ~4–5 hrs | Phase 3 |
| **Total** | **30 tasks** | **~21–26 hrs** | |

---

## Risks & Dependencies to Watch

| Risk | Severity | Mitigation |
|------|----------|------------|
| PySpark sdist build time (first `uv add pyspark`) | 🟡 Medium | Takes 3–5 min on first install; subsequent runs use cached wheel |
| WCL rate limits during Phase 3 development | 🔴 High | Token-bucket with 3600 pts/hr; `match_reports.py` uses stratified sampling to minimize API calls |
| Dungeon ↔ encounterID mapping (requires real WCL data) | 🔴 High | ID mapping table verified in design.md; Layer 1 fuzzy join depends on `challenge_mode_id` = `encounterID` |
| PyArrow transitive dependency from PySpark | 🟡 Medium | Avoid importing PyArrow directly; let Spark manage Parquet; verify `spark.sparkContext._jvm.org.apache.hadoop.fs.s3a.S3AFileSystem` available |
| Fuzzy join precision — no cross-reference key | 🟡 Medium | Layer 1 (exact IDs) + Layer 2 (30s window) + Layer 3 (3/5 roster) gives high confidence; manual audit in T-025 validates |
| Chronological 80/20 split — insufficient recent data | 🟢 Low | MVP uses current season data; chronological split by `completed_at` prevents leakage |

---

## Next Step

Ready for `sdd-apply`. Before starting, confirm:
1. Chain strategy (stacked-to-main vs feature-branch-chain)
2. WCL OAuth credentials available for Phase 3
3. Docker + MinIO running for local development