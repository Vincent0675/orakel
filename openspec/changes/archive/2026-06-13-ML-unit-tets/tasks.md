# Tasks: ML unit tets

## Review Workload Forecast

| Field | Value |
|-------|-------|
| Estimated changed lines | ~650 (Phase 1: ~350, Phase 2: ~300) |
| 400-line budget risk | Medium |
| Chained PRs recommended | Yes |
| Suggested split | PR 1 (Phase 1) → PR 2 (Phase 2) |
| Delivery strategy | force-chained |
| Chain strategy | stacked-to-main |

Decision needed before apply: No
Chained PRs recommended: Yes
Chain strategy: stacked-to-main
400-line budget risk: Medium

### Suggested Work Units

| Unit | Goal | Likely PR | Notes |
|------|------|-----------|-------|
| 1 | Phase 1 — schemas, trainer, predict tests | PR 1 | Base: main; ~350 lines; 14–18 Tier 1 tests |
| 2 | Phase 2 — features.py Spark tests | PR 2 | Base: main (after PR 1 merges); ~300 lines; 8–10 Tier 2 tests |

## Phase 1: Tier 1 — Pure pytest tests (PR 1)

- [x] 1.1 Create `tests/test_ml/__init__.py` — empty package init
- [x] 1.2 Create `tests/test_ml/test_schemas.py` — assert FEATURE_COLUMNS = union of role/affix/dungeon, NUMERIC_FEATURES non-empty, TARGET_COLUMN = "clear_time_seconds", AFFIX_COLUMNS count = 16, DUNGEON_COLUMNS count = 8
- [x] 1.3 Create `tests/test_ml/test_trainer.py` — factory `_make_training_df(n, seed)` with np.random.default_rng; test train_model() with mocked `mlflow.start_run` (MagicMock context manager) and mocked `minio.Minio`; cover: happy path (metrics returned), <10 rows (None + warning), NULL imputation (mean fill), temporal 80/20 split (no leakage), MLflow log_params/log_metrics called, MinIO put_object called, `_get_numeric_feature_names` returns correct list
- [x] 1.4 Create `tests/test_ml/test_predict.py` — test predict() with trained Ridge: valid DF (positive floats in output), missing cols (actual behavior: silent NaN fill, not ValueError), empty DF (empty output); test load_model_from_mlflow with mocked mlflow.keras.load_model

## Phase 2: Tier 2 — SparkSession tests (PR 2)

- [x] 2.1 Create `tests/test_ml/test_features.py` — reuse `_patch_reads` pattern from `tests/test_pipeline/test_gold.py`; mock Spark DataFrameReader.parquet to return synthetic DataFrames
- [x] 2.2 Test role counts: 1-2-3 roster → assert num_tanks=1, num_healers=2, num_dps=3 from build_feature_view output
- [x] 2.3 Test affix flags: affix_ids [9,10,123,124] → assert matching flags=1, non-matching=0
- [x] 2.4 Test log1p transform: death_clock_seconds=10.0 → death_clock_seconds_log1p ≈ 2.398
- [x] 2.5 Test NULL KPI imputation: deficit_ratio=NULL → pins actual behavior (NULL flows through, NOT imputed to 0.0)
- [x] 2.6 Test join: two runs + KPIs → one row per run with aggregated death_clock, deficit_ratio, interrupts_per_minute, synergy_score per comp