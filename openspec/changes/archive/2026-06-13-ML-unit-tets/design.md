# Design: ML unit tests

## Technical Approach

Two-phase test implementation for `orakel/ml/` (649 lines, 4 files). Phase 1 (Tier 1) uses pure pytest with `unittest.mock` for MLflow + MinIO — zero external services. Phase 2 (Tier 2) uses SparkSession + `DataFrameReader.parquet` patching (pattern from `test_gold.py`). Tests are additive: no production code changes.

**Spec discrepancy note**: The spec references `CATEGORICAL_FEATURES`, `AFFIX_FEATURES`, `DUNGEON_FEATURES`, and `log1p_death_clock_seconds` column name. The actual code uses `ROLE_COUNT_FEATURES + AFFIX_COLUMNS + DUNGEON_COLUMNS` and `death_clock_seconds_log1p`. Tests will validate against ACTUAL code constants, not spec names. Similarly, `predict()` does NOT raise `ValueError` for missing columns — it silently uses `available_features`. Tests match real behavior.

## Architecture Decisions

| Decision | Option | Tradeoff | Choice |
|----------|--------|----------|--------|
| MLflow mock | (A) Patch `mlflow.start_run` etc. at module level | Simple, no server | **A** — patch `mlflow.start_run`, `mlflow.log_params`, `mlflow.log_metrics`, `mlflow.sklearn.log_model`, `mlflow.log_artifact`, `mlflow.active_run`, `mlflow.set_tracking_uri` with `MagicMock`. Context manager via `mock_start_run.return_value.__enter__` returning a mock with `.info.run_id`. |
| | (B) Local MLflow file server | Real integration | Too slow, CI-dependent |
| MinIO mock | (A) Mock `minio.Minio` constructor | No network, verifies call args | **A** — `patch("orakel.ml.trainer.Minio")` returns MagicMock. Chain `.bucket_exists()`, `.put_object()`. |
| | (B) Patch `upload_model_to_minio` entirely | Skips upload logic | Doesn't test serialization |
| Test data | (A) Factory functions (`_make_training_df(n)`) | Parameterizable, deterministic | **A** — module-level factories with `np.random.default_rng(seed)`. |
| | (B) CSV/parquet fixture files | Realistic but rigid | Can't vary row count easily |
| conftest additions | (A) Add ML fixtures to root `conftest.py` | Centralized | **B** — keep fixtures local to each test file. ML mocks are specific to trainer/predict; `spark_session` already exists in root conftest. |
| | (B) Per-file fixtures | Self-contained, minimal cross-pollution | |
| Phase 2 DataFrameReader patch | (A) Reuse `_patch_reads` pattern from `test_gold.py` | Proven, consistent | **A** — same `patch.object(DataFrameReader, "parquet", mock_fn)` pattern, adapted for feature view's multiple parquet reads. |
| | (B) Read from local Parquet files on disk | More realistic | Slower, file management overhead |

## Data Flow

### Phase 1 — Trainer test path

```
_make_training_df(n=15)
    │
    ▼
train_model(df)  ──mlflow.start_run──→ MagicMock (context mgr)
    │             ──mlflow.log_params──→ MagicMock
    │             ──mlflow.sklearn.log_model──→ MagicMock
    │             ──upload_model_to_minio──→ Minio() MagicMock
    ▼
(model, metrics_dict)
```

### Phase 2 — Features test path

```
spark_session.createDataFrame(rows, schema)
    │
    ▼
_patch_reads({"silver/dungeon_runs": dr_df, "gold/kpi_tank_death_clock": dc_df, ...})
    │
    ▼
build_feature_view(spark, "season-tww-3")
    │  ──spark.read.parquet──→ in-memory DF (mocked)
    │  ──role counts, log1p, affix flags, dungeon one-hot
    │  ──NULL target filter
    ▼
result DataFrame  ──chispa assertions──→ pass/fail
```

## File Changes

| File | Action | Description |
|------|--------|-------------|
| `tests/test_ml/__init__.py` | Create | Empty package init |
| `tests/test_ml/test_schemas.py` | Create | Validate FEATURE_COLUMNS composition, column naming, column counts, TARGET_COLUMN |
| `tests/test_ml/test_trainer.py` | Create | train_model happy path, insufficient data, NULL imputation, temporal split, MLflow logging, MinIO upload, `_get_numeric_feature_names` |
| `tests/test_ml/test_predict.py` | Create | predict happy path, NaN fill behavior, empty DataFrame, load_model_from_mlflow mock |
| `tests/test_ml/test_features.py` | Create | Role counts, log1p transform, affix binary flags, dungeon one-hot, NULL target filter, KPI join fallbacks (Phase 2) |

## Interfaces / Contracts

### MLflow mock fixture (`test_trainer.py`)

```python
@pytest.fixture(autouse=True)
def mock_mlflow():
    with patch("mlflow.start_run") as mock_start_run, \
         patch("mlflow.log_params"), \
         patch("mlflow.log_metrics"), \
         patch("mlflow.sklearn.log_model"), \
         patch("mlflow.log_artifact"), \
         patch("mlflow.active_run") as mock_active_run, \
         patch("mlflow.set_tracking_uri"):
        run_mock = MagicMock()
        run_mock.info.run_id = "test-run-123"
        mock_start_run.return_value.__enter__ = MagicMock(return_value=run_mock)
        mock_start_run.return_value.__exit__ = MagicMock(return_value=False)
        yield
```

### MinIO mock fixture (`test_trainer.py`)

```python
@pytest.fixture(autouse=True)
def mock_minio():
    with patch("orakel.ml.trainer.Minio") as MockMinio:
        client = MagicMock()
        client.bucket_exists.return_value = True
        MockMinio.return_value = client
        yield client
```

### Training data factory (`test_trainer.py`)

```python
def _make_training_df(n: int, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    return pd.DataFrame({
        "key_level": rng.integers(2, 20, size=n),
        "death_clock_seconds_log1p": rng.uniform(5, 10, size=n),
        # ... all FEATURE_COLUMNS + TARGET_COLUMN + completed_at
    })
```

## Testing Strategy

| Layer | What | Approach |
|-------|------|----------|
| Unit | schemas.py: column counts, naming, composition | Direct const assertions |
| Unit | trainer.py: happy path, <10 rows, NULL imputation, split order | Mock MLflow+MinIO, assert model type + metrics keys |
| Unit | trainer.py: MLflow logging | Verify `mock_mlflow.log_params.called`, `log_metrics.called` |
| Unit | trainer.py: MinIO upload | Verify `client.put_object.called` with bucket/key args |
| Unit | trainer.py: `_get_numeric_feature_names` | Assert correct subset filtering |
| Unit | predict.py: predict + NaN fill | Feed DF with NaN → assert fillna(0) + output schema |
| Unit | predict.py: empty DF | Assert empty output with correct columns |
| Unit | predict.py: load_model_from_mlflow | Mock `MlflowClient.search_runs` + `mlflow.sklearn.load_model` |
| Integration | features.py: transforms | SparkSession + mock DataFrameReader, chispa column assertions |

## Migration / Rollout

No migration required. Tests are additive — zero production code changes. Rollback = delete `tests/test_ml/`.

**Phase boundaries**: Phase 1 PR (~350 lines) and Phase 2 PR (~300 lines) each under 400-line review budget. Phase 2 targets Phase 1 branch.

**conftest.py**: No changes needed for Phase 1. Phase 2 reuses existing `spark_session` fixture from root conftest. ML-specific mocks stay local to their test files.

## Open Questions

- None — all decisions resolved in proposal question round.