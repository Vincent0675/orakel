## Exploration: ML unit tets

### Current State

**Orakel** is a WoW Mythic+ dungeon performance predictor with a Medallion pipeline (Bronze → Silver → Gold) using PySpark, MinIO, Dagster orchestration, and **scikit-learn ML** for predicting `clear_time_seconds`.

The project already has **a fully implemented ML module** (`orakel/ml/`) with 4 files (~649 lines total):

| File | Lines | Purpose |
|------|-------|---------|
| `orakel/ml/schemas.py` | 76 | Column definitions: feature lists, affix IDs, dungeon IDs, target column |
| `orakel/ml/features.py` | 209 | Spark-based feature engineering: joins Gold KPIs, computes derived features (log1p, role counts, affix flags, dungeon one-hot), writes to Parquet |
| `orakel/ml/trainer.py` | 279 | Ridge regression training: temporal train/test split, z-score normalization, MLflow tracking, MinIO artifact upload |
| `orakel/ml/predict.py` | 85 | Prediction: load model from MLflow, predict clear_time_seconds, return pandas DataFrame |

Plus ML-related Dagster assets in `orakel/pipeline/assets/gold.py`:
- `gold_features` asset (lines 289-316) — wraps `build_feature_view()`
- `ml_model` asset (lines 326-401) — wraps `train_model()`, handles < 10 rows skip

And an asset check in `orakel/pipeline/assets/checks.py`:
- `gold_features_check` (lines 338-378) — validates feature view row count and nulls

**Existing test infrastructure**: 71 tests across 13 files, using pytest with a two-tier approach (Tier 1: pure pytest without Spark, Tier 2: SparkSession via `@pytest.mark.spark`). The test suite covers clients, pipeline stages, and utilities, but **there is zero test coverage for the ML module — no `tests/test_ml/` directory exists**.

### What ML Unit Tests Would Cover

The ML module can be tested in the same two-tier pattern:

**Tier 1 (pure pytest, no Spark/S3A)**:
- `schemas.py` — Validate column counts, feature lists match expectations, constants are correct
- `trainer.py` — Train with a small in-memory pandas DataFrame (mock MLflow and MinIO), verify metrics output, test edge cases (< 10 rows, all-NULL features)
- `predict.py` — Test prediction with a trained model, edge cases (missing columns, all zeros)
- `orakel/ml/__init__.py` — Module docstring/import test

**Tier 2 (SparkSession or Parquet mocking)**:
- `features.py` — Test feature engineering logic with in-memory Spark DataFrames (mock Parquet reads similar to `test_gold.py` pattern)
- `gold_features` and `ml_model` Dagster assets — Test with mock Spark session and mock settings

### Affected Areas

| File | Why Affected |
|------|-------------|
| `orakel/ml/schemas.py` | Schema and constant definitions need validation tests |
| `orakel/ml/features.py` | Spark feature engineering — core ML logic needs Spark tests |
| `orakel/ml/trainer.py` | Training pipeline — needs pandas-level mocks for MLflow/MinIO |
| `orakel/ml/predict.py` | Prediction function — needs model loading and prediction tests |
| `orakel/pipeline/assets/gold.py` | `gold_features` and `ml_model` Dagster assets need integration tests |
| `tests/test_ml/` | **New directory** — no ML tests exist today |
| `tests/conftest.py` | May need additional ML-specific fixtures |
| `pyproject.toml` | May need test-only deps if mlflow mocking is needed |
| `openspec/specs/ml-training/spec.md` | Existing ML training spec that tests would validate against |

### Approaches

#### Approach 1: ML-focused Tier 1 unit tests only (no Spark)

**Description**: Write pure-pytest tests for `schemas.py`, `trainer.py` (mocked MLflow and MinIO), and `predict.py`. Skip `features.py` Spark tests. This follows the pattern of the existing `test_models/test_kpi.py`.

- `test_schemas.py` — 4-5 tests: column counts, feature list sizes, constant values
- `test_trainer.py` — 6-8 tests: train with synthetic DataFrame, < 10 rows skip, NULL imputation, temporal split correctness, MLflow mock verification, MinIO upload error handling
- `test_predict.py` — 4-5 tests: predict with trained Ridge, missing columns, empty DataFrame, model loading

**Pros**:
- Zero Spark overhead — fast (< 1s) tests
- Covers core business logic: feature schema correctness, training pipeline, prediction
- Uses existing test infrastructure (pytest, mock)
- Can run immediately in the current environment
- ~15-20 tests, very manageable

**Cons**:
- Leaves `features.py` untested — the most complex ML code (Spark joins, derived features)
- Dagster asset orchestration not tested
- No integration coverage between features → training → prediction

**Effort**: Low (1 day, ~15-20 tests)

#### Approach 2: Full ML test suite (Tier 1 + Tier 2 with Spark)

**Description**: Cover all ML code including `features.py` using the same SparkSession patching pattern from `test_gold.py` (mock `DataFrameReader.parquet` to return in-memory DataFrames).

Adds:
- `test_features.py` (Tier 2, Spark) — 6-8 tests: feature view with all KPIs available, missing KPIs cause graceful fallback, role count computation, affix flag generation, dungeon one-hot encoding, NULL target filtering
- `test_trainer.py` (Tier 1, mock-heavy) — as described in Approach 1
- `test_predict.py` (Tier 1) — as described in Approach 1
- `test_schemas.py` (Tier 1) — as described in Approach 1
- Optionally: `test_gold_assets.py` (Tier 2, Spark) for `gold_features` and `ml_model` assets

**Pros**:
- Complete coverage of all ML code
- Feature engineering is the most complex and error-prone part — catching bugs here is high value
- Reuses established mocking patterns from existing `test_gold.py` (proven approach)
- ~25-35 tests across both tiers

**Cons**:
- SparkSession tests are slow (~10-15s bootstrap) and resource-heavy (2g memory)
- Requires PySpark 4.x working in the test environment
- Need to generate realistic synthetic player_performance and KPI DataFrames for fixtures

**Effort**: Medium (2-3 days, ~25-35 tests)

#### Approach 3: Pure pandas-based feature engineering tests (no Spark, using pandas backfill)

**Description**: Test feature engineering logic (`features.py`) by extracting the core computation into a pure-pandas helper, or by using a pandas DataFrame with the same shape as the Spark output. The core feature logic (log1p transform, role counts, affix flags, dungeon one-hot) can be tested without Spark.

However, `features.py` is currently tightly coupled to PySpark DataFrames — the join logic, grouping, and column expressions are PySpark-native. Testing without Spark would require significant refactoring (extract pure functions, wrap Spark-specific calls).

**Pros**:
- Avoids Spark dependency for feature tests
- Tests the actual transform logic

**Cons**:
- Requires refactoring `features.py` to separate Spark I/O from transform logic — scope creep
- The existing `test_gold.py` pattern already handles Spark tests cleanly
- Higher risk: refactoring production code to make it testable adds regression risk

**Effort**: High (3-4 days including refactoring, ~20-25 tests)

### Recommendation

**Approach 2: Full ML test suite (Tier 1 + Tier 2 with Spark)** — but with a practical phased delivery:

**Phase 1 (Tier 1, immediate)**: `test_schemas.py`, `test_trainer.py`, `test_predict.py` — pure pytest tests that run immediately with zero Spark overhead. These validate the core ML logic (column definitions, training pipeline, prediction) and provide immediate value. **~15-20 tests, ~1 day**.

**Phase 2 (Tier 2, Spark-dependent)**: `test_features.py` — follows the exact mocking pattern proven in `test_gold.py` (patch `DataFrameReader.parquet` to return in-memory DataFrames). Tests the feature engineering pipeline with synthetic KPIs. **~8-10 tests, ~1-2 days**.

The split is natural because Phase 1 is Spark-independent (training takes a pandas DataFrame, prediction takes numpy arrays, schemas are pure Python) while Phase 2 needs Spark for Parquet mocking. Phase 1 can ship immediately; Phase 2 requires PySpark 4.x in the test environment.

### Risks

- **MLflow mocking complexity**: `trainer.py` creates an MLflow run, logs params/metrics, and uploads artifacts. Tests need to mock `mlflow.start_run`, `mlflow.log_params`, `mlflow.log_metrics`, `mlflow.sklearn.log_model`, and `mlflow.set_tracking_uri`. The context manager pattern (`with mlflow.start_run(...)`) makes this trickier than standard function mocking.
- **MinIO mocking**: `upload_model_to_minio()` creates a real `Minio` client and tries to connect. Tests must mock `minio.Minio` and `.put_object()`.
- **PySpark version requirements**: Feature tests need PySpark 4.x running locally. The existing `test_gold.py` tests already prove this works — same pattern can be reused.
- **Synthetic data fidelity**: Feature tests need realistic synthetic KPI DataFrames that match expected schemas. Schema drift between modules could cause test maintenance burden.
- **No Python 3.13 on dev machine**: Spark tests won't run locally without Python 3.13 (same constraint as the existing Tier 2 tests). However, Phase 1 (Tier 1) is fully runnable.
- **400-line review budget**: Estimated ~600-700 lines of test code for both phases combined. This **exceeds the 400-line review budget** — chained PRs or stacked delivery should be planned from the start.

### Ready for Proposal

**Yes.** This exploration is complete and grounded in real code analysis. The ML module is well-structured with clear separation between Spark I/O (`features.py`) and pandas/numpy logic (`trainer.py`, `predict.py`), which maps naturally to the two-tier testing approach already established in the project.

Key decision points for the proposal:
1. **Phase split**: Ship Tier 1 tests (trainer, predict, schemas) first, Tier 2 (features) second?
2. **Chained PRs**: ~600-700 lines likely exceeds the 400-line review budget — recommend planning stacked delivery
3. **MLflow mock strategy**: Use `unittest.patch` context managers vs pytest fixtures? The `with mlflow.start_run()` pattern needs careful handling
4. **Existing test patterns**: Follow `test_gold.py`'s DataFrameReader patching for feature tests
5. **Coverage target**: What threshold for ML module? Recommend ≥80% on ML module lines
