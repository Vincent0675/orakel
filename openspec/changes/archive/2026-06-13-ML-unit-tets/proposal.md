# Proposal: ML unit tets

## Intent

Zero test coverage on the ML module (`orakel/ml/`, 649 lines, 4 files) of a prediction pipeline that forecasts Mythic+ `clear_time_seconds`. Without tests, any ML change risks silent regressions in feature engineering, training correctness, and prediction output.

## Scope

### In Scope
- **Phase 1 (Tier 1, pure pytest)**: `test_schemas.py` (4–5 tests), `test_trainer.py` (6–8 tests), `test_predict.py` (4–5 tests)
- **Phase 2 (Tier 2, Spark)**: `test_features.py` (8–10 tests) with mocked `Spark DataFrameReader`
- MLflow mocking via `unittest.patch` — no MLflow server needed
- MinIO mocking for `upload_model_to_minio()`

### Out of Scope
- Refactoring `features.py` to separate I/O from transform logic
- Dagster asset integration tests (`gold_features`, `ml_model`)
- Full branch coverage — core paths + critical edge cases only
- Integration/end-to-end ML pipeline or performance benchmarks

## Capabilities

### New Capabilities
- `ml-testing`: ML module unit tests covering schemas, training, prediction, and feature engineering

### Modified Capabilities
- `test-suite`: Add `tests/test_ml/` directory structure, ML coverage targets, and Tier 1/Tier 2 ML test markers
- `ml-training`: Existing spec (ML-1 through ML-10) is validated by Phase 1 tests (trainer, predict) and Phase 2 tests (features). No spec-level behavior changes — tests verify existing requirements.

## Approach

Two-phase delivery following the project's two-tier test pattern. Phase 1: pure pytest for `schemas.py`, `trainer.py` (`unittest.patch` for MLflow + MinIO), and `predict.py` — fast, no Spark dependency. Phase 2: SparkSession tests for `features.py` using the `DataFrameReader.parquet` mocking pattern from `test_gold.py`. Delivered as chained PRs (Phase 1 → Phase 2).

## Affected Areas

| Area | Impact | Description |
|------|--------|-------------|
| `tests/test_ml/__init__.py` | New | Package init |
| `tests/test_ml/test_schemas.py` | New | Column counts, feature lists, constants |
| `tests/test_ml/test_trainer.py` | New | Training pipeline with MLflow/MinIO mocks |
| `tests/test_ml/test_predict.py` | New | Prediction with loaded model, edge cases |
| `tests/test_ml/test_features.py` | New | Spark feature engineering (Phase 2) |
| `tests/conftest.py` | Modified | ML-specific fixtures if needed |
| `openspec/specs/test-suite/spec.md` | Modified | Add ML test directory & markers |
| `openspec/specs/ml-training/spec.md` | Reference | Tests validate existing requirements |

## Risks

| Risk | Likelihood | Mitigation |
|------|------------|------------|
| MLflow context manager mocking complexity | Medium | Prototype mock for `mlflow.start_run()`; use pytest fixtures |
| MinIO client instantiation in trainer.py | Low | Mock `minio.Minio` constructor + `.put_object()` |
| PySpark not available for Phase 2 | Medium | Phase 1 is independent and ships first; same pattern as existing Tier 2 tests |
| ~600–700 lines exceeds 400-line review budget | High | Force-chained PRs — Phase 1 then Phase 2 |

## Rollback Plan

Remove `tests/test_ml/` directory and revert any `conftest.py` changes. No production code is touched — zero deployment risk.

## Dependencies

- **Phase 1**: pytest, unittest.mock (current stack)
- **Phase 2**: PySpark 4.x (already present), chispa (already installed)

## Success Criteria

- [ ] Phase 1: 14–18 tests pass with `uv run pytest tests/test_ml/ -m "not spark"`
- [ ] Phase 2: 8–10 tests pass with `uv run pytest tests/test_ml/test_features.py`
- [ ] ≥80% line coverage on `orakel/ml/schemas.py`, `orakel/ml/trainer.py`, `orakel/ml/predict.py` combined
- [ ] ≥70% line coverage on `orakel/ml/features.py` after Phase 2
