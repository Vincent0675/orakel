# Delta for test-suite

## ADDED Requirements

### Requirement: ML Test Directory Structure

The `tests/test_ml/` directory MUST mirror the `orakel/ml/` package structure with the following layout:

```
tests/test_ml/
├── __init__.py              # Package init
├── test_schemas.py          # Tier 1 — schema constants and column definitions
├── test_trainer.py          # Tier 1 — training pipeline with MLflow/MinIO mocks
├── test_predict.py          # Tier 1 — prediction with loaded model, edge cases
└── test_features.py         # Tier 2 — Spark feature engineering (Phase 2)
```

#### Scenario: ML test files exist

- GIVEN the `tests/test_ml/` directory
- WHEN listing its contents
- THEN `test_schemas.py`, `test_trainer.py`, `test_predict.py` SHALL exist (Phase 1)
- AND `test_features.py` SHALL exist after Phase 2

### Requirement: ML Coverage Targets

ML module coverage SHALL meet the following targets:

| Target | Metric | Phase |
|--------|--------|-------|
| ≥80% line coverage | `schemas.py`, `trainer.py`, `predict.py` combined | Phase 1 |
| ≥70% line coverage | `features.py` | Phase 2 |
| ≥14 Tier 1 tests | `test_schemas.py` + `test_trainer.py` + `test_predict.py` | Phase 1 |
| ≥8 Tier 2 tests | `test_features.py` | Phase 2 |

#### Scenario: ML coverage measured after Phase 1

- GIVEN all Tier 1 ML tests pass
- WHEN running `uv run pytest tests/test_ml/ -m "not spark" --cov=orakel/ml --cov-report=term`
- THEN combined line coverage on `schemas.py`, `trainer.py`, `predict.py` SHALL be ≥80%

## MODIFIED Requirements

### Requirement: Test Infrastructure

(Previously: test directory structure without `tests/test_ml/`)

#### Test directory structure

The `tests/` directory MUST mirror the `orakel/` package layout:

```
tests/
├── conftest.py              # Shared fixtures (SparkSession, mock clients)
├── pytest.ini               # Markers, asyncio mode, test discovery
├── test_ml/
│   ├── __init__.py
│   ├── test_schemas.py      # Tier 1 — schema constants
│   ├── test_trainer.py      # Tier 1 — trainer with mocks
│   ├── test_predict.py      # Tier 1 — prediction edge cases
│   └── test_features.py     # Tier 2 — Spark feature engineering
├── test_models/
│   ├── test_kpi.py          # Tier 1 — pytest
│   └── test_schemas.py      # Tier 1 — schema constructors
├── test_utils/
│   ├── test_rate_limiter.py # Tier 1 — pytest (mock time)
│   └── test_minio.py        # Tier 2 — SparkSession fixture
├── test_clients/
│   ├── test_raiderio.py     # Tier 1 — responses mock
│   └── test_warcraftlogs.py # Tier 1 — responses/mock OAuth
└── test_pipeline/
    ├── test_bronze.py       # Tier 1 (_run_to_row) + Tier 2 (ingest_raiderio_runs)
    ├── test_silver.py       # Tier 2 — chispa + SparkSession
    └── test_gold.py         # Tier 2 — chispa + SparkSession + UDFs
```

#### Markers

- `spark` — Tier 2 tests that require a SparkSession. Tests without this marker MUST NOT start a SparkSession.
- Configuration in `pytest.ini` or `pyproject.toml`:

  ```ini
  [pytest]
  markers =
      spark: marks tests that require a PySpark SparkSession (local mode)
  ```

#### conftest.py fixtures

The root `conftest.py` MUST provide:

1. `spark_session` — session-scoped `SparkSession` builder: `local[1]`, 4g driver memory, `spark.sql.session.timeZone=UTC`
2. `mock_raiderio_session` — pre-configured `responses` mock for Raider.IO API
3. `mock_wcl_session` — pre-configured `responses` mock for WarcraftLogs OAuth + GraphQL

#### Scenario: Tier 1 tests pass without Spark

- GIVEN a test without the `spark` marker
- WHEN running `uv run pytest tests/ -m "not spark"`
- THEN all tests SHALL pass
- AND no SparkSession SHALL be created

#### Scenario: SparkSession fixture creates once

- GIVEN two Tier 2 tests requesting the `spark_session` fixture
- WHEN both tests run
- THEN the same SparkSession instance SHALL be reused (session scope)
- AND SparkSession SHALL be local[1] with UTC time zone

### Non-Functional Requirements

(Previously: requirements 1–5. Added: ML coverage targets 6–9)

1. **SparkSession config**: local[1], `spark.driver.memory=4g`, `spark.sql.session.timeZone=UTC`
2. **No network access**: All external HTTP calls MUST be mocked via `responses` or `unittest.mock`
3. **Coverage target**: ≥80% on pure modules (`kpi.py`, `config.py`, `bronze.py::_run_to_row`, `rate_limiter.py`) when running Tier 1
4. **Total test count**: ≥150 tests across both tiers
5. **Cleanup**: SparkSession SHALL be stopped in a finalizer via `pytest` session finish hook
6. **ML Tier 1 coverage**: ≥80% line coverage on `orakel/ml/schemas.py`, `orakel/ml/trainer.py`, `orakel/ml/predict.py` combined
7. **ML Tier 2 coverage**: ≥70% line coverage on `orakel/ml/features.py` after Phase 2
8. **ML test count**: ≥14 Tier 1 tests across `test_schemas.py`, `test_trainer.py`, `test_predict.py`
9. **ML Tier 2 test count**: ≥8 tests in `test_features.py`
