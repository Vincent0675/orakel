# Tasks: agregar-tests — Hybrid Test Suite

## Work Unit Summary

| WU | Tier | Description | Files | Est. Lines |
|----|------|------------|-------|------------|
| WU #1 | Tier 1 (no Spark) | Dev deps, conftest (no Spark), pure unit tests | 1 mod, 8 new | ~560 |
| WU #2 | Tier 2 (SparkSession) | SparkSession fixture, pipeline integration tests | 4 new | ~710 |

---

## WU #1 — Tier 1: Pure pytest, no Spark

### Task W1-T1 — [x] COMPLETE
- **Files**: `pyproject.toml` (modify)
- **What**: Added `[dependency-groups]` test deps, `[tool.pytest.ini_options]`, `[tool.coverage.run]`

### Task W1-T2 — [x] COMPLETE
- **Files**: `tests/conftest.py`, `tests/__init__.py` + sub-package inits (create)
- **What**: Tier 1 fixtures: mock_settings, mock_raiderio_session, mock_wcl_session

### Task W1-T3 — [x] COMPLETE
- **Files**: `tests/test_models/test_kpi.py` (create)
- **What**: 17 parametrized scenarios for compute_death_clock, compute_healer_deficit, compute_synergy_score

### Task W1-T4 — [x] COMPLETE
- **Files**: `tests/test_utils/test_config.py` (create)
- **What**: 4 tests for Settings env var loading and defaults

### Task W1-T5 — [x] COMPLETE
- **Files**: `tests/test_utils/test_rate_limiter.py` (create)
- **What**: 7 tests for WCLRateLimiter token bucket behavior

### Task W1-T6 — [x] COMPLETE
- **Files**: `tests/test_clients/test_raiderio.py` (create)
- **What**: 6 tests for RaiderIOClient REST + 429 retry

### Task W1-T7 — [x] COMPLETE
- **Files**: `tests/test_clients/test_warcraftlogs.py` (create)
- **What**: 8 tests for WarcraftLogsClient OAuth + GraphQL + rate limit

### Task W1-T8 — [x] COMPLETE
- **Files**: `tests/test_pipeline/test_bronze.py` (create)
- **What**: 9 tests for _run_to_row dict transform (Tier 1 portion only)

---

## WU #2 — Tier 2: SparkSession + chispa

### Task W2-T1 — [ ] PENDING
### Task W2-T2 — [ ] PENDING
### Task W2-T3 — [ ] PENDING
### Task W2-T4 — [ ] PENDING