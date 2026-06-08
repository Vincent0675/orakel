# Design: agregar-tests

## Technical Approach

Hybrid tiered test suite for Orakel's Medallion pipeline. Tier 1 uses pure pytest (no JVM, no Spark) for `kpi.py`, `config.py`, `rate_limiter.py`, `_run_to_row`, and Raider.IO client. Tier 2 uses SparkSession + chispa for `bronze.py`, `silver.py`, `gold.py` pipeline transforms, gated behind `@pytest.mark.spark`. Dev dependencies go in `pyproject.toml` `[dependency-groups]`. Tests mirror `orakel/` package structure under `tests/`.

## Architecture Decisions

### Decision: Test Tier Separation

**Choice**: Two tiers — `@pytest.mark.spark` marker to split pure vs Spark-dependent tests
**Alternatives considered**: (1) Single tier with Spark everywhere, (2) Separate test runner configs
**Rationale**: Tier 1 runs in <2s without JVM. Developers get instant feedback on pure Python modules. SparkSession startup adds ~8-12s per session; gating it behind a marker lets CI run Tier 1 on every push and Tier 2 on schedule.

### Decision: chispa for DataFrame Assertions

**Choice**: `chispa>=0.12` with `assert_df_equality`
**Alternatives considered**: (1) Manual column-by-column assertions, (2) pandas conversion + assert_frame_equal
**Rationale**: chispa provides schema-aware, null-safe DataFrame comparison with readable diffs. Version 0.12+ tests against PySpark 4.1.x. Manual assertions are fragile and don't handle null equality; pandas conversion adds a dependency and loses type information.

### Decision: `responses` Library for HTTP Mocking

**Choice**: `responses>=0.25` to mock `requests` calls in Raider.IO and WCL clients
**Alternatives considered**: (1) `unittest.mock.patch`, (2) `pytest-httpserver`, (3) VCR/cassette
**Rationale**: `responses` decorates `requests.Session` at the adapter level — no need to patch internal `_session` attributes. It supports JSON response bodies, status codes, and matchers. Less intrusive than `patch`, lighter than a real HTTP server, and more maintainable than cassettes for evolving APIs.

### Decision: `freezegun` NOT Included — Use Manual Mocks for Time

**Choice**: Mock `time.monotonic` and `time.time` via `unittest.mock.patch` in rate_limiter tests; no `freezegun` dependency
**Alternatives considered**: `freezegun` for time-based tests
**Rationale**: `WCLRateLimiter` uses `time.time()` and `time.sleep()` — we need `patch` for `time.sleep` anyway to avoid actual waits. Adding `freezegun` for one inline time check is unnecessary dependency weight. We'll `patch("time.time")` and `patch("time.sleep")` directly.

### Decision: Test Directory Mirrors Source Package

**Choice**: `tests/test_models/`, `tests/test_clients/`, `tests/test_pipeline/`, `tests/test_utils/` matching `orakel/models/`, `orakel/clients/`, etc.
**Alternatives considered**: Flat `tests/` with `test_kpi.py`, `test_bronze.py`, etc.
**Rationale**: Mirroring the package structure makes it trivial to locate the test for any module. This is the existing convention in `orakel/` itself (models/, clients/, pipeline/, utils/). A flat layout would collide with 9+ test files and mixed concerns.

## Data Flow

```
Tier 1 (no Spark):
  orakel/models/kpi.py ──→ tests/test_models/test_kpi.py
  orakel/config.py ──────→ tests/test_config.py
  orakel/utils/rate_limiter.py ──→ tests/test_utils/test_rate_limiter.py
  orakel/clients/raiderio.py ──→ tests/test_clients/test_raiderio.py
  orakel/clients/warcraftlogs.py ──→ tests/test_clients/test_warcraftlogs.py
  orakel/pipeline/bronze.py::_run_to_row ──→ tests/test_pipeline/test_bronze.py (Tier 1 part)

Tier 2 (SparkSession + chispa):
  conftest.py#spark ──→ tests/test_pipeline/test_bronze.py (Spark part)
  conftest.py#spark ──→ tests/test_pipeline/test_silver.py
  conftest.py#spark ──→ tests/test_pipeline/test_gold.py
  
  test_bronze.py:
    _run_to_row (Tier 1, dict→dict) + ingest_raiderio_runs (Tier 2, Spark)
  test_silver.py:
    SilverPipeline.clean_raiderio + _flatten_roster + apply_fuzzy_join
  test_gold.py:
    GoldPipeline._build_comp_signature + compute_kpi_* methods
```

## File Changes

| File | Action | Description |
|------|--------|-------------|
| `pyproject.toml` | Modify | Add `[dependency-groups]` and `[tool.pytest.ini_options]` and `[tool.coverage.run]` |
| `tests/__init__.py` | Create | Empty, makes `tests/` a package |
| `tests/conftest.py` | Create | Shared fixtures: `spark` (session-scoped SparkSession), `mock_settings` |
| `tests/test_models/__init__.py` | Create | Empty |
| `tests/test_models/test_kpi.py` | Create | Parametrized pure function tests for compute_death_clock, compute_healer_deficit, compute_synergy_score |
| `tests/test_config.py` | Create | Settings dataclass default and env-var override tests |
| `tests/test_utils/__init__.py` | Create | Empty |
| `tests/test_utils/test_rate_limiter.py` | Create | WCLRateLimiter unit tests with time/sleep mocks |
| `tests/test_clients/__init__.py` | Create | Empty |
| `tests/test_clients/test_raiderio.py` | Create | RaiderIOClient tests with `responses` mock |
| `tests/test_clients/test_warcraftlogs.py` | Create | WarcraftLogsClient tests with `responses` mock for OAuth + GraphQL |
| `tests/test_pipeline/__init__.py` | Create | Empty |
| `tests/test_pipeline/test_bronze.py` | Create | `_run_to_row` (Tier 1) + `ingest_raiderio_runs` (Tier 2) |
| `tests/test_pipeline/test_silver.py` | Create | SilverPipeline transforms with Spark (Tier 2) |
| `tests/test_pipeline/test_gold.py` | Create | GoldPipeline transforms with Spark (Tier 2) |

## Interfaces / Contracts

### SparkSession Fixture (conftest.py)

```python
@pytest.fixture(scope="session")
def spark():
    """Session-scoped SparkSession for Tier 2 tests.
    Configured for minimal resource usage in CI/local dev."""
    from pyspark.sql import SparkSession
    session = SparkSession.builder \
        .master("local[1]") \
        .appName("orakel-test") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.driver.memory", "2g") \
        .config("spark.ui.enabled", "false") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()
    yield session
    session.stop()

@pytest.fixture(autouse=True)
def _spark_cleanup(spark):
    """Clean up any temp views/spark catalog between tests."""
    yield
    spark.catalog.clearCache()
```

### pyproject.toml Additions

```toml
[dependency-groups]
test = [
    "pytest>=8",
    "pytest-cov>=6",
    "responses>=0.25",
    "chispa>=0.12",
]

[tool.pytest.ini_options]
testpaths = ["tests"]
markers = [
    "spark: marks tests that require SparkSession (deselect with '-m \"not spark\"')",
]
addopts = "-m 'not spark' --tb=short -q"

[tool.coverage.run]
source = ["orakel"]
omit = ["scripts/*", "tests/*"]
```

### Test Data Factory Pattern (conftest.py)

```python
# For Tier 2 tests — building DataFrames from Python dicts/lists
def make_bronze_row(overrides: dict | None = None) -> dict:
    """Factory for a single Bronze Raider.IO row dict.
    Pass overrides to customize specific fields."""
    base = {
        "source": "raiderio",
        "keystone_run_id": 12345,
        "dungeon_id": 15093,
        "challenge_mode_id": 401,
        "dungeon_name": "Ara-Kara, City of Echoes",
        "mythic_level": 10,
        "clear_time_ms": 1800000,
        "keystone_time_ms": 2100000,
        "completed_at": datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
        "weekly_modifiers": [9, 10],
        "roster": [{
            "name": "Tanko",
            "class": "Warrior",
            "spec": "Protection",
            "role": "tank",
            "realm": {"id": 1, "slug": "azjol-nerub", "name": "Azjol-Nerub"},
            "region": {"name": "US", "slug": "us", "short_name": "us"},
        }],
        "score": 150.5,
        "rank": 42,
        "season": "season-tww-3",
        "ingested_at": datetime(2025, 1, 15, 13, 0, 0, tzinfo=timezone.utc),
    }
    if overrides:
        base.update(overrides)
    return base
```

### Marker Usage

```python
# Tier 1 — no marker needed (default)
def test_compute_death_clock_safe():
    ...

# Tier 2 — explicit Spark marker
@pytest.mark.spark
def test_ingest_raiderio_runs(spark, ...):
    ...
```

## Testing Strategy

| Layer | What to Test | Approach |
|-------|-------------|----------|
| **Unit (Tier 1)** | `compute_death_clock`, `compute_healer_deficit`, `compute_synergy_score` | `@pytest.mark.parametrize` with boundary/category cases |
| **Unit (Tier 1)** | `Settings` dataclass defaults and env-var overrides | `monkeypatch.setenv` in tempfile, assert defaults |
| **Unit (Tier 1)** | `WCLRateLimiter.record_usage`, `wait_if_needed` | `patch("time.time")`, `patch("time.sleep")`, assert state transitions |
| **Unit (Tier 1)** | `RaiderIOClient.fetch_runs` | `responses` mock HTTP 200/429/error, assert pagination + parsing |
| **Unit (Tier 1)** | `WarcraftLogsClient.authenticate`, `query` | `responses` mock OAuth token + GraphQL responses, assert retry on 401/429 |
| **Unit (Tier 1)** | `bronze._run_to_row` | Build sample API response dict, assert output dict structure + field types |
| **Integration (Tier 2)** | `ingest_raiderio_runs` | Mock `RaiderIOClient`, create SparkSession, assert Parquet write + row count |
| **Integration (Tier 2)** | `SilverPipeline.clean_raiderio` | Build Bronze DF from `make_bronze_row()`, apply `clean_raiderio`, assert dedup + flatten |
| **Integration (Tier 2)** | `SilverPipeline._flatten_roster` | Build DF with nested realm/region structs, apply flatten, assert string extraction |
| **Integration (Tier 2)** | `GoldPipeline._build_comp_signature` | Build roster array DF, apply transform, assert signature format |
| **Integration (Tier 2)** | `GoldPipeline.compute_kpi_synergy` | Build Silver dungeon_runs DF, apply KPI computation, assert grouping + scoring |

### Edge Cases by Module

**kpi.py**: zero/negative inputs → sentinel values (`(-1.0, "safe")`, `(None, None)`), category boundary at 5s/15s for death clock, ratio boundary at 1.0/1.2 for healer deficit
**rate_limiter**: budget exhaustion → `False` return, seconds→milliseconds conversion in `reset_at`, retry loop with `time.sleep` mock
**raiderio**: HTTP 429 with/without Retry-After header, empty rankings, missing keys in run dict
**warcraftlogs**: OAuth 401 retry, GraphQL errors in 200 response, missing rateLimitData field, empty character data
**_run_to_row**: missing fields (`completed_at`, `roster`, `score`), nested struct vs string for `class`/`spec`/`realm`, invalid datetime format
**silver**: empty DataFrame, null roster, already-flat realm/region strings (idempotent flatten)
**gold**: single-row DataFrame, null fields in roster, comp_signature with nulls

## Migration / Rollout

No migration required. This change adds test infrastructure only — zero impact on production code or data. Dev dependencies are isolated in `[dependency-groups]` and don't affect production installs.

## Open Questions

- [ ] Verify chispa 0.12 compatibility with PySpark 4.1.2 on Python 3.13 at `uv sync --group test` time
- [ ] Confirm whether `freezegun` is needed for any future time-dependent tests beyond rate_limiter (currently excluded)