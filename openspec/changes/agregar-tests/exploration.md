## Exploration: agregar-tests

### Current State

**Zero tests. Zero CI. Zero test config.** This is Warning #1 deferred from the SDD Phase 3 implementation — the entire pipeline runs on trust.

The project is a **Python 3.13+** data pipeline (WoW Mythic+ analytics) with Medallion architecture (Bronze → Silver → Gold) on MinIO/S3A via PySpark. Managed exclusively with `uv` (no `pip`, no poetry). ~2,800 lines of Python across 19 source files.

### Testing Profile by Module

| Module | Lines | Type | Testability | Effort |
|--------|-------|------|-------------|--------|
| `orakel/models/kpi.py` | 130 | Pure Python functions | **Trivial** — no deps, no I/O | Low |
| `orakel/models/schemas.py` | 310 | PySpark StructType defs | Schema validation only | Low |
| `orakel/config.py` | 51 | Env-var dataclass | Monkeypatch-friendly | Low |
| `orakel/utils/rate_limiter.py` | 155 | Time-based state machine | Mock `time.*` needed | Low-Med |
| `orakel/clients/raiderio.py` | 118 | REST client (requests) | Mock `requests.Session` | Medium |
| `orakel/clients/warcraftlogs.py` | 497 | GraphQL+OAuth client | Complex mock (OAuth, rate limits, GraphQL) | Medium-High |
| `orakel/utils/minio.py` | 60 | SparkSession factory | Needs PySpark local mode | Medium |
| `orakel/pipeline/bronze.py` | 153 | Spark ETL (DataFrame) | Needs SparkSession + mock client | Medium-High |
| `orakel/pipeline/silver.py` | 507 | Spark ETL (DataFrame transforms) | Needs SparkSession + Parquet fixtures | High |
| `orakel/pipeline/gold.py` | 940 | Spark ETL (KPIs + dims) | Needs SparkSession + UDF registration | High |
| `scripts/*.py` | ~1,250 | CLI wrappers (argparse) | Thin — test via `orakel.pipeline` | Low |

### Affected Areas

- `orakel/models/kpi.py` — **highest value/lowest effort**: pure functions, trivial to unit-test
- `orakel/models/schemas.py` — schema contract validation tests
- `orakel/clients/raiderio.py` — HTTP mock tests for pagination, 429 retry, empty data
- `orakel/clients/warcraftlogs.py` — OAuth flow, rate limit, GraphQL query mocking
- `orakel/utils/rate_limiter.py` — point budget tracking, wait logic, edge cases
- `orakel/utils/minio.py` — SparkSession configuration validation
- `orakel/pipeline/bronze.py` — row transformation + Spark write path
- `orakel/pipeline/silver.py` — dedup window, roster flattening, fuzzy join, fallbacks
- `orakel/pipeline/gold.py` — KPI computations (death clock UDF, healer deficit UDF, synergy), dim tables
- `pyproject.toml` — add `[dependency-groups]` for dev/test deps
- `tests/` — new directory (whole structure)
- `.github/workflows/` — new CI pipeline (optional but recommended)

### Approaches

#### Approach 1: Minimal Viable Tests (pytest + pure-Python + mock)

**Description**: Start with only pure-Python modules (KPI, config, rate_limiter) and HTTP clients with `unittest.mock`. No Spark tests initially — defer pipeline tests to a second pass.

**Dependencies**: `pytest`, `pytest-cov`

**Test structure**: `tests/test_models/test_kpi.py`, `tests/test_clients/`, `tests/test_utils/`

**Pros**:
- Zero Spark overhead (no `$SPARK_HOME`, no JARs, no 4g memory)
- Fast feedback loop (sub-second test runs)
- Covers ~40% of business logic (KPIs = the actual analytics value)
- Can run on this machine without Python 3.13 (pytest runs with any Python)
- Minimal `pyproject.toml` changes

**Cons**:
- Pipeline modules (bronze/silver/gold) remain untested — the most complex and error-prone code
- Spark schema contracts not validated
- No integration/end-to-end coverage
- Leaves the hardest-to-debug parts uncovered

**Effort**: Low (1-2 days, ~60-80 tests)

#### Approach 2: Full Unit Test Suite (pytest + chispa + SparkSession fixture)

**Description**: Full test suite covering everything including PySpark pipeline modules. Uses `pytest` for unit tests, `chispa` for DataFrame assertions, a SparkSession fixture in local mode, and Parquet fixtures for pipeline tests.

**Dependencies**: `pytest`, `pytest-cov`, `chispa`, `py4j` (already transitive), `responses` (for HTTP mocking)

**Test structure**:
```
tests/
├── conftest.py              # SparkSession fixture
├── test_models/
│   ├── test_kpi.py          # Pure Python unit tests
│   └── test_schemas.py      # Schema contract validation
├── test_clients/
│   ├── test_raiderio.py     # HTTP mock tests
│   └── test_warcraftlogs.py # OAuth + GraphQL mock tests
├── test_utils/
│   ├── test_rate_limiter.py # Time-mocked tests
│   └── test_minio.py        # SparkSession config test
├── test_pipeline/
│   ├── test_bronze.py       # Row transform + Spark write
│   ├── test_silver.py       # Dedup, roster flatten, fuzzy join
│   └── test_gold.py         # KPI UDFs, dim tables
└── fixtures/
    ├── raiderio_runs.parquet
    └── wcl_events.parquet
```

**Pros**:
- Comprehensive coverage across all layers
- `chispa` provides readable DataFrame assertion failures
- Catches schema drift, UDF errors, and Spark-specific bugs
- Parquet fixtures can be checked into git (small, generated)

**Cons**:
- **Requires Python 3.13** and a working PySpark install (significant env setup)
- SparkSession startup is slow (~10-15s per session)
- Test memory requirements: 4g+ for Spark driver
- Not runnable on this machine without a PySpark-compatible Python
- `chispa` was designed for PySpark 3.x — compatibility with PySpark 4.x uncertain
- Parquet fixtures need generation scripts (not trivial)

**Effort**: High (4-6 days, ~200-250 tests)

#### Approach 3: Tiered Testing (pytest for units + PySpark via local mode manual trigger)

**Description**: Tiered approach — unit tests (pytest) run on every `uv run pytest`, pipeline/integration tests (PySpark) are opt-in with `uv run pytest tests_pipeline/ --spark` using a marker. Pipeline tests use a `SparkSession` created once per session (not per function), and pre-generated Parquet fixtures that live in `tests/fixtures/`.

**Dependencies**: `pytest`, `pytest-cov`, `chispa` (for pipeline tests only), `responses`

**Test structure**: Same as Approach 2, but pipeline tests are gated behind a `@pytest.mark.spark` marker and a CLI flag.

**Pros**:
- Unit tests stay fast (< 1s) and run everywhere
- Pipeline tests available for full coverage when Spark env is available
- Same test runner, same conventions — just a marker difference
- Pre-generated Parquet fixtures avoid network/API dependencies
- Most pragmatic for CI: unit tests in PR checks, pipeline tests in nightly/scheduled

**Cons**:
- Two-tier test split can lead to confusion ("did I break spark tests?")
- Still needs Python 3.13 with PySpark for pipeline tests
- Chispa compatibility uncertainty with PySpark 4.x
- Parquet fixtures need maintenance when schemas change

**Effort**: Medium (3-5 days, ~150-200 tests)

### Recommendation

**Approach 3: Tiered Testing** — it's the only approach that balances coverage with practical constraints.

Why not Approach 1: leaves the most complex code (pipeline transforms, UDFs, Spark schema enforcement) completely uncovered. These are exactly where bugs happen — schema drift, null handling, type coercions, UDF edge cases.

Why not Approach 2: SparkSession startup is too expensive for per-function usage in CI, and chispa compatibility with PySpark 4.x is unconfirmed. Also requires Python 3.13 on every test run which is a heavy CI requirement.

Approach 3 gives us:
1. **Fast unit tests** (KPI, config, rate limiter, clients) that run everywhere — these cover real business logic
2. **Spark pipeline tests** gated by marker and runnable when the environment supports it
3. **Pre-generated Parquet fixtures** to avoid external API dependencies
4. **Same pyproject.toml conventions** for both tiers

### Concrete Plan for `pyproject.toml`

```toml
[dependency-groups]
test = [
    "pytest>=8",
    "pytest-cov>=6",
    "responses>=0.25",
    "chispa>=0.10",
]
```

Then tests run with: `uv run --group test pytest`

### Test Dependency Notes

| Dependency | Why | Alternatives |
|-----------|-----|-------------|
| `pytest` | Standard test runner | nose2, unittest (not recommended) |
| `pytest-cov` | Coverage reporting | coverage.py directly |
| `responses` | HTTP request mocking for `requests` | `requests-mock`, `unittest.mock` |
| `chispa` | PySpark DataFrame assertions with readable diffs | Manual `df.collect()` asserts (verbose) |

Key insight: **`chispa` may not support PySpark 4.x**. Current chispa releases (0.9.x) target PySpark 3.x. If incompatible, the pipeline tests will need to use manual DataFrame assertions with `df.collect()` and `assertRowDataEquals`.

### PySpark Testing Constraints

1. **Python 3.13+ is required** — PySpark 4.x wheels are not available for Python 3.10 (the system Python). Tests won't run on this dev machine.
2. **SparkSession in local mode** needs `master("local[*]")` and `spark.driver.memory "4g"` — significant resource requirement.
3. **Session reuse critical** — create the SparkSession ONCE at module/conftest level, not per test function. Py4J gateway startup is the bottleneck.
4. **JARs still needed** — S3A tests (writing/reading from MinIO) need `hadoop-aws` + AWS SDK bundle on the classpath. For unit tests, avoid S3A — test the transformation logic with in-memory DataFrames.

### Recommended First Phase (for Proposal)

The proposal should suggest splitting this into **two work units**:

**Work Unit 1** (small, independent, immediate value):
- Add `pytest` to dev deps
- Write tests for `orakel/models/kpi.py` (pure functions, trivial to test)
- Write tests for `orakel/utils/rate_limiter.py` (core state machine)
- Write tests for `orakel/clients/raiderio.py` (HTTP mocking with `responses`)
- Configure `pytest-cov`
- **Total**: ~80 tests, 1-2 days

**Work Unit 2** (Spark-dependent, needs env setup):
- Add `chispa` (or fallback assertion helpers) to dev deps
- Create SparkSession fixture in `conftest.py`
- Write tests for `orakel/pipeline/bronze.py` (row transforms)
- Write tests for `orakel/pipeline/silver.py` (dedup, roster flatten, fuzzy join)
- Write tests for `orakel/pipeline/gold.py` (KPI 1-4 UDFs, dim tables)
- Write tests for `orakel/clients/warcraftlogs.py` (OAuth flow, retries, rate limiting)
- **Total**: ~120 tests, 3-4 days

### Risks

- **PySpark 4.x + chispa compatibility unknown** — chispa may not support the PySpark 4.x API. If not, pipeline tests must use manual assertion helpers, adding ~100 lines of boilerplate.
- **No Python 3.13 on this dev machine** — Spark tests cannot run locally until Python 3.13 is available (via `uv python install 3.13` or a container).
- **SparkSession memory requirements** — 4g driver memory is significant for CI runners. Consider `spark.driver.memory "2g"` for tests and reduce `spark.sql.shuffle.partitions`.
- **Parquet fixture maintenance** — schemas may drift between Bronze/Silver/Gold. Fixture generation scripts must be kept in sync with schema changes. Consider generating fixtures programmatically in conftest.py instead of checking binary files in.
- **Test isolation** — SparkSession state leaking between tests can cause flaky failures. Ensure each test gets clean DataFrames via `df.clone()` or fresh `createDataFrame()` calls.
- **uv group quoting** — `[dependency-groups]` keys with both `=` and `>`, `<` can cause parsing issues. Pin test dependencies to major versions only.
- **400-line budget** — Work Unit 2 alone (~120 tests across pipeline modules) estimated at ~600-800 lines of test code. This **exceeds the 400-line review budget**. The delivery strategy (auto-chain vs single PR) must be decided before `sdd-tasks`.

### Ready for Proposal

**Yes.** This exploration is complete and grounded in real code analysis. Key decision points for the proposal:

1. **Split or single Work Unit?** — Strongly recommend splitting into 2 work units (non-Spark + Spark-dependent) to ship value early.
2. **chispa or manual assertions?** — Need to verify PySpark 4.x compatibility before committing.
3. **`tests/` directory structure** — Use standard `tests/` mirroring `orakel/` package structure.
4. **CI integration** — Recommend GitHub Actions with `uv` setup. Unit tests on every push; pipeline tests nightly.
5. **Python 3.13 availability** — Either `uv python install 3.13` or Docker-based dev environment for Spark tests.

### Return Envelope

**Status**: success
**Summary**: Exploration complete for `agregar-tests`. The project has zero test infrastructure. Three approaches were compared (minimal, full, tiered). Tiered testing recommended: fast unit tests (pytest + mock) for pure logic + API clients, and Spark-gated tests for pipeline modules.
**Artifacts**: `.openspec/changes/agregar-tests/exploration.md` | Engram `sdd/agregar-tests/explore`
**Next**: sdd-propose
**Risks**: PySpark 4.x + chispa compatibility uncertain; no Python 3.13 on dev machine; SparkSession memory overhead; 400-line budget exceeded
**Skill Resolution**: paths-injected — 2 skills (_shared, sdd-explore)
