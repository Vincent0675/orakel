# Proposal: agregar-tests

## Intent

Warning #1 — Orakel has **zero tests, zero CI, zero test config**. The entire Medallion pipeline (Bronze → Silver → Gold) runs on trust. This proposal introduces a hybrid test suite to prevent regressions, document expected behavior, and enable safe refactoring.

## Scope

### In Scope

- **Tier 1 — Pure unit tests**: `kpi.py`, `config.py`, `rate_limiter.py`, `bronze.py::_run_to_row`, `schemas.py` (schema validation). Fast pytest, no Spark.
- **Tier 2 — Pipeline tests**: bronze/silver/gold transforms via `chispa` + SparkSession in local mode, gated behind `@pytest.mark.spark`.
- **Test infrastructure**: `pyproject.toml` dev deps, `tests/` directory structure, conftest with fixtures.
- **Branch**: `feat/agregar-tests` from `main`.
- **Delivery**: Split into 2 work units → 2 PRs with `ask-on-risk` strategy.

### Out of Scope

- End-to-end / integration tests with real MinIO or APIs — mocked at the client layer.
- CI/CD pipeline config (recommended but deferred).
- Performance or load tests.
- Scripts (`scripts/`) — thin CLI wrappers, tested transitively via pipeline tests.

## Capabilities

### New Capabilities

None — this is test infrastructure, no new user-facing or system capabilities.

### Modified Capabilities

None — no spec-level behavior changes.

## Approach

**Hybrid tiered testing** (Approach 3 from exploration):

1. **Tier 1** — `pytest` for all pure Python modules. Fast, no JVM, runs everywhere.
2. **Tier 2** — `pytest` + `chispa` + `SparkSession` for pipeline transforms. Gated behind `@pytest.mark.spark`. Uses `responses` for HTTP client mocking.
3. Dev deps in `[dependency-groups]` via `uv`:

   ```toml
   [dependency-groups]
   test = [
       "pytest>=8",
       "pytest-cov>=6",
       "responses>=0.25",
       "chispa>=0.12",
   ]
   ```

4. Structure: `tests/` mirrors `orakel/` package layout, standard conftest pattern.

## Affected Areas

| Area | Impact | Description |
|------|--------|-------------|
| `pyproject.toml` | Modified | Add `[dependency-groups]` for test deps |
| `tests/` | New | Full test directory hierarchy |
| `tests/conftest.py` | New | Shared fixtures (SparkSession, mock clients) |
| `tests/test_models/` | New | kpi.py, schemas.py tests |
| `tests/test_utils/` | New | rate_limiter.py tests |
| `tests/test_clients/` | New | raiderio.py, warcraftlogs.py tests |
| `tests/test_pipeline/` | New | bronze/silver/gold tests (Tier 2) |

## Risks

| Risk | Likelihood | Mitigation |
|------|------------|------------|
| chispa + PySpark 4.1.x compat | Low | `uv run` to verify; fallback to raw `assert_df_equality` |
| SparkSession 4GB memory on CI | Medium | Marker-gated; conditional CI job with `SPARK_LOCAL_IP` tuning |
| Parquet fixture drift in Tier 2 | Low | Generate fixtures from known schema versions |
| Exceeds 400-line review budget | High | Split into 2 PRs (Tier 1 / Tier 2) via `ask-on-risk` |

## Rollback Plan

1. **Per work unit**: revert the commit, remove files added in that PR. `pyproject.toml` dev deps don't affect production.
2. **Full rollback**: `git revert` the merge commits in reverse order (Tier 2 first, then Tier 1). Delete `tests/` directory if empty.

## Dependencies

- `uv` — already present, add dev deps only.
- Python 3.13+ — already satisfied.
- PySpark 4.1.2 — already installed.
- chispa 0.12.0+ — verify on first `uv sync --group test`.

## Success Criteria

- [ ] `uv run pytest tests/ -m "not spark"` passes with ≥80% coverage on pure modules.
- [ ] `uv run pytest tests/ -m spark` passes with SparkSession in local mode.
- [ ] Tests run without network access (all HTTP mocked, Parquet fixtures local).
- [ ] Total test count ≥ 150 across both tiers.
