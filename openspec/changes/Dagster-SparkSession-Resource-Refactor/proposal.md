# Proposal: Dagster SparkSession Resource Refactor

## Intent

Fix P0 bug where Dagster's `multiprocess_executor` spawns parallel SparkSessions on port 4040, producing 0-row/corrupt Gold tables. Solution: shared SparkSession via Dagster resource + sequential execution.

## Scope

### In Scope
- 37 assets/checks across 8 files: `definitions.py`, `gold.py`, `silver.py`, `bronze.py`, `checks.py`, `checks_completeness.py`, `checks_referential.py`, `checks_schema.py`
- `SparkResource` in `definitions.py` using `@resource` generator pattern
- `in_process_executor` on `daily_pipeline_job`
- Per-asset/check: migrate `get_spark_session(...)` → `context.resources.spark`, remove outer try/finally, add `required_resource_keys={"spark"}`
- Remove dead `_read_parquet` from `checks.py`
- Helper refactors: `_run_cr`, `_run_ri`, `_run_sd` — receive spark as parameter from caller

### Out of Scope
- `orakel/utils/minio.py` — keep `get_spark_session` as factory
- `orakel/pipeline/io_managers.py` — `getOrCreate()` picks up shared session
- `orakel/pipeline/{gold,silver,bronze}.py` (classes) — take `spark` as param
- `scripts/*.py` — standalone, use `get_spark_session` directly

## Capabilities

### New Capabilities
- `spark-resource`: Reusable Dagster resource providing shared SparkSession via `@resource` generator. Auto-stops on run completion.

### Modified Capabilities
None — pure implementation refactor. No spec-level behavior changes.

## Approach

1. **`spark_resource`**: `@resource` generator → `get_spark_session("orakel_pipeline")`, yield, `spark.stop()` in `finally`
2. **Executor**: Switch `daily_pipeline_job` to `in_process_executor`
3. **Registration**: `Definitions(…, resources={"spark": spark_resource})`
4. **Per-asset/check**: Add `required_resource_keys={"spark"}`, replace session init, remove outer try/finally
5. **Helpers**: `_run_cr(spark, …)`, `_run_ri(spark, …)`, `_run_sd(spark, …)` — callers pass `context.resources.spark`
6. **Dead code**: Delete `_read_parquet` from `checks.py`

## Affected Areas

| Area | Impact | Description |
|------|--------|-------------|
| `definitions.py` | Modified | Add `spark_resource`, executor switch, resource registration |
| `gold.py` (10 assets) | Modified | Migrate to `context.resources.spark` |
| `silver.py` (3 assets) | Modified | Same |
| `bronze.py` (4 assets) | Modified | Same; careful with `bronze_wcl` outer try/finally |
| `checks.py` (15 checks) | Modified | Same + remove `_read_parquet` |
| `checks_completeness.py` | Modified | Refactor `_run_cr` helper |
| `checks_referential.py` | Modified | Refactor `_run_ri` helper |
| `checks_schema.py` | Modified | Refactor `_run_sd` helper |

## Risks

| Risk | Likelihood | Mitigation |
|------|------------|------------|
| Outer try/finally removal breaks early returns in `bronze_wcl`/`ml_model` | Low | Early returns stay, only outer guard removed |
| `io_managers.py` `getOrCreate()` creates separate session | Low | `in_process_executor` = single JVM; `getOrCreate()` reuses existing session |
| Helper callers pass spark incorrectly | Low | Each helper has single call site — one change to verify |

## Rollback Plan

Revert single commit. All changes are mechanical — no migration or data impact.

## Dependencies

- Dagster built-in `@resource` and `in_process_executor` (no new packages)

## Success Criteria

- [ ] Pipeline runs clean with shared SparkSession (no 4040 conflicts)
- [ ] All 37 assets/checks produce correct output
- [ ] No `get_spark_session` calls remain in asset/check files (only in `spark_resource` and `minio.py`)
- [ ] Gold tables contain full row counts (no 0-row corruptions)
