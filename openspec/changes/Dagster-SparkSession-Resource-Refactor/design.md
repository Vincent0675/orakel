# Design: Dagster SparkSession Resource Refactor

## Technical Approach

Create a single `SparkResource` via `@resource` that yields one shared `SparkSession` for the entire pipeline run, switch `daily_pipeline_job` to `in_process_executor`, and mechanically migrate all 37 assets/checks across 8 files from `get_spark_session()` + `try/finally: spark.stop()` to `context.resources.spark`. This eliminates the P0 bug where parallel SparkSessions on port 4040 produce corrupt Gold tables.

## Architecture Decisions

| Decision | Choice | Alternatives | Rationale |
|----------|--------|--------------|----------|
| Resource pattern | `@resource` generator with yield + finally stop | Class-based `ConfigurableResource` | Generator is simpler; single session, no config needed. `yield`/`finally` guarantees cleanup even on error. |
| Executor | `in_process_executor` | Keep `multiprocess_executor` with session isolation | In-process eliminates the 4040 conflict root cause. Pipeline is CPU-bound by Spark; multiprocess gains nothing. |
| Asset access pattern | `context.resources.spark` directly | Extract helper function | Direct access is clearer, fewer abstractions. Each asset already has unique logic — no DRY gain from a wrapper. |
| Helper refactoring | Add `spark` parameter to existing helpers | Remove helpers, inline | Helpers (`_run_cr`, `_run_ri`, `_run_sd`) encapsulate check logic; just change their session acquisition. Single call site each. |
| Dead code | Delete `_read_parquet` | Keep | Zero callers in codebase; dead since PR2 refactor. |
| IO Manager | No changes | Pass spark via resource to IOManager | `MinIOIOManager.handle_output` calls `SparkSession.builder.getOrCreate()` — under `in_process_executor` this picks up the shared session automatically. |

## Data Flow

```
daily_pipeline_job (in_process_executor)
    │
    ├─ spark_resource (yield SparkSession)
    │       │
    │       └─ get_spark_session("orakel_pipeline")
    │          ← shared session created once per run
    │
    ├─ Bronze assets  ──→ context.resources.spark ──┐
    ├─ Silver assets  ──→ context.resources.spark ──┤
    ├─ Gold assets    ──→ context.resources.spark ──┤
    ├─ asset_checks   ──→ context.resources.spark ──┤  same JVM,
    │                                                │  same session
    └─ MinIOIOManager  ──→ .getOrCreate() ──────────┘  (reuses)
                                                    │
                                                    ▼
                                              spark.stop()
                                         (resource teardown)
```

**IO Manager interaction**: `MinIOIOManager` calls `SparkSession.builder.getOrCreate()` in both `handle_output` and `load_input`. Under `in_process_executor`, there is one JVM per process — `getOrCreate()` returns the existing session. No changes needed.

## File Changes

| File | Action | Lines | Description |
|------|--------|-------|-------------|
| `orakel/pipeline/definitions.py` | Modify | +12 | Add `spark_resource`, `in_process_executor`, `resources={"spark": spark_resource}` |
| `orakel/pipeline/assets/gold.py` | Modify | ~-30 | Remove 10× `get_spark_session` + `try/finally: spark.stop()`; add `required_resource_keys`; use `context.resources.spark` |
| `orakel/pipeline/assets/silver.py` | Modify | ~-12 | Same pattern × 3 assets; `silver_player_performance` reads via `context.resources.spark` |
| `orakel/pipeline/assets/bronze.py` | Modify | ~-16 | Same pattern × 4 assets; **`bronze_wcl`**: remove outer try/finally, keep inner exception handling |
| `orakel/pipeline/assets/checks.py` | Modify | ~-45 | 15 checks: remove `get_spark_session` + `try/finally`; add `required_resource_keys`; **delete `_read_parquet`** |
| `orakel/pipeline/assets/checks_completeness.py` | Modify | ~-5 | Refactor `_run_cr(spark, …)` — add `spark` param, remove `get_spark_session`/`try/finally`; composite check uses `context.resources.spark` directly |
| `orakel/pipeline/assets/checks_referential.py` | Modify | ~-5 | Refactor `_run_ri(spark, …)` — same pattern as `_run_cr` |
| `orakel/pipeline/assets/checks_schema.py` | Modify | ~-5 | Refactor `_run_sd(spark, …)` — same pattern; composite check uses `context.resources.spark` directly |

**Total**: ~8 new, ~-118 removed = net ~-110 lines across 8 files. Approximate final delta: ~201 lines changed.

## Interfaces / Contracts

### SparkResource (`definitions.py`)

```python
from dagster import resource
from orakel.utils.minio import get_spark_session

@resource
def spark_resource(init_context):
    """Provide a shared SparkSession for the entire pipeline run."""
    spark = get_spark_session("orakel_pipeline")
    try:
        yield spark
    finally:
        spark.stop()
```

### Executor configuration (`definitions.py`)

```python
from dagster import in_process_executor

daily_pipeline_job = define_asset_job(
    name="daily_pipeline",
    selection="*",
    executor_def=in_process_executor,
)
```

### Per-asset migration template

**Before** (asset):
```python
@asset(key_prefix=["orakel"])
def gold_dim_dungeon(context: AssetExecutionContext) -> Output:
    spark = get_spark_session("gold_dim_dungeon")
    try:
        # ... asset logic ...
    finally:
        spark.stop()
```

**After** (asset):
```python
@asset(key_prefix=["orakel"], required_resource_keys={"spark"})
def gold_dim_dungeon(context: AssetExecutionContext) -> Output:
    spark = context.resources.spark
    # ... asset logic (no try/finally) ...
```

**Before** (asset_check):
```python
@asset_check(asset=AssetKey(["orakel", "bronze_rio"]))
def bronze_rio_checks() -> dict:
    spark = get_spark_session("check_bronze_rio")
    try:
        # ... check logic ...
    finally:
        spark.stop()
```

**After** (asset_check):
```python
@asset_check(asset=AssetKey(["orakel", "bronze_rio"]), required_resource_keys={"spark"})
def bronze_rio_checks(context) -> dict:
    spark = context.resources.spark
    # ... check logic (no try/finally) ...
```

### Helper refactors

**Before** (`_run_cr`):
```python
def _run_cr(upstream_path, downstream_path, min_ratio, season=None):
    spark = get_spark_session("cr_check")
    try:
        return check_completeness_ratio(spark, ...)
    finally:
        spark.stop()
```

**After** (`_run_cr`):
```python
def _run_cr(spark, upstream_path, downstream_path, min_ratio, season=None):
    return check_completeness_ratio(spark, ...)
```

Same pattern for `_run_ri` and `_run_sd`.

## Special Cases

### `bronze_wcl` — nested exception handling

**Before**: outer `try/finally: spark.stop()` wraps an inner `try/except Exception` that returns `Output(value=0)` for graceful degradation.

**After**: Remove only the outer `spark = get_spark_session()` / `try` / `finally: spark.stop()`. The inner `try/except` with early returns stays unchanged. The `context.resources.spark` assignment replaces the `spark = get_spark_session("bronze_wcl")` line.

### `ml_model` — multiple early returns

**Before**: `spark = get_spark_session("ml_model")` + `try/finally: spark.stop()`. Two early `return` paths (`< 10 rows`, `train_model` skip).

**After**: Replace with `spark = context.resources.spark`. Early returns are safe — no `spark.stop()` needed since the resource manages lifecycle.

### Composite checks (`cr_silver_rio_to_gold_kpis_composite_check`, `sd_gold_kpis_composite_check`)

These create their own session for multi-KPI loops. Migrate to `required_resource_keys={"spark"}` + `context.resources.spark`. Loop logic unchanged.

### `check_minio_state` — exception-based flow

**Before**: `try/except/finally` with `raise Failure(...)` in except. **After**: Remove outer session init/finally. The `raise Failure` stays — resource teardown still runs.

## Testing Strategy

| Layer | What | Approach |
|-------|------|----------|
| Unit | `spark_resource` yields and stops | Mock `get_spark_session`; verify yield + stop on cleanup |
| Unit | `_run_cr`, `_run_ri`, `_run_sd` accept spark param | Call with mock spark; verify delegation |
| Integration | Pipeline end-to-end | Run `daily_pipeline_job` with `in_process_executor`; verify all 37 assets/checks produce correct output; no 4040 conflicts |
| Integration | IO Manager reuses session | Verify `MinIOIOManager.handle_output` gets the same session as assets |

## Migration / Rollout

No data migration. Single commit revert. If issues arise:

1. `git revert <sha>` — restores `multiprocess_executor` and per-asset `get_spark_session`
2. No schema changes, no feature flags needed
3. Verify by checking Spark UI (port 4040 should show one session, not multiple)

## Open Questions

None — all decisions resolved in proposal.