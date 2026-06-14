# Tasks: Dagster SparkSession Resource Refactor

## Review Workload Forecast

| Field | Value |
|-------|-------|
| Estimated changed lines | ~201 (net -110 after deletions) |
| 400-line budget risk | Low |
| Chained PRs recommended | No |
| Suggested split | Single PR |
| Delivery strategy | single-pr |
| Chain strategy | pending |

Decision needed before apply: No
Chained PRs recommended: No
Chain strategy: pending
400-line budget risk: Low

## Phase 1: Infrastructure — definitions.py

- [x] 1.1 Add imports to `orakel/pipeline/definitions.py`: `from dagster import in_process_executor, resource` and `from orakel.utils.minio import get_spark_session`
- [x] 1.2 Add `spark_resource` to `orakel/pipeline/definitions.py`: `@resource` generator yielding `get_spark_session("orakel_pipeline")`, teardown calls `spark.stop()` in `finally`
- [x] 1.3 Add `executor_def=in_process_executor` to `daily_pipeline_job` in `orakel/pipeline/definitions.py`
- [x] 1.4 Add `resources={"spark": spark_resource}` to `Definitions` object in `orakel/pipeline/definitions.py`

**Verification**: `python -c "from orakel.pipeline.definitions import defs; ..."` runs without import errors. ✓ (17 assets, 34 asset check keys, `in_process` executor, `['spark']` resource defs)

## Phase 2: Gold Assets — gold.py (10 assets)

- [x] 2.1 Remove `from orakel.utils.minio import get_spark_session` from `orakel/pipeline/assets/gold.py`
- [x] 2.2 Add `required_resource_keys={"spark"}` to `@asset` decorator on `gold_dim_dungeon`, change `spark = get_spark_session(...)` → `spark = context.resources.spark`, remove `try/finally: spark.stop()`
- [x] 2.3 Same for `gold_dim_player`
- [x] 2.4 Same for `gold_dim_affix`
- [x] 2.5 Same for `gold_dim_spec`
- [x] 2.6 Same for `gold_kpi_death_clock`
- [x] 2.7 Same for `gold_kpi_healer_deficit`
- [x] 2.8 Same for `gold_kpi_interrupt_rate`
- [x] 2.9 Same for `gold_kpi_synergy`
- [x] 2.10 Same for `gold_features`; keep `merge_write` import
- [x] 2.11 For `ml_model`: replace `spark = get_spark_session(...)` + outer `try/finally` with `spark = context.resources.spark`; keep both early returns + nested `try/except`

**Verification**: `grep -n "get_spark_session" orakel/pipeline/assets/gold.py` returns zero matches. ✓

## Phase 3: Silver Assets — silver.py (3 assets)

- [x] 3.1 Remove `get_spark_session` import
- [x] 3.2 Migrate `silver_raiderio`: add `required_resource_keys={"spark"}`, `spark = context.resources.spark`, remove `try/finally`
- [x] 3.3 Migrate `silver_dungeon_runs`: same pattern
- [x] 3.4 Migrate `silver_player_performance`: same pattern

**Note**: tasks.md originally mentioned `silver_bronze_players` and `silver_raid_stats` but the actual file contains `silver_raiderio`, `silver_dungeon_runs`, and `silver_player_performance`. Migrated the 3 real assets.

**Verification**: `grep -n "get_spark_session" orakel/pipeline/assets/silver.py` returns zero matches. ✓

## Phase 4: Bronze Assets — bronze.py (4 assets)

- [x] 4.1 Remove `get_spark_session` import
- [x] 4.2 Migrate `bronze_rio`: add `required_resource_keys={"spark"}`, `spark = context.resources.spark`, remove `try/finally`
- [x] 4.3 Migrate `bronze_wcl`: remove only outer `spark = get_spark_session(...)` / `try` / `finally: spark.stop()`; keep inner `try/except` and both early returns unchanged
- [x] 4.4 Migrate `check_minio_state`: remove outer `try/finally`; `raise Failure(...)` in except block stays; resource teardown handles cleanup
- [x] 4.5 Migrate `match_manifest`: remove outer `try/finally`; nested `try/except` stays

**Verification**: `grep -n "get_spark_session" orakel/pipeline/assets/bronze.py` returns zero matches. ✓

## Phase 5: Checks — checks.py (15 checks + dead code removal)

- [x] 5.1 Remove `from orakel.utils.minio import get_spark_session` from `orakel/pipeline/assets/checks.py`
- [x] 5.2 Delete entire `_read_parquet` function (lines 26–39) — has zero callers
- [x] 5.3 Renumber remaining lines after deletion; verify file is syntactically valid
- [x] 5.4 Migrate all 15 checks: add `required_resource_keys={"spark"}` to each `@asset_check` decorator, change to `spark = context.resources.spark`, remove `try/finally: spark.stop()` per check

**Note**: The file uses `from __future__ import annotations`, which made the `context: AssetCheckExecutionContext` annotation resolve to a string. Removed the annotation on all 15 check functions (Dagster's `_validate_context_type_hint` accepts a blank annotation). Also removed the now-unused `AssetCheckExecutionContext` import from all 4 check files.

**Verification**: `grep -n "get_spark_session\|_read_parquet" orakel/pipeline/assets/checks.py` returns zero matches. ✓

## Phase 6: Helper Refactors

### checks_completeness.py
- [x] 6.1 Remove `from orakel.utils.minio import get_spark_session` from `orakel/pipeline/assets/checks_completeness.py`
- [x] 6.2 Refactor `_run_cr(upstream_path, …)` → `_run_cr(spark, upstream_path, …)`: accept spark as first param, remove internal session creation and `try/finally`
- [x] 6.3 Update `cr_silver_rio_to_gold_kpis_composite_check`: add `required_resource_keys={"spark"}`, use `context.resources.spark` directly in loop
- [x] 6.3b Migrate the 4 simple callers (CR-1, CR-2, CR-3, CR-5) to inject `context.resources.spark`

### checks_referential.py
- [x] 6.4 Remove `from orakel.utils.minio import get_spark_session` from `orakel/pipeline/assets/checks_referential.py`
- [x] 6.5 Refactor `_run_ri(upstream_path, …)` → `_run_ri(spark, upstream_path, …)`: accept spark as first param, remove internal session creation and `try/finally`
- [x] 6.5b Migrate all 7 callers (RI-1, RI-2, RI-3a/b/c, RI-4, RI-5) to inject `context.resources.spark`

### checks_schema.py
- [x] 6.6 Remove `from orakel.utils.minio import get_spark_session` from `orakel/pipeline/assets/checks_schema.py`
- [x] 6.7 Refactor `_run_sd(upstream_path, …)` → `_run_sd(spark, upstream_path, …)`: accept spark as first param, remove internal session creation and `try/finally`
- [x] 6.8 Update `sd_gold_kpis_composite_check`: add `required_resource_keys={"spark"}`, use `context.resources.spark` directly in loop
- [x] 6.8b Migrate the 6 simple callers (SD-1..5, SD-7) to inject `context.resources.spark`

**Verification**: `grep -n "get_spark_session" orakel/pipeline/assets/checks_*.py` returns zero matches for all three files. ✓

## Phase 7: Verification

- [x] 7.1 `python -c "from orakel.pipeline.definitions import defs, daily_pipeline_job"` — all imports resolve ✓
- [x] 7.2 `grep -rn "get_spark_session" orakel/pipeline/assets/` — zero matches in source (only in stale .pyc bytecode) ✓
- [x] 7.3 `grep -rn "_read_parquet" orakel/pipeline/assets/` — zero matches in source ✓
- [x] 7.4 `python -m py_compile` on all 8 modified files — all compile ✓
- [x] 7.5 `grep "in_process_executor" orakel/pipeline/definitions.py` — present (lines 15, 99, 115) ✓
- [x] 7.6 `grep "spark_resource" orakel/pipeline/definitions.py` — present (lines 91, 188) ✓
- [x] 7.7 `daily_pipeline_job.executor_def.name` == `"in_process"` (runtime check) ✓
- [x] 7.8 `defs.resolve_asset_graph()` returns 17 assets + 34 asset check keys (runtime check) ✓
