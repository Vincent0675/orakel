# Spark Resource Specification

## Purpose

Provide a shared, lifecycle-managed SparkSession to all Dagster pipeline assets via resource injection, eliminating SparkSession conflicts from parallel executor spawns.

## Requirements

### Requirement: Shared SparkSession

The `spark_resource` MUST yield a single SparkSession created via `get_spark_session("orakel_pipeline")` that is shared across all assets and checks within a single pipeline run.

#### Scenario: Single session across all assets

- GIVEN a pipeline run with 37 assets/checks all requiring `context.resources.spark`
- WHEN the pipeline executes
- THEN each asset/check receives the SAME SparkSession instance via Dagster resource injection
- AND no asset/check calls `get_spark_session()` directly

#### Scenario: Sequential execution prevents port collisions

- GIVEN the `daily_pipeline_job` configured with `in_process_executor`
- WHEN the pipeline runs
- THEN assets execute sequentially within a single process
- AND no two assets access SparkSession concurrently on port 4040

### Requirement: Lifecycle Management

The `spark_resource` MUST start the SparkSession before any asset consumes it and MUST stop the SparkSession after the run completes, whether in success or failure.

#### Scenario: Clean shutdown on success

- GIVEN all 37 assets/checks complete successfully
- WHEN the pipeline finishes
- THEN `spark.stop()` is called by the resource teardown
- AND no orphaned SparkSession remains in the JVM

#### Scenario: Clean shutdown on failure

- GIVEN any asset or check fails mid-run
- WHEN the pipeline errors and terminates
- THEN `spark.stop()` is called by the resource teardown
- AND no orphaned SparkSession leaks resources

### Requirement: io_manager Compatibility

The `io_managers` that use `SparkSession.builder.getOrCreate()` MUST reuse the same SparkSession instantiated by `spark_resource`.

#### Scenario: getOrCreate reuses shared session

- GIVEN the `spark_resource` has started a SparkSession
- WHEN an io_manager calls `SparkSession.builder.getOrCreate()`
- THEN it returns the existing shared session (no new session on port 4040)

### Requirement: Dead Code Removal

The `_read_parquet` helper function in `checks.py` MUST be removed, as it depends on `get_spark_session()` and has no remaining callers.

#### Scenario: Function fully removed

- GIVEN all 15 checks use `context.resources.spark`
- WHEN the codebase is searched for `_read_parquet`
- THEN the function definition is not found in `checks.py`

## Acceptance Criteria

1. Pipeline runs clean with shared SparkSession — no 4040 port conflicts
2. All 37 assets/checks produce correct row counts (no 0-row corruptions)
3. No `get_spark_session()` calls remain in asset/check files (only in `spark_resource` and `minio.py`)
4. Gold tables contain full row counts matching pre-refactor output
