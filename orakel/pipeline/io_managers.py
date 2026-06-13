"""Dagster IO Manager for incremental merge writes to MinIO via Spark S3A.

Replaces mode("overwrite") with upsert-by-key semantics:
  - If the target Parquet path exists, reads existing data and performs a
    full_outer join on the merge key(s).  New data wins on conflict
    (via coalesce).
  - If the path does not exist yet, appends (first write).
"""

from __future__ import annotations

import logging
from typing import Any

from dagster import InputContext, IOManager, OutputContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from orakel.config import settings

logger = logging.getLogger(__name__)


def path_exists(spark: SparkSession, path: str) -> bool:
    """Check whether a Parquet path exists on MinIO (S3A)."""
    try:
        spark.read.parquet(path).limit(1).collect()
        return True
    except Exception:
        return False


def merge_write(
    spark: SparkSession,
    df: DataFrame,
    path: str,
    merge_key: list[str],
) -> int:
    """Write *df* to *path* using merge-by-key semantics.

    If *path* does not exist yet, the data is written as a fresh Parquet
    dataset.  On subsequent runs the existing data is read, full-outer joined
    with the new data on *merge_key*, and the merged result overwrites the
    path.  In case of column conflicts new data wins (coalesce).

    Args:
        spark: Active SparkSession.
        df: New data to merge.
        path: S3A target path (e.g. ``s3a://orakel/silver/dungeon_runs``).
        merge_key: Columns that form the natural key for upsert.

    Returns:
        Row count of the merged DataFrame.
    """
    if path_exists(spark, path):
        logger.info("Merging into existing data at %s on key %s", path, merge_key)
        existing = spark.read.parquet(path)

        # Build coalesce expressions: new data wins, fall back to existing
        cols = []
        for c in df.columns:
            if c in existing.columns:
                cols.append(F.coalesce(df[c], existing[c]).alias(c))
            else:
                # Column only in new data
                cols.append(df[c])

        merged = existing.join(df, on=merge_key, how="full_outer").select(*cols)
        row_count = merged.count()
        merged.write.mode("overwrite").parquet(path)
        logger.info("Merge complete: %d rows at %s", row_count, path)
        return row_count
    else:
        row_count = df.count()
        logger.info("Initial write: %d rows to %s", row_count, path)
        df.write.mode("append").parquet(path)
        return row_count


class MinIOIOManager(IOManager):
    """Dagster IOManager that writes Spark DataFrames to MinIO via merge_write.

    The S3A path is built from ``key_prefix`` joined with the asset key.
    Each asset must configure its ``merge_key`` via resource config or metadata.
    """

    def handle_output(self, context: OutputContext, obj: Any) -> None:
        """Persist *obj* (DataFrame) to MinIO."""
        if obj is None:
            logger.info("Asset %s produced None — skipping write.", context.asset_key)
            return

        if isinstance(obj, DataFrame):
            spark = SparkSession.builder.appName(
                f"io_manager_{context.asset_key.path[-1]}"
            ).getOrCreate()
            try:
                merge_key = context.metadata.get("merge_key", []) if context.metadata else []
                if isinstance(merge_key, str):
                    merge_key = [merge_key]

                path = self._resolve_path(context)

                if merge_key:
                    row_count = merge_write(spark, obj, path, merge_key)
                else:
                    # No merge key → simple overwrite (dimension tables, etc.)
                    row_count = obj.count()
                    obj.write.mode("overwrite").parquet(path)
                    logger.info("Overwrite write: %d rows to %s", row_count, path)

                context.add_output_metadata({"row_count": row_count, "path": path})
            finally:
                # Don't stop the session — the asset body created it
                pass
        else:
            logger.warning("MinIOIOManager received non-DataFrame: %s", type(obj))

    def load_input(self, context: InputContext) -> Any:
        """Load a previously written DataFrame from MinIO."""
        # upstream_output is available on InputContext for getting metadata
        path = self._resolve_path_for_input(context)
        if path and path_exists(
            SparkSession.builder.appName("io_manager_load").getOrCreate(),
            path,
        ):
            return SparkSession.builder.appName("io_manager_load").getOrCreate().read.parquet(path)
        return None

    def _resolve_path(self, context: OutputContext) -> str:
        """Build the S3A path for an output asset."""
        parts = context.asset_key.path
        key_prefix = context.metadata.get("key_prefix", []) if context.metadata else []
        full_parts = list(key_prefix) + list(parts)
        relative = "/".join(full_parts)
        return f"s3a://{settings.MINIO_BUCKET}/{relative}"

    def _resolve_path_for_input(self, context: InputContext) -> str | None:
        """Resolve path for an input context (best-effort)."""
        # InputContext doesn't have asset_key directly, but we can compute from upstream
        if context.asset_key:
            parts = context.asset_key.path
            relative = "/".join(parts)
            return f"s3a://{settings.MINIO_BUCKET}/{relative}"
        return None