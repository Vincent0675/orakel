"""Silver-layer Dagster assets: clean, dedup, type-cast, fuzzy-join."""

import logging

from dagster import AssetExecutionContext, AssetKey, MetadataValue, Output, asset

from orakel.config import settings
from orakel.pipeline.io_managers import merge_write

logger = logging.getLogger(__name__)


@asset(
    key_prefix=["orakel"],
    deps=[AssetKey(["orakel", "bronze_rio"])],
    required_resource_keys={"spark"},
)
def silver_raiderio(context: AssetExecutionContext) -> Output:
    """Clean and deduplicate Bronze Raider.IO data into Silver.

    Wraps ``SilverPipeline.clean_raiderio()`` and writes to
    ``silver/raiderio_runs`` on MinIO.
    """
    from orakel.pipeline.silver import SilverPipeline

    spark = context.resources.spark
    # write=False: asset handles the write
    silver_df = SilverPipeline.clean_raiderio(spark, settings.SEASON, write=False)
    path = f"s3a://{settings.MINIO_BUCKET}/silver/raiderio_runs"
    row_count = silver_df.count()
    silver_df.write.mode("overwrite").partitionBy("season").parquet(path)
    context.log.info("Silver Raider.IO: %d rows", row_count)
    return Output(
        value=row_count,
        metadata={
            "row_count": MetadataValue.int(row_count),
            "season": MetadataValue.text(settings.SEASON),
        },
    )


@asset(
    key_prefix=["orakel"],
    deps=[AssetKey(["orakel", "silver_raiderio"]), AssetKey(["orakel", "match_manifest"]), AssetKey(["orakel", "bronze_wcl"])],
    required_resource_keys={"spark"},
)
def silver_dungeon_runs(
    context: AssetExecutionContext,
) -> Output:
    """Silver dungeon runs — enriched runs with WCL match metadata.

    Computes both dungeon_runs AND player_performance from
    ``SilverPipeline.apply_fuzzy_join()``, writes dungeon_runs with
    merge_write, and also writes player_performance for the companion
    ``silver_player_performance`` asset to read.

    Uses merge_write for incremental upsert by ``run_id``.
    """
    from orakel.pipeline.silver import SilverPipeline

    spark = context.resources.spark
    # Compute both outputs (but don't let the pipeline write)
    dungeon_runs_df, player_perf_df = SilverPipeline.apply_fuzzy_join(
        spark, settings.SEASON, write=False
    )

    # Write dungeon_runs with merge
    dr_path = f"s3a://{settings.MINIO_BUCKET}/silver/dungeon_runs"
    dr_count = merge_write(spark, dungeon_runs_df, dr_path, merge_key=["run_id"])

    # Also write player_performance so silver_player_performance asset can read it
    pp_path = f"s3a://{settings.MINIO_BUCKET}/silver/player_performance"
    pp_count = player_perf_df.count()
    player_perf_df.write.mode("overwrite").partitionBy("season").parquet(pp_path)

    context.log.info(
        "Silver dungeon_runs: %d rows (merged), player_performance: %d rows",
        dr_count,
        pp_count,
    )
    return Output(
        value=dr_count,
        metadata={
            "row_count": MetadataValue.int(dr_count),
            "season": MetadataValue.text(settings.SEASON),
        },
    )


@asset(
    key_prefix=["orakel"],
    deps=[AssetKey(["orakel", "silver_dungeon_runs"])],
    required_resource_keys={"spark"},
)
def silver_player_performance(
    context: AssetExecutionContext,
) -> Output:
    """Silver player performance — per-player combat stats.

    Reads the already-written player_performance data from
    ``silver_dungeon_runs`` (which computes both outputs together)
    and applies merge_write for incremental upsert.
    """
    from pyspark.sql import functions as F

    spark = context.resources.spark
    pp_path = f"s3a://{settings.MINIO_BUCKET}/silver/player_performance"
    # Read the already-written player_performance data
    player_perf_df = spark.read.parquet(pp_path).filter(
        F.col("season") == settings.SEASON
    )
    row_count = merge_write(
        spark, player_perf_df, pp_path, merge_key=["run_id", "player_name"]
    )
    context.log.info("Silver player_performance: %d rows (merged)", row_count)
    return Output(
        value=row_count,
        metadata={
            "row_count": MetadataValue.int(row_count),
            "season": MetadataValue.text(settings.SEASON),
        },
    )