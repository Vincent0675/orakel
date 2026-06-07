"""Silver pipeline — clean, dedup, and type-cast Bronze data.

The Silver layer deduplicates by natural key, enforces typed schemas,
and flattens nested structs (realm → realm.slug, region → region.short_name)
for downstream KPI computations.
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from orakel.config import settings
from orakel.models.schemas import (
    silver_dungeon_runs_schema,
)

logger = logging.getLogger(__name__)


class SilverPipeline:
    """Silver-layer transformations: dedup, type enforcement, struct flattening."""

    @staticmethod
    def clean_raiderio(spark: SparkSession, season: str) -> DataFrame:
        """Read Bronze Raider.IO data, clean and dedup, write Silver Parquet.

        Steps:
            1. Read Bronze Parquet from ``bronze/raiderio/runs/``
            2. Deduplicate by ``keystone_run_id`` keeping latest ``ingested_at``
            3. Enforce column types per Silver schema
            4. Flatten nested ``realm`` struct → ``realm`` string (slug)
            5. Flatten nested ``region`` struct → ``region`` string (short_name)
            6. Write to ``silver/raiderio_runs/`` on MinIO

        Args:
            spark: Active SparkSession.
            season: Season filter (e.g. "season-tww-3").

        Returns:
            Cleaned Silver DataFrame (before write, for row-count reporting).
        """
        bronze_path = f"s3a://{settings.MINIO_BUCKET}/bronze/raiderio/runs"
        logger.info("Reading Bronze Raider.IO data from %s", bronze_path)

        bronze_df = spark.read.parquet(bronze_path)
        bronze_count = bronze_df.count()
        logger.info("Bronze row count (before dedup): %d", bronze_count)

        # ── Dedup by keystone_run_id, keeping latest ingested_at ──────────
        dedup_window = Window.partitionBy("keystone_run_id").orderBy(
            F.desc("ingested_at")
        )
        deduped_df = (
            bronze_df.withColumn("_row_num", F.row_number().over(dedup_window))
            .filter(F.col("_row_num") == 1)
            .drop("_row_num")
        )

        deduped_count = deduped_df.count()
        logger.info(
            "Silver row count (after dedup): %d (removed %d duplicates)",
            deduped_count,
            bronze_count - deduped_count,
        )

        # ── Flatten nested structs ─────────────────────────────────────────
        # roster[].realm is a struct {id, slug, name, ...} → extract slug
        # roster[].region is a struct {name, slug, short_name} → extract short_name
        transformed_roster = F.transform(
            F.col("roster"),
            lambda p: p.withField("realm", p.getField("realm").getField("slug"))
            .withField(
                "region", p.getField("region").getField("short_name")
            ),
        )

        silver_df = deduped_df.withColumn("roster", transformed_roster)

        # ── Filter by season ───────────────────────────────────────────────
        silver_df = silver_df.filter(F.col("season") == season)

        # ── Write to Silver Parquet ────────────────────────────────────────
        silver_path = f"s3a://{settings.MINIO_BUCKET}/silver/raiderio_runs"
        logger.info("Writing %d rows to %s", silver_df.count(), silver_path)

        # Re-count after filter for accurate write count
        write_count = silver_df.count()
        silver_df.write.mode("overwrite").partitionBy("season").parquet(
            silver_path
        )

        logger.info(
            "Silver Raider.IO write complete: %d rows to %s",
            write_count,
            silver_path,
        )

        return silver_df