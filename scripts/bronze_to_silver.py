#!/usr/bin/env python3
"""Bronze → Silver pipeline job.

Reads Bronze Raider.IO Parquet, cleans and deduplicates, and writes
Silver Parquet to MinIO.

Usage:
    uv run python scripts/bronze_to_silver.py --season season-tww-3
    uv run python scripts/bronze_to_silver.py --season season-tww-3 --limit 100
"""

from __future__ import annotations

import argparse
import logging
import sys

from orakel.config import settings
from orakel.pipeline.silver import SilverPipeline
from orakel.utils.minio import get_spark_session

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Transform Bronze Raider.IO data into Silver Parquet"
    )
    parser.add_argument(
        "--season",
        default=settings.SEASON,
        help=f"Season identifier (default: {settings.SEASON})",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    logger.info("Starting Bronze → Silver pipeline for season=%s", args.season)

    spark = get_spark_session("bronze_to_silver")

    try:
        silver_df = SilverPipeline.clean_raiderio(spark, season=args.season)

        if silver_df.count() == 0:
            logger.warning("No Silver data produced. Check Bronze data availability.")
            sys.exit(1)

        # Print summary
        row_count = silver_df.count()
        logger.info("Silver pipeline complete: %d rows written", row_count)
        silver_df.printSchema()

        # Verify dedup: show some sample keystone_run_ids
        logger.info("Sample keystone_run_ids (first 10):")
        silver_df.select("keystone_run_id").show(10, truncate=False)

    except Exception:
        logger.exception("Silver pipeline failed")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()