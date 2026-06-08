#!/usr/bin/env python3
"""Bronze → Silver pipeline job.

Reads Bronze Raider.IO Parquet, cleans and deduplicates, and writes
Silver Parquet to MinIO. Optionally processes WCL fuzzy join when
a match manifest exists.

Usage:
    uv run python scripts/bronze_to_silver.py --season season-tww-3
    uv run python scripts/bronze_to_silver.py --season season-tww-3 --with-wcl
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
        description="Transform Bronze data into Silver Parquet"
    )
    parser.add_argument(
        "--season",
        default=settings.SEASON,
        help=f"Season identifier (default: {settings.SEASON})",
    )
    parser.add_argument(
        "--with-wcl",
        action="store_true",
        help="Also process WCL fuzzy join (requires match manifest)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    logger.info("Starting Bronze → Silver pipeline for season=%s", args.season)

    spark = get_spark_session("bronze_to_silver")

    try:
        # Step 1: Clean Raider.IO data (always runs)
        silver_df = SilverPipeline.clean_raiderio(spark, season=args.season)

        if silver_df.count() == 0:
            logger.warning("No Silver data produced. Check Bronze data availability.")
            sys.exit(1)

        # Print summary
        row_count = silver_df.count()
        logger.info("Silver Raider.IO: %d rows written", row_count)
        silver_df.printSchema()

        # Verify dedup: show some sample keystone_run_ids
        logger.info("Sample keystone_run_ids (first 10):")
        silver_df.select("keystone_run_id").show(10, truncate=False)

        # Step 2: WCL fuzzy join (optional, requires match manifest)
        if args.with_wcl:
            logger.info("Processing WCL fuzzy join...")
            try:
                dungeon_runs_df, player_perf_df = SilverPipeline.apply_fuzzy_join(
                    spark, season=args.season
                )

                dr_count = dungeon_runs_df.count()
                pp_count = player_perf_df.count()
                logger.info("Silver dungeon_runs: %d rows", dr_count)
                logger.info("Silver player_performance: %d rows", pp_count)

                # Show sample data
                logger.info("Sample dungeon_runs (first 5):")
                dungeon_runs_df.select(
                    "run_id", "rio_run_id", "wcl_report_code", "confidence", "match_method"
                ).show(5, truncate=False)

                if pp_count > 0:
                    logger.info("Sample player_performance (first 5):")
                    player_perf_df.select(
                        "run_id", "player_name", "role", "class_name"
                    ).show(5, truncate=False)

            except Exception as e:
                logger.warning(
                    "WCL fuzzy join failed (this is expected if no match manifest exists): %s",
                    e,
                )
                logger.info(
                    "To enable WCL join: run scripts/match_reports.py first, "
                    "then scripts/ingest_warcraftlogs.py"
                )

        logger.info("Silver pipeline complete: %d rows", row_count)

    except Exception:
        logger.exception("Silver pipeline failed")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()