#!/usr/bin/env python3
"""Silver → Gold pipeline job.

Reads Silver Raider.IO data, computes KPI 4 (Composition Synergy Score),
and builds dimension tables (dungeon, player, affix, spec).

Usage:
    uv run python scripts/silver_to_gold.py --season season-tww-3
"""

from __future__ import annotations

import argparse
import logging
import sys

from pyspark.sql import functions as F

from orakel.config import settings
from orakel.pipeline.gold import GoldPipeline
from orakel.utils.minio import get_spark_session

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Transform Silver data into Gold KPIs and dimension tables"
    )
    parser.add_argument(
        "--season",
        default=settings.SEASON,
        help=f"Season identifier (default: {settings.SEASON})",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    logger.info("Starting Silver → Gold pipeline for season=%s", args.season)

    spark = get_spark_session("silver_to_gold")

    results: dict[str, int] = {}

    try:
        # KPI 4: Composition Synergy Score
        logger.info("Computing KPI 4 — Composition Synergy Score...")
        kpi4_df = GoldPipeline.compute_kpi_synergy(spark, season=args.season)
        results["kpi_composition_synergy"] = kpi4_df.count()

        # Dimension tables
        logger.info("Building dim_dungeon...")
        dim_dungeon = GoldPipeline.build_dim_dungeon(spark, season=args.season)
        results["dim_dungeon"] = dim_dungeon.count()

        logger.info("Building dim_player...")
        dim_player = GoldPipeline.build_dim_player(spark, season=args.season)
        results["dim_player"] = dim_player.count()

        logger.info("Building dim_affix...")
        dim_affix = GoldPipeline.build_dim_affix(spark, season=args.season)
        results["dim_affix"] = dim_affix.count()

        logger.info("Building dim_spec...")
        dim_spec = GoldPipeline.build_dim_spec(spark)
        results["dim_spec"] = dim_spec.count()

        # Summary
        logger.info("=" * 60)
        logger.info("Gold pipeline complete — Summary:")
        for table, count in results.items():
            logger.info("  %s: %d rows", table, count)
        logger.info("=" * 60)

        # Verify KPI 4 results
        logger.info("KPI 4 verification:")
        kpi4_df.show(20, truncate=False)
        kpi4_df.printSchema()

        # Check for NULL synergy_scores with sample_count < 2
        null_scores = kpi4_df.filter(
            (F.col("sample_count") < 2) & F.col("synergy_score").isNotNull()
        ).count()
        if null_scores > 0:
            logger.warning(
                "Found %d rows with sample_count < 2 but non-NULL synergy_score!",
                null_scores,
            )
        else:
            logger.info("✓ NULL synergy_score for sample_count < 2 verified")

        # Check non-null scores for comps with ≥2 samples
        valid_scores = kpi4_df.filter(
            (F.col("sample_count") >= 2) & F.col("synergy_score").isNotNull()
        ).count()
        logger.info("✓ %d comp groups with valid synergy scores", valid_scores)

    except Exception:
        logger.exception("Gold pipeline failed")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()