#!/usr/bin/env python3
"""Seed lookup tables from hardcoded data to MinIO Parquet.

Extracts _TWW3_DUNGEONS, _TWW3_AFFIXES, and _WOW_SPEC_ROLE_MAP
from orakel.pipeline.gold and writes them as Parquet lookup tables
to bronze/lookups/ on MinIO.

Usage:
    uv run python scripts/seed_lookups.py
"""

from __future__ import annotations

import logging
import sys

from pyspark.sql import SparkSession

from orakel.config import settings
from orakel.models.schemas import dim_affix_schema, dim_dungeon_schema, dim_spec_schema
from orakel.pipeline.gold import _TWW3_AFFIXES, _TWW3_DUNGEONS, _WOW_SPEC_ROLE_MAP
from orakel.utils.minio import get_spark_session

logger = logging.getLogger(__name__)

# ─── Lookup table paths ─────────────────────────────────────────────────────

LOOKUPS = [
    {
        "name": "dim_dungeon_timer",
        "data": _TWW3_DUNGEONS,
        "schema": dim_dungeon_schema,
        "path": "bronze/lookups/dim_dungeon_timer",
    },
    {
        "name": "dim_affix",
        "data": _TWW3_AFFIXES,
        "schema": dim_affix_schema,
        "path": "bronze/lookups/dim_affix",
    },
    {
        "name": "dim_spec_role",
        "data": _WOW_SPEC_ROLE_MAP,
        "schema": dim_spec_schema,
        "path": "bronze/lookups/dim_spec_role",
    },
]


def seed_lookup_tables(spark: SparkSession) -> None:
    """Write all lookup tables from hardcoded data to MinIO Parquet.

    Args:
        spark: Active SparkSession.
    """
    for lookup in LOOKUPS:
        name = lookup["name"]
        data = lookup["data"]
        schema = lookup["schema"]
        full_path = f"s3a://{settings.MINIO_BUCKET}/{lookup['path']}"

        df = spark.createDataFrame(data, schema=schema)
        row_count = df.count()
        df.write.mode("overwrite").parquet(full_path)
        logger.info("Seeded %s: %d rows to %s", name, row_count, full_path)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    spark = get_spark_session("seed_lookups")

    try:
        seed_lookup_tables(spark)
        logger.info("Lookup table seeding complete.")
    except Exception as e:
        logger.exception("Failed to seed lookup tables [%s]", type(e).__name__)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()