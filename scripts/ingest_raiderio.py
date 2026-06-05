#!/usr/bin/env python3
"""Ingest Raider.IO Mythic+ runs into Bronze Parquet on MinIO.

Usage:
    uv run python scripts/ingest_raiderio.py --season season-tww-3 --limit 5
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone

from pyspark.sql import functions as F

from orakel.clients.raiderio import RaiderIOClient
from orakel.config import settings
from orakel.models.schemas import bronze_raiderio_schema
from orakel.utils.minio import get_spark_session

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest Raider.IO Mythic+ runs to Bronze Parquet"
    )
    parser.add_argument(
        "--season",
        default=settings.SEASON,
        help=f"Season identifier (default: {settings.SEASON})",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Max pages to fetch (0 = unlimited)",
    )
    return parser.parse_args()


def run_to_row(run: dict, season: str) -> dict:
    """Convert a single Raider.IO run dict to a row matching bronze_raiderio_schema."""
    roster_data = run.get("roster", [])
    roster = [
        {
            "name": player.get("character", {}).get("name"),
            "class": (
                player.get("character", {}).get("class", {}).get("name")
                if isinstance(player.get("character", {}).get("class"), dict)
                else player.get("character", {}).get("class")
            ),
            "spec": (
                player.get("character", {}).get("spec", {}).get("name")
                if isinstance(player.get("character", {}).get("spec"), dict)
                else player.get("character", {}).get("spec")
            ),
            "role": player.get("role"),
            "realm": player.get("character", {}).get("realm"),
            "region": player.get("character", {}).get("region"),
        }
        for player in roster_data
    ]

    completed_at_str = run.get("completed_at")
    completed_at = None
    if completed_at_str:
        try:
            # Raider.IO returns ISO 8601 timestamps
            completed_at = datetime.fromisoformat(
                completed_at_str.replace("Z", "+00:00")
            )
        except (ValueError, TypeError):
            logger.warning("Could not parse completed_at: %s", completed_at_str)

    weekly_modifiers = [
        mod.get("id") if isinstance(mod, dict) else mod
        for mod in run.get("weekly_modifiers", [])
    ]

    return {
        "source": "raiderio",
        "keystone_run_id": run.get("keystone_run_id", run.get("id")),
        "dungeon_id": run.get("dungeon", {}).get("id"),
        "challenge_mode_id": run.get("dungeon", {}).get("map_challenge_mode_id"),
        "dungeon_name": run.get("dungeon", {}).get("name"),
        "mythic_level": run.get("mythic_level"),
        "clear_time_ms": run.get("clear_time_ms"),
        "keystone_time_ms": run.get("keystone_time_ms"),
        "completed_at": completed_at,
        "weekly_modifiers": weekly_modifiers,
        "roster": roster,
        "score": run.get("score"),
        "rank": run.get("rank"),
        "season": season,
        "ingested_at": datetime.now(timezone.utc),
    }


def main() -> None:
    args = parse_args()

    logger.info("Starting Raider.IO Bronze ingestion for season=%s", args.season)

    spark = get_spark_session("ingest_raiderio")
    client = RaiderIOClient(api_key=settings.RAIDERIO_API_KEY or None)

    all_rows: list[dict] = []
    page = 1

    while True:
        if args.limit and page > args.limit:
            logger.info("Reached page limit (%d). Stopping.", args.limit)
            break

        logger.info("Fetching page %d...", page)
        runs = client.fetch_runs(season=args.season, page=page)

        if not runs:
            logger.info("Empty page %d — end of data.", page)
            break

        for run in runs:
            row = run_to_row(run, args.season)
            all_rows.append(row)

        logger.info(
            "Page %d: fetched %d runs (total: %d)",
            page,
            len(runs),
            len(all_rows),
        )
        page += 1

    if not all_rows:
        logger.warning("No runs fetched. Nothing to write.")
        spark.stop()
        return

    # Create DataFrame with explicit schema
    df = spark.createDataFrame(all_rows, schema=bronze_raiderio_schema)

    # Add year/month partition columns from completed_at (fallback to ingested_at)
    partition_col = F.coalesce(df.completed_at, df.ingested_at)
    df = df.withColumn("year", F.year(partition_col)).withColumn(
        "month", F.month(partition_col)
    )

    # Write to MinIO S3 as Parquet, partitioned by season/year/month
    write_path = f"s3a://{settings.MINIO_BUCKET}/bronze/raiderio/runs"
    logger.info("Writing %d rows to %s", len(all_rows), write_path)

    df.write.mode("overwrite").partitionBy("season", "year", "month").parquet(
        write_path
    )

    logger.info(
        "Ingestion complete: %d pages, %d rows written to %s",
        page - 1,
        len(all_rows),
        write_path,
    )

    # Verification: read back and show count
    read_df = spark.read.parquet(write_path)
    logger.info("Verification: read back %d rows from Bronze", read_df.count())
    read_df.printSchema()

    spark.stop()


if __name__ == "__main__":
    main()