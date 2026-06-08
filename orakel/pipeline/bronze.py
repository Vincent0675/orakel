"""Bronze pipeline — ingest raw data from APIs into Bronze Parquet on MinIO.

This module provides the reusable API for Bronze ingestion, refactored from
the standalone ``scripts/ingest_raiderio.py`` script.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from orakel.clients.raiderio import RaiderIOClient
from orakel.config import settings
from orakel.models.schemas import bronze_raiderio_schema

logger = logging.getLogger(__name__)


def _run_to_row(run: dict[str, Any], season: str) -> dict[str, Any]:
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
        "score": float(run["score"]) if run.get("score") is not None else None,
        "rank": run.get("rank"),
        "season": season,
        "ingested_at": datetime.now(timezone.utc),
    }


def ingest_raiderio_runs(
    client: RaiderIOClient,
    spark: SparkSession,
    season: str,
    limit: int = 0,
) -> int:
    """Fetch Raider.IO runs and write to Bronze Parquet on MinIO.

    Reuses the logic from ``scripts/ingest_raiderio.py`` but as a reusable
    function callable from other pipeline modules.

    Args:
        client: Configured RaiderIOClient instance.
        spark: Active SparkSession.
        season: Season identifier (e.g. "season-tww-3").
        limit: Max pages to fetch (0 = unlimited).

    Returns:
        Number of rows written to Bronze.
    """
    all_rows: list[dict[str, Any]] = []
    page = 1

    while True:
        if limit and page > limit:
            logger.info("Reached page limit (%d). Stopping.", limit)
            break

        logger.info("Fetching page %d...", page)
        runs = client.fetch_runs(season=season, page=page)

        if not runs:
            logger.info("Empty page %d — end of data.", page)
            break

        for run in runs:
            row = _run_to_row(run, season)
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
        return 0

    df = spark.createDataFrame(all_rows, schema=bronze_raiderio_schema)

    # Add year/month partition columns from completed_at (fallback to ingested_at)
    partition_col = F.coalesce(df.completed_at, df.ingested_at)
    df = df.withColumn("year", F.year(partition_col)).withColumn(
        "month", F.month(partition_col)
    )

    write_path = f"s3a://{settings.MINIO_BUCKET}/bronze/raiderio/runs"
    logger.info("Writing %d rows to %s", len(all_rows), write_path)

    df.write.mode("overwrite").partitionBy("season", "year", "month").parquet(
        write_path
    )

    logger.info(
        "Bronze ingestion complete: %d pages, %d rows written to %s",
        page - 1,
        len(all_rows),
        write_path,
    )

    return len(all_rows)