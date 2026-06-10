#!/usr/bin/env python3
"""Ingest WCL combat events for matched runs.

Reads the match manifest from Silver, then fetches DamageTaken,
Healing, and Interrupt events from WCL for each matched fight,
writing them to Bronze Parquet on MinIO.

Usage:
    uv run python scripts/ingest_warcraftlogs.py --season season-tww-3
    uv run python scripts/ingest_warcraftlogs.py --season season-tww-3 --limit 10
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone

import requests

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pyspark.sql.types import BooleanType, IntegerType, StringType, StructField, StructType, TimestampType

from orakel.clients.warcraftlogs import WCLAuthError, WCLRateLimitError, WarcraftLogsClient
from orakel.config import settings
from orakel.models.schemas import bronze_wcl_events_schema, bronze_wcl_reports_schema
from orakel.utils.minio import get_spark_session

# ─── Checkpoint Schema ─────────────────────────────────────────────────────────

CHECKPOINT_SCHEMA = StructType([
    StructField("report_code", StringType(), nullable=False),
    StructField("damage_taken_done", BooleanType(), nullable=False),
    StructField("healing_done", BooleanType(), nullable=False),
    StructField("interrupts_done", BooleanType(), nullable=False),
    StructField("fight_count", IntegerType(), nullable=False),
    StructField("events_count", IntegerType(), nullable=False),
    StructField("processed_at", TimestampType(), nullable=False),
])

logger = logging.getLogger(__name__)


def load_match_manifest(spark: SparkSession, season: str) -> list[dict]:
    """Load match manifest from Silver.

    Args:
        spark: Active SparkSession.
        season: Season filter.

    Returns:
        List of match dicts from the manifest.
    """
    path = f"s3a://{settings.MINIO_BUCKET}/silver/matches"
    logger.info("Loading match manifest from %s", path)
    df = spark.read.parquet(path).filter(F.col("season") == season)
    count = df.count()
    logger.info("Loaded %d match records", count)
    return [row.asDict() for row in df.collect()]


def ingest_damage_taken(
    wcl_client: WarcraftLogsClient,
    report_code: str,
    fight_ids: list[int],
) -> list[dict]:
    """Fetch DamageTaken table data for given fights.

    Args:
        wcl_client: Authenticated WCL client.
        report_code: WCL report code.
        fight_ids: List of fight IDs.

    Returns:
        List of damage event dicts.
    """
    events = []
    try:
        for fid in fight_ids:
            table = wcl_client.get_table(report_code, [fid], "DamageTaken")
            table_data = table.get("data", {})
            entries = table_data.get("entries", []) if isinstance(table_data, dict) else table.get("entries", [])
            for entry in entries:
                events.append({
                    "timestamp": 0,
                    "actor_id": entry.get("id", 0),
                    "player_name": entry.get("name", ""),
                    "source_id": entry.get("sourceID"),
                    "target_id": None,
                    "target_name": None,
                    "ability_id": 0,
                    "ability_name": entry.get("type", ""),
                    "damage_amount": entry.get("total", 0),
                    "damage_type": "damage_taken",
                    "fight_id": fid,
                    "report_code": report_code,
                })
    except (WCLRateLimitError, Exception) as e:
        logger.warning("Failed to fetch DamageTaken for %s fight %s: %s", report_code, fight_ids, e)
    return events


def ingest_healing(
    wcl_client: WarcraftLogsClient,
    report_code: str,
    fight_ids: list[int],
    actor_map: dict[int, str] | None = None,
) -> list[dict]:
    """Fetch Healing table data for given fights.

    Extracts per-TARGET healing from the WCL Healing table entries.
    Each source (healer) entry may contain a ``targets`` sub-array
    showing how much each target received.  When targets are available,
    we emit one row per (healer → target) pair so that the Silver
    layer can aggregate healing RECEIVED by each player (not just
    healing DONE by each healer).

    Falls back to source-level rows when ``targets`` is absent,
    retaining backward compatibility with older ingestion runs.

    Args:
        wcl_client: Authenticated WCL client.
        report_code: WCL report code.
        fight_ids: List of fight IDs.
        actor_map: Dict mapping actor_id → player_name from masterData.

    Returns:
        List of healing event dicts with target_name populated where available.
    """
    events = []
    try:
        for fid in fight_ids:
            table = wcl_client.get_table(report_code, [fid], "Healing")
            table_data = table.get("data", {})
            entries = table_data.get("entries", []) if isinstance(table_data, dict) else table.get("entries", [])

            for entry in entries:
                source_id = entry.get("id", 0)
                source_name = entry.get("name", "")

                # Try to extract per-target healing breakdown
                targets = entry.get("targets", [])
                if targets and isinstance(targets, list):
                    # Per-target breakdown available — emit one row per target
                    for target in targets:
                        target_id = target.get("id")
                        target_name_raw = target.get("name", "")
                        # Resolve target_name via actor_map if available
                        target_name = (
                            (actor_map or {}).get(target_id, target_name_raw)
                            if target_id is not None
                            else target_name_raw
                        )
                        events.append({
                            "timestamp": 0,
                            "actor_id": source_id,
                            "player_name": source_name,
                            "source_id": source_id,
                            "target_id": target_id,
                            "target_name": target_name,
                            "ability_id": 0,
                            "ability_name": entry.get("type", ""),
                            "damage_amount": target.get("total", 0),
                            "damage_type": "healing",
                            "fight_id": fid,
                            "report_code": report_code,
                        })
                else:
                    # No target breakdown — emit source-level row (backward compat)
                    events.append({
                        "timestamp": 0,
                        "actor_id": source_id,
                        "player_name": source_name,
                        "source_id": source_id,
                        "target_id": None,
                        "target_name": None,
                        "ability_id": 0,
                        "ability_name": entry.get("type", ""),
                        "damage_amount": entry.get("total", 0),
                        "damage_type": "healing",
                        "fight_id": fid,
                        "report_code": report_code,
                    })
    except (WCLRateLimitError, Exception) as e:
        logger.warning("Failed to fetch Healing for %s fight %s: %s", report_code, fight_ids, e)
    return events


def ingest_interrupts(
    wcl_client: WarcraftLogsClient,
    report_code: str,
    fight_ids: list[int],
    actor_map: dict[int, str] | None = None,
) -> list[dict]:
    """Fetch Interrupt events for given fights.

    Resolves actor_id → player_name via masterData (actor_map).

    Args:
        wcl_client: Authenticated WCL client.
        report_code: WCL report code.
        fight_ids: List of fight IDs.
        actor_map: Dict mapping actor_id → player_name from masterData.

    Returns:
        List of interrupt event dicts with resolved player_name.
    """
    events = []
    try:
        for fid in fight_ids:
            raw_events = wcl_client.get_events(report_code, [fid], "Interrupts")
            for evt in raw_events:
                source_id = evt.get("sourceID", 0)
                player_name = (actor_map or {}).get(source_id)
                events.append({
                    "timestamp": evt.get("timestamp", 0),
                    "actor_id": source_id,
                    "player_name": player_name,
                    "source_id": source_id,
                    "target_id": evt.get("targetID"),
                    "target_name": None,
                    "ability_id": evt.get("abilityGameID", 0),
                    "ability_name": str(evt.get("abilityGameID", "")),
                    "damage_amount": 0,
                    "damage_type": "interrupt",
                    "fight_id": fid,
                    "report_code": report_code,
                })
    except (WCLRateLimitError, Exception) as e:
        logger.warning("Failed to fetch Interrupts for %s fight %s: %s", report_code, fight_ids, e)
    return events


def write_checkpoint(
    spark: SparkSession,
    report_code: str,
    damage_taken_done: bool,
    healing_done: bool,
    interrupts_done: bool,
    fight_count: int,
    events_count: int,
) -> None:
    """Write a single checkpoint entry for a processed report.

    Args:
        spark: Active SparkSession.
        report_code: WCL report code.
        damage_taken_done: Whether DamageTaken was fetched.
        healing_done: Whether Healing was fetched.
        interrupts_done: Whether Interrupts was fetched.
        fight_count: Number of fights in the report.
        events_count: Total events fetched for this report.
    """
    row = [{
        "report_code": report_code,
        "damage_taken_done": damage_taken_done,
        "healing_done": healing_done,
        "interrupts_done": interrupts_done,
        "fight_count": fight_count,
        "events_count": events_count,
        "processed_at": datetime.now(timezone.utc),
    }]
    df = spark.createDataFrame(row, schema=CHECKPOINT_SCHEMA)
    path = f"s3a://{settings.MINIO_BUCKET}/bronze/warcraftlogs/checkpoints"
    logger.info("Writing checkpoint for report %s (%d events, %d fights)", report_code, events_count, fight_count)
    df.write.mode("append").parquet(path)


def write_bronze_events(spark: SparkSession, events: list[dict], event_type: str) -> None:
    """Write Bronze events to MinIO Parquet.

    Args:
        spark: Active SparkSession.
        events: List of event dicts.
        event_type: Event category for path (e.g., "damage_taken", "healing", "interrupts").
    """
    if not events:
        logger.warning("No %s events to write.", event_type)
        return

    df = spark.createDataFrame(events, schema=bronze_wcl_events_schema)
    path = f"s3a://{settings.MINIO_BUCKET}/bronze/warcraftlogs/events/{event_type}"
    row_count = df.count()
    logger.info("Writing %d %s events to %s", row_count, event_type, path)
    df.write.mode("append").parquet(path)


def write_bronze_reports(spark: SparkSession, reports: list[dict]) -> None:
    """Write Bronze WCL reports to MinIO Parquet.

    Args:
        spark: Active SparkSession.
        reports: List of report dicts with fight data.
    """
    if not reports:
        logger.warning("No WCL reports to write.")
        return

    # Transform reports to match schema
    rows = []
    for r in reports:
        fights_data = r.get("fights", [])
        fights_struct = []
        for f in (fights_data or []):
            fights_struct.append({
                "fight_id": f.get("id") or f.get("fight_id"),
                "encounter_id": f.get("encounterID") or f.get("encounter_id"),
                "keystone_level": f.get("keystoneLevel") or f.get("keystone_level"),
                "keystone_affixes": [
                    a.get("id") if isinstance(a, dict) else a
                    for a in (f.get("keystoneAffixes") or [])
                ],
                "keystone_time_ms": f.get("keystoneTime") or f.get("keystone_time_ms"),
                "kill": str(f.get("kill", "")),
                "start_time_ms": f.get("startTime") or f.get("start_time_ms"),
                "end_time_ms": f.get("endTime") or f.get("end_time_ms"),
            })

        rows.append({
            "source": "warcraftlogs",
            "report_code": r.get("code", ""),
            "report_title": r.get("title", ""),
            "zone_id": r.get("zoneID"),
            "start_time": r.get("startTime", 0),
            "end_time": r.get("endTime", 0),
            "owner_name": r.get("owner", {}).get("name", "") if isinstance(r.get("owner"), dict) else "",
            "guild_name": r.get("guild", {}).get("name", "") if isinstance(r.get("guild"), dict) else "",
            "visibility": r.get("visibility", ""),
            "fights": fights_struct,
            "ingested_at": datetime.now(timezone.utc),
        })

    df = spark.createDataFrame(rows, schema=bronze_wcl_reports_schema)
    path = f"s3a://{settings.MINIO_BUCKET}/bronze/warcraftlogs/reports"
    row_count = df.count()
    logger.info("Writing %d WCL reports to %s", row_count, path)
    df.write.mode("append").parquet(path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest WCL events for matched runs from Silver match manifest"
    )
    parser.add_argument(
        "--season",
        default=settings.SEASON,
        help=f"Season identifier (default: {settings.SEASON})",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of NEW reports to process (after skipping checkpointed ones)",
    )
    parser.add_argument(
        "--skip-damage",
        action="store_true",
        help="Skip DamageTaken table fetch (saves WCL points)",
    )
    parser.add_argument(
        "--skip-healing",
        action="store_true",
        help="Skip Healing table fetch (saves WCL points)",
    )
    parser.add_argument(
        "--skip-interrupts",
        action="store_true",
        help="Skip Interrupt events fetch (saves WCL points)",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        default=True,
        help="Resume from checkpoints — skip already-processed reports (default)",
    )
    parser.add_argument(
        "--no-resume",
        action="store_false",
        dest="resume",
        help="Start fresh — ignore existing checkpoints",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    logger.info("Starting WCL events ingestion for season=%s", args.season)

    # Check WCL credentials
    if not settings.WCL_CLIENT_ID or not settings.WCL_CLIENT_SECRET:
        logger.error(
            "WCL_CLIENT_ID and WCL_CLIENT_SECRET must be set in .env\n"
            "Get credentials at: https://www.warcraftlogs.com/profile\n"
            "Then add them to your .env file:\n"
            "  WCL_CLIENT_ID=your_client_id\n"
            "  WCL_CLIENT_SECRET=your_client_secret"
        )
        sys.exit(1)

    spark = get_spark_session("ingest_warcraftlogs")

    try:
        # Load match manifest
        matches = load_match_manifest(spark, args.season)

        if not matches:
            logger.error("No match manifest found. Run match_reports.py first.")
            sys.exit(1)

        # ── Resume: load checkpoints and skip already-processed reports ──────────
        checkpoint_data: dict[str, dict] = {}  # report_code -> checkpoint row dict
        if args.resume:
            path = f"s3a://{settings.MINIO_BUCKET}/bronze/warcraftlogs/checkpoints"
            try:
                cp_df = spark.read.parquet(path)
                for row in cp_df.collect():
                    r = row.asDict()
                    code = r["report_code"]
                    # Keep the LATEST checkpoint per report_code (most recent processed_at)
                    if code not in checkpoint_data or r.get("processed_at", 0) > checkpoint_data[code].get("processed_at", 0):
                        checkpoint_data[code] = r
                logger.info("Loaded %d existing checkpoints", len(checkpoint_data))
            except Exception:
                logger.info("No existing checkpoints found — starting fresh")

        # Group matches by report_code for efficient API usage
        from collections import defaultdict
        by_report: dict[str, list[dict]] = defaultdict(list)
        for m in matches:
            by_report[m["wcl_report_code"]].append(m)

        # ── Filter out already-processed reports ────────────────────────────────
        remaining_reports: dict[str, list[dict]] = {}
        for report_code, report_matches in by_report.items():
            cp = checkpoint_data.get(report_code)
            if cp is not None:
                # A report is complete when all non-skipped event types are done
                needed_damage = not args.skip_damage
                needed_healing = not args.skip_healing
                needed_interrupts = not args.skip_interrupts

                damage_ok = (not needed_damage) or cp.get("damage_taken_done", False)
                healing_ok = (not needed_healing) or cp.get("healing_done", False)
                interrupts_ok = (not needed_interrupts) or cp.get("interrupts_done", False)

                if damage_ok and healing_ok and interrupts_ok:
                    logger.info("Skipping %s — already checkpointed", report_code)
                    continue

            remaining_reports[report_code] = report_matches

        logger.info(
            "Reports: %d total, %d already checkpointed, %d remaining",
            len(by_report), len(by_report) - len(remaining_reports), len(remaining_reports),
        )

        # ── Apply --limit to NEW (remaining) reports ───────────────────────────
        if args.limit:
            # --limit N means process up to N NEW reports after skipping
            remaining_items = list(remaining_reports.items())
            remaining_reports = dict(remaining_items[:args.limit])
            logger.info("Limited to %d new reports", args.limit)

        if not remaining_reports:
            logger.info("No new reports to process. Exiting.")
            sys.exit(0)

        # Create WCL client
        wcl_client = WarcraftLogsClient()

        total_reports = len(remaining_reports)
        for i, (report_code, report_matches) in enumerate(remaining_reports.items(), 1):
            fight_ids = [m["wcl_fight_id"] for m in report_matches]
            logger.info(
                "Processing report %d/%d: %s (%d fights)",
                i, total_reports, report_code, len(fight_ids),
            )

            # Track per-report event counts
            report_event_count = 0

            # Fetch masterData for actor_id → player_name resolution
            actor_map: dict[int, str] = {}
            try:
                master_data = wcl_client.get_master_data(report_code)
                for actor in master_data.get("actors", []):
                    if actor.get("type") == "Player":
                        aid = actor.get("id")
                        aname = actor.get("name", "")
                        if aid and aname:
                            actor_map[aid] = aname
            except (WCLRateLimitError, WCLAuthError, requests.RequestException) as e:
                logger.warning("Failed to fetch masterData for %s (%s): %s", report_code, type(e).__name__, e)

            # Track which event types were done for this report
            damage_done = False
            healing_done = False
            interrupts_done = False

            # Fetch DamageTaken events
            if not args.skip_damage:
                damage_events = ingest_damage_taken(wcl_client, report_code, fight_ids)
                write_bronze_events(spark, damage_events, "damage_taken")
                report_event_count += len(damage_events)
                damage_done = True
                logger.info("  DamageTaken: %d events", len(damage_events))

            # Fetch Healing events (+ resolve target_name via masterData)
            if not args.skip_healing:
                healing_events = ingest_healing(wcl_client, report_code, fight_ids, actor_map)
                write_bronze_events(spark, healing_events, "healing")
                report_event_count += len(healing_events)
                healing_done = True
                logger.info("  Healing: %d events", len(healing_events))

            # Fetch Interrupt events + resolve player_name via masterData
            if not args.skip_interrupts:
                interrupt_events = ingest_interrupts(wcl_client, report_code, fight_ids, actor_map)
                write_bronze_events(spark, interrupt_events, "interrupts")
                report_event_count += len(interrupt_events)
                interrupts_done = True
                logger.info("  Interrupts: %d events", len(interrupt_events))

            # Log point usage
            logger.info(
                "  WCL points: remaining=%d, spent=%d",
                wcl_client.rate_limiter.points_remaining,
                wcl_client.rate_limiter.total_spent,
            )

            # ── Write checkpoint per report ─────────────────────────────────────
            write_checkpoint(
                spark,
                report_code=report_code,
                damage_taken_done=damage_done,
                healing_done=healing_done,
                interrupts_done=interrupts_done,
                fight_count=len(fight_ids),
                events_count=report_event_count,
            )

        # Summary
        logger.info("=" * 60)
        logger.info("WCL events ingestion complete. Summary:")
        logger.info("  Reports processed this run: %d", total_reports)
        logger.info("  Total WCL points spent: %d", wcl_client.rate_limiter.total_spent)
        logger.info("=" * 60)

    except Exception as e:
        logger.exception("WCL events ingestion failed [%s]", type(e).__name__)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    main()