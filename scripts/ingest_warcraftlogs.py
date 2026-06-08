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

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from orakel.clients.warcraftlogs import WCLAuthError, WCLRateLimitError, WarcraftLogsClient
from orakel.config import settings
from orakel.models.schemas import bronze_wcl_events_schema, bronze_wcl_reports_schema
from orakel.utils.minio import get_spark_session

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
        help="Maximum number of matches to process",
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

        if args.limit:
            matches = matches[:args.limit]
            logger.info("Limited to %d matches", args.limit)

        # Create WCL client
        wcl_client = WarcraftLogsClient()

        # Group matches by report_code for efficient API usage
        from collections import defaultdict
        by_report: dict[str, list[dict]] = defaultdict(list)
        for m in matches:
            by_report[m["wcl_report_code"]].append(m)

        all_damage_events: list[dict] = []
        all_healing_events: list[dict] = []
        all_interrupt_events: list[dict] = []
        all_reports: list[dict] = []

        total_reports = len(by_report)
        for i, (report_code, report_matches) in enumerate(by_report.items(), 1):
            fight_ids = [m["wcl_fight_id"] for m in report_matches]
            logger.info(
                "Processing report %d/%d: %s (%d fights)",
                i, total_reports, report_code, len(fight_ids),
            )

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
            except Exception as e:
                logger.warning("Failed to fetch masterData for %s: %s", report_code, e)

            # Fetch DamageTaken events
            if not args.skip_damage:
                damage_events = ingest_damage_taken(wcl_client, report_code, fight_ids)
                all_damage_events.extend(damage_events)
                logger.info("  DamageTaken: %d events", len(damage_events))

            # Fetch Healing events (+ resolve target_name via masterData)
            if not args.skip_healing:
                healing_events = ingest_healing(wcl_client, report_code, fight_ids, actor_map)
                all_healing_events.extend(healing_events)
                logger.info("  Healing: %d events", len(healing_events))

            # Fetch Interrupt events + resolve player_name via masterData
            if not args.skip_interrupts:
                interrupt_events = ingest_interrupts(wcl_client, report_code, fight_ids, actor_map)
                all_interrupt_events.extend(interrupt_events)
                logger.info("  Interrupts: %d events", len(interrupt_events))

            # Log point usage
            logger.info(
                "  WCL points: remaining=%d, spent=%d",
                wcl_client.rate_limiter.points_remaining,
                wcl_client.rate_limiter.total_spent,
            )

        # Write all Bronze events
        if not args.skip_damage:
            write_bronze_events(spark, all_damage_events, "damage_taken")

        if not args.skip_healing:
            write_bronze_events(spark, all_healing_events, "healing")

        if not args.skip_interrupts:
            write_bronze_events(spark, all_interrupt_events, "interrupts")

        # Summary
        logger.info("=" * 60)
        logger.info("WCL events ingestion complete. Summary:")
        if not args.skip_damage:
            logger.info("  DamageTaken: %d events", len(all_damage_events))
        if not args.skip_healing:
            logger.info("  Healing: %d events", len(all_healing_events))
        if not args.skip_interrupts:
            logger.info("  Interrupts: %d events", len(all_interrupt_events))
        logger.info("  Total WCL points spent: %d", wcl_client.rate_limiter.total_spent)
        logger.info("=" * 60)

    except Exception:
        logger.exception("WCL events ingestion failed")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    main()