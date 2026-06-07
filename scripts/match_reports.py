#!/usr/bin/env python3
"""Fuzzy join engine — match Raider.IO runs to WarcraftLogs reports.

This script implements the 3-layer fuzzy join algorithm:
  1. Layer 1: challenge_mode_id == encounterID + key_level + affixes
  2. Layer 2: |completed_at - (report.startTime + fight.endTime)| <= 30s
  3. Layer 3: roster overlap >= 3/5 players

With stratified tank sampling to prevent ML bias toward overrepresented classes.

Usage:
    uv run python scripts/match_reports.py --season season-tww-3
    uv run python scripts/match_reports.py --season season-tww-3 --limit-tanks 10
    uv run python scripts/match_reports.py --season season-tww-3 --min-confidence 0.5

Output:
    Writes match manifest Parquet to silver/matches/ on MinIO.
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import sys
import uuid
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from orakel.clients.warcraftlogs import WCLAuthError, WarcraftLogsClient
from orakel.config import settings
from orakel.utils.minio import get_spark_session

logger = logging.getLogger(__name__)

# Match manifest schema
MATCH_MANIFEST_SCHEMA = StructType(
    [
        StructField("rio_run_id", LongType(), nullable=False),
        StructField("wcl_report_code", StringType(), nullable=False),
        StructField("wcl_fight_id", IntegerType(), nullable=False),
        StructField("confidence", DoubleType(), nullable=False),
        StructField("match_method", StringType(), nullable=False),
        StructField("matched_at", TimestampType(), nullable=False),
    ]
)

# Tank class names for stratified sampling
TANK_SPECS = {
    "Warrior": "Protection",
    "Paladin": "Protection",
    "Death Knight": "Blood",
    "Monk": "Brewmaster",
    "Druid": "Guardian",
    "Demon Hunter": "Vengeance",
}

# encounterID mapping for TWW Season 3 dungeons
# challenge_mode_id (Raider.IO) == encounterID (WCL)
CHALLENGE_MODE_TO_ENCOUNTER = {
    503: "Ara-Kara, City of Echoes",
    542: "Eco-Dome Al'dani",
    378: "Halls of Atonement",
    525: "Operation: Floodgate",
    499: "Priory of the Sacred Flame",
    392: "Tazavesh: So'leah's Gambit",
    391: "Tazavesh: Streets of Wonder",
    505: "The Dawnbreaker",
}


def load_raiderio_runs(spark: SparkSession, season: str) -> Any:
    """Load Silver Raider.IO runs from MinIO.

    Args:
        spark: Active SparkSession.
        season: Season filter.

    Returns:
        DataFrame with Raider.IO run data.
    """
    path = f"s3a://{settings.MINIO_BUCKET}/silver/raiderio_runs"
    logger.info("Loading Silver Raider.IO runs from %s", path)
    df = spark.read.parquet(path).filter(F.col("season") == season)
    count = df.count()
    logger.info("Loaded %d Raider.IO runs for season %s", count, season)
    return df


def extract_tanks(runs_df: Any) -> list[dict[str, Any]]:
    """Extract unique tanks from Raider.IO runs, grouped by class.

    A tank is a roster player with role='tank'.

    Args:
        runs_df: Silver Raider.IO DataFrame.

    Returns:
        List of dicts: {name, class, spec, realm, region, run_ids}
    """
    from pyspark.sql import functions as F2

    # Explode roster to get individual players
    players = runs_df.select(
        F2.col("keystone_run_id"),
        F2.col("challenge_mode_id"),
        F2.col("mythic_level"),
        F2.explode(F2.col("roster")).alias("player"),
        F2.col("completed_at"),
    ).filter(
        F2.col("player.role") == "tank"
    )

    # Group by tank identity
    tank_rows = players.select(
        F2.col("player.name").alias("tank_name"),
        F2.col("player.class").alias("tank_class"),
        F2.col("player.spec").alias("tank_spec"),
        F2.col("player.realm").alias("tank_realm"),
        F2.col("player.region").alias("tank_region"),
        F2.col("keystone_run_id"),
    ).collect()

    # Group tanks by (name, realm, region) to get unique tanks
    tanks: dict[tuple, dict] = {}
    for row in tank_rows:
        key = (row["tank_name"], row["tank_realm"], row["tank_region"])
        if key not in tanks:
            tanks[key] = {
                "name": row["tank_name"],
                "class": row["tank_class"],
                "spec": row["tank_spec"],
                "realm": row["tank_realm"],
                "region": row["tank_region"],
                "run_ids": set(),
            }
        tanks[key]["run_ids"].add(row["keystone_run_id"])

    return list(tanks.values())


def stratified_sample_tanks(
    tanks: list[dict],
    limit: int | None = None,
) -> list[dict]:
    """Stratified sampling of tanks by class.

    Groups tanks by class, then samples evenly across all 6 tank-capable
    classes. Rare classes (DH, Druid, DK) get all their tanks sampled;
    common classes (Warrior, Paladin) get proportionally fewer.

    Args:
        tanks: List of tank dicts with 'class' field.
        limit: Maximum total tanks to sample (None = no limit).

    Returns:
        Stratified sample of tank dicts.
    """
    # Group by class
    by_class: dict[str, list[dict]] = {}
    for tank in tanks:
        cls = tank.get("class", "Unknown")
        if cls not in by_class:
            by_class[cls] = []
        by_class[cls].append(tank)

    total_tanks = len(tanks)
    num_classes = len(by_class)

    if num_classes == 0:
        return []

    # Target: at least 50 per class, distributed evenly
    target_per_class = max(50, total_tanks // num_classes)
    if limit is not None:
        # Scale down proportionally to limit
        target_per_class = max(1, limit // num_classes)

    sampled: list[dict] = []
    for cls, class_tanks in sorted(by_class.items()):
        # Take all for rare classes, sample proportionally for common ones
        n = min(len(class_tanks), target_per_class)
        sampled.extend(class_tanks[:n])

    logger.info(
        "Stratified sampling: %d unique tanks across %d classes, "
        "selected %d tanks (limit=%s)",
        total_tanks,
        num_classes,
        len(sampled),
        str(limit),
    )
    return sampled


def match_layer1(
    rio_run: dict,
    wcl_fights: list[dict],
) -> list[dict]:
    """Layer 1: exact match on challenge_mode_id, keystone_level, and affixes.

    Args:
        rio_run: Raider.IO run row dict.
        wcl_fights: List of WCL fight dicts from a report.

    Returns:
        List of candidate fights matching Layer 1.
    """
    rio_encounter_id = rio_run.get("challenge_mode_id")
    rio_level = rio_run.get("mythic_level")
    rio_affixes = set(rio_run.get("weekly_modifiers", []) or [])

    if rio_encounter_id is None:
        return []

    candidates = []
    for fight in wcl_fights:
        wcl_encounter = fight.get("encounterID") or fight.get("encounter_id")
        wcl_level = fight.get("keystoneLevel") or fight.get("keystone_level")
        wcl_affixes = set(
            (a.get("id") if isinstance(a, dict) else a)
            for a in (fight.get("keystoneAffixes") or [])
        )

        if wcl_encounter is None or wcl_level is None:
            continue

        if wcl_encounter == rio_encounter_id and wcl_level == rio_level:
            # Affixes must match (sorted comparison)
            if not rio_affixes or rio_affixes == wcl_affixes or rio_affixes.issubset(wcl_affixes):
                candidates.append(fight)

    return candidates


def match_layer2(
    rio_run: dict,
    wcl_fight: dict,
    report_start_time_ms: int,
    tolerance_seconds: int = 30,
) -> tuple[bool, float]:
    """Layer 2: temporal window match.

    Checks if the WCL fight timestamp is within `tolerance_seconds` of
    the Raider.IO run completion time.

    Args:
        rio_run: Raider.IO run row dict.
        wcl_fight: WCL fight dict.
        report_start_time_ms: Report start time in epoch milliseconds.
        tolerance_seconds: Max allowed difference in seconds.

    Returns:
        Tuple of (is_match, time_diff_seconds).
    """
    completed_at = rio_run.get("completed_at")
    fight_end_ms = wcl_fight.get("end_time_ms") or wcl_fight.get("endTime")
    fight_start_ms = wcl_fight.get("start_time_ms") or wcl_fight.get("startTime")

    if completed_at is None or fight_end_ms is None or report_start_time_ms is None:
        return False, float("inf")

    # Convert WCL fight time to epoch ms
    # WCL times are relative to report start
    wcl_absolute_time_ms = report_start_time_ms + (fight_end_ms or 0)

    # Convert rio completed_at to epoch ms
    if isinstance(completed_at, str):
        rio_epoch_ms = datetime.fromisoformat(completed_at.replace("Z", "+00:00")).timestamp() * 1000
    else:
        # Already a datetime or timestamp
        if hasattr(completed_at, "timestamp"):
            rio_epoch_ms = completed_at.timestamp() * 1000
        else:
            rio_epoch_ms = float(completed_at)

    time_diff_ms = abs(rio_epoch_ms - wcl_absolute_time_ms)
    time_diff_s = time_diff_ms / 1000.0

    return time_diff_s <= tolerance_seconds, time_diff_s


def match_layer3(
    rio_roster: list[dict],
    wcl_actors: list[dict],
    min_overlap: int = 3,
) -> tuple[bool, int]:
    """Layer 3: roster overlap verification.

    Checks if at least `min_overlap` players from the Raider.IO roster
    appear in the WCL master data (by character name + realm).

    Args:
        rio_roster: List of Raider.IO roster dicts with 'name' and 'realm'.
        wcl_actors: List of WCL actor dicts with 'name' and 'server'.
        min_overlap: Minimum overlap count for a match.

    Returns:
        Tuple of (is_match, overlap_count).
    """
    # Build set of normalized (name_lower, realm_lower) from WCL
    wcl_names = set()
    for actor in wcl_actors:
        name = (actor.get("name") or "").lower()
        server = (actor.get("server") or "").lower().replace(" ", "-")
        if name:
            wcl_names.add((name, server))

    # Check overlap with Raider.IO roster
    overlap = 0
    for player in rio_roster:
        name = (player.get("name") or "").lower()
        realm = player.get("realm", "")
        if isinstance(realm, dict):
            realm = realm.get("slug", "")
        realm = (realm or "").lower().replace(" ", "-")
        if name and (name, realm) in wcl_names:
            overlap += 1

    return overlap >= min_overlap, overlap


def compute_confidence(
    layer1_match: bool,
    layer2_match: bool,
    layer2_time_diff: float,
    layer3_overlap: int,
    total_players: int = 5,
    time_tolerance: int = 30,
) -> float:
    """Compute confidence score for a fuzzy join match.

    Formula:
        Base: 0.2 if Layer 1 matches, 0 otherwise
        Layer 2: (1.0 - |time_diff| / tolerance) * 0.3 if Layer 2 matches
        Layer 3: (overlap / total_players) * 0.5 if Layer 3 matches

    Args:
        layer1_match: Whether Layer 1 (exact ID match) succeeded.
        layer2_match: Whether Layer 2 (temporal window) succeeded.
        layer2_time_diff: Time difference in seconds (for scoring).
        layer3_overlap: Number of overlapping roster players.
        total_players: Size of roster (default 5 for M+).
        time_tolerance: Layer 2 tolerance in seconds.

    Returns:
        Confidence score between 0.0 and 1.0.
    """
    score = 0.0

    # Layer 1 base score
    if layer1_match:
        score += 0.2

    # Layer 2 temporal score
    if layer2_match:
        window_fraction = max(0.0, 1.0 - (layer2_time_diff / time_tolerance))
        score += window_fraction * 0.3

    # Layer 3 roster overlap score
    if layer3_overlap > 0:
        overlap_fraction = layer3_overlap / total_players
        score += overlap_fraction * 0.5

    return round(min(score, 1.0), 4)


def run_fuzzy_join(
    spark: SparkSession,
    season: str,
    limit_tanks: int | None = None,
    min_confidence: float = 0.5,
) -> list[dict]:
    """Main fuzzy join algorithm: match Raider.IO runs to WCL fights.

    1. Load Raider.IO runs from Silver.
    2. Extract unique tanks and stratified-sample by class.
    3. For each sampled tank, query WCL for recent reports.
    4. For each report, check fights for M+ matches using 3-layer algorithm.
    5. Write match manifest to MinIO.

    Args:
        spark: Active SparkSession.
        season: Season filter.
        limit_tanks: Maximum tanks to query (None = no limit).
        min_confidence: Minimum confidence score to include in manifest.

    Returns:
        List of match dicts suitable for Parquet write.
    """
    # 1. Load Silver Raider.IO data
    runs_df = load_raiderio_runs(spark, season)

    # 2. Extract and stratified-sample tanks
    tanks = extract_tanks(runs_df)
    if not tanks:
        logger.warning("No tanks found in Silver data. Check roster data.")
        return []

    sampled_tanks = stratified_sample_tanks(tanks, limit=limit_tanks)
    logger.info("Sampled %d tanks for WCL queries", len(sampled_tanks))

    # Collect run data for matching
    runs_data = runs_df.collect()
    runs_by_id = {row["keystone_run_id"]: row.asDict() for row in runs_data}

    # 3. Create WCL client
    try:
        wcl_client = WarcraftLogsClient()
    except WCLAuthError as e:
        logger.error("WCL authentication failed: %s", e)
        logger.error(
            "Set WCL_CLIENT_ID and WCL_CLIENT_SECRET in .env. "
            "Get credentials at https://www.warcraftlogs.com/profile"
        )
        return []

    matches: list[dict] = []
    tanks_processed = 0

    for tank in sampled_tanks:
        tanks_processed += 1
        tank_name = tank["name"]
        tank_realm = tank["realm"]
        tank_region = tank["region"]
        logger.info(
            "Processing tank %d/%d: %s on %s-%s",
            tanks_processed,
            len(sampled_tanks),
            tank_name,
            tank_realm,
            tank_region,
        )

        if isinstance(tank_realm, dict):
            tank_realm = tank_realm.get("slug", str(tank_realm))

        try:
            reports = wcl_client.get_recent_reports(
                character_name=tank_name,
                server_slug=tank_realm,
                region=tank_region,
                limit=100,
            )
        except Exception as e:
            logger.warning("Failed to fetch reports for tank %s: %s", tank_name, e)
            continue

        for report in reports:
            report_code = report.get("code", "")
            report_start_time = report.get("startTime", 0)
            fights = report.get("fights", [])

            if not fights:
                # Fetch fights separately if not included
                try:
                    fights = wcl_client.get_fights(report_code)
                except Exception as e:
                    logger.warning("Failed to fetch fights for report %s: %s", report_code, e)
                    continue

            # Find M+ fights (have keystoneLevel)
            mp_fights = [
                f for f in fights
                if f.get("keystoneLevel") is not None or f.get("keystone_level") is not None
            ]

            if not mp_fights:
                continue

            # Try to match each run that this tank participated in
            for run_id in tank.get("run_ids", set()):
                rio_run = runs_by_id.get(run_id)
                if rio_run is None:
                    continue

                # Layer 1: exact ID + level + affix match
                candidates = match_layer1(rio_run, mp_fights)
                if not candidates:
                    continue

                # Get master data for roster verification
                wcl_actors = []
                try:
                    master_data = wcl_client.get_master_data(report_code)
                    wcl_actors = master_data.get("actors", [])
                except Exception as e:
                    logger.warning(
                        "Failed to fetch master data for %s: %s", report_code, e
                    )

                for fight in candidates:
                    fight_id = fight.get("id") or fight.get("fight_id")

                    # Layer 2: temporal window
                    l2_match, time_diff = match_layer2(
                        rio_run, fight, report_start_time
                    )

                    # Layer 3: roster overlap
                    rio_roster = rio_run.get("roster", []) or []
                    l3_match, overlap_count = match_layer3(rio_roster, wcl_actors)

                    # Compute confidence
                    confidence = compute_confidence(
                        layer1_match=True,
                        layer2_match=l2_match,
                        layer2_time_diff=time_diff,
                        layer3_overlap=overlap_count,
                    )

                    if confidence < min_confidence:
                        continue

                    # Determine match method
                    if l2_match and l3_match:
                        method = "full_3_layer"
                    elif l2_match:
                        method = "layer1_layer2"
                    else:
                        method = "layer1_only"

                    matches.append({
                        "rio_run_id": int(run_id),
                        "wcl_report_code": report_code,
                        "wcl_fight_id": int(fight_id) if fight_id else 0,
                        "confidence": float(confidence),
                        "match_method": method,
                        "matched_at": datetime.now(timezone.utc),
                    })

        # Rate limit awareness between tanks
        logger.info(
            "Tank %s done. WCL points remaining: %d, total spent: %d",
            tank_name,
            wcl_client.rate_limiter.points_remaining,
            wcl_client.rate_limiter.total_spent,
        )

    logger.info("Total matches found: %d", len(matches))
    return matches


def write_match_manifest(spark: SparkSession, matches: list[dict], season: str) -> None:
    """Write match manifest to MinIO as Parquet.

    Args:
        spark: Active SparkSession.
        matches: List of match dicts.
        season: Season string for partitioning.
    """
    if not matches:
        logger.warning("No matches to write.")
        return

    df = spark.createDataFrame(matches, schema=MATCH_MANIFEST_SCHEMA)

    # Add season column for partitioning
    df = df.withColumn("season", F.lit(season))

    path = f"s3a://{settings.MINIO_BUCKET}/silver/matches"
    row_count = df.count()
    logger.info("Writing %d match records to %s", row_count, path)

    df.write.mode("overwrite").partitionBy("season").parquet(path)
    logger.info("Match manifest written: %d rows to %s", row_count, path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fuzzy join Raider.IO runs to WCL reports with stratified tank sampling"
    )
    parser.add_argument(
        "--season",
        default=settings.SEASON,
        help=f"Season identifier (default: {settings.SEASON})",
    )
    parser.add_argument(
        "--limit-tanks",
        type=int,
        default=None,
        help="Maximum number of tanks to query WCL for (default: all)",
    )
    parser.add_argument(
        "--min-confidence",
        type=float,
        default=0.3,
        help="Minimum confidence score for match inclusion (default: 0.3)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run matching without writing to MinIO",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    logger.info(
        "Starting fuzzy join: season=%s, limit_tanks=%s, min_confidence=%.2f",
        args.season,
        args.limit_tanks,
        args.min_confidence,
    )

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

    spark = get_spark_session("match_reports")

    try:
        matches = run_fuzzy_join(
            spark,
            season=args.season,
            limit_tanks=args.limit_tanks,
            min_confidence=args.min_confidence,
        )

        if not matches:
            logger.warning("No matches found. Check WCL credentials and Silver data.")
            sys.exit(1)

        if not args.dry_run:
            write_match_manifest(spark, matches, args.season)

        # Summary
        logger.info("=" * 60)
        logger.info("Fuzzy join complete. Summary:")
        methods = {}
        for m in matches:
            method = m["match_method"]
            methods[method] = methods.get(method, 0) + 1
        for method, count in methods.items():
            logger.info("  %s: %d matches", method, count)
        avg_confidence = sum(m["confidence"] for m in matches) / len(matches)
        logger.info("  Average confidence: %.4f", avg_confidence)
        logger.info("  Total matches: %d", len(matches))
        logger.info("=" * 60)

    except Exception:
        logger.exception("Fuzzy join failed")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    main()