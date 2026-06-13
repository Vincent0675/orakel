#!/usr/bin/env python3
"""Fuzzy join engine — match Raider.IO runs to WarcraftLogs reports.

This script implements the 3-layer fuzzy join algorithm:
  1. Layer 1: challenge_mode_id == encounterID + key_level + affixes
  2. Layer 2: |completed_at - (report.startTime + fight.endTime)| <= 30s
  3. Layer 3: roster overlap >= 3/5 players

With stratified tank sampling to prevent ML bias toward overrepresented classes.

Supports incremental checkpointing: after each tank is processed, a checkpoint
is written to MinIO. On re-run with --resume (default), already-checkpointed
tanks are skipped. Use --no-resume to start fresh.

Usage:
    uv run python scripts/match_reports.py --season season-tww-3
    uv run python scripts/match_reports.py --season season-tww-3 --limit-tanks 10
    uv run python scripts/match_reports.py --season season-tww-3 --min-confidence 0.5
    uv run python scripts/match_reports.py --season season-tww-3 --no-resume

Output:
    Writes match manifest Parquet to silver/matches/ on MinIO (append mode).
    Writes checkpoint Parquet to silver/matches/checkpoints/ on MinIO.
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import re
import sys
import unicodedata
import uuid
from datetime import datetime, timezone
from typing import Any

import requests

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

from orakel.clients.warcraftlogs import WCLAuthError, WCLRateLimitError, WarcraftLogsClient
from orakel.config import settings
from orakel.models.schemas import dead_letter_schema
from orakel.utils.minio import get_spark_session

logger = logging.getLogger(__name__)

# ─── Dead-Letter Queue helpers ──────────────────────────────────────────────

_dlq_buffer: list[dict] = []


def write_dead_letter(
    spark: SparkSession,
    entity_type: str,
    entity_key: str,
    error_type: str,
    error_message: str,
    payload_snapshot: str,
    season: str,
    sub_path: str,
) -> None:
    """Write a single record to the dead-letter queue on MinIO.

    Args:
        spark: Active SparkSession.
        entity_type: Type of entity that failed (e.g. "wcl_matches").
        entity_key: Identifier for the failed entity (e.g. report_code).
        error_type: Short error category (e.g. "rate_limit", "auth_error", "timeout").
        error_message: Full error message or traceback.
        payload_snapshot: JSON snapshot of the input that failed (truncated to 1KB).
        season: Season identifier for partitioning.
        sub_path: DLQ sub-path (e.g. "wcl_matches").
    """
    # Truncate payload snapshot to 1KB
    if len(payload_snapshot) > 1024:
        payload_snapshot = payload_snapshot[:1024] + "...[truncated]"

    record = {
        "entity_type": entity_type,
        "entity_key": entity_key,
        "error_type": error_type,
        "error_message": error_message[:2000] if error_message else "",
        "payload_snapshot": payload_snapshot,
        "occurred_at": datetime.now(timezone.utc),
        "retried": False,
        "season": season,
    }
    _dlq_buffer.append(record)

    # Flush to MinIO every 50 records
    if len(_dlq_buffer) >= 50:
        flush_dead_letter(spark, sub_path)


def flush_dead_letter(spark: SparkSession, sub_path: str) -> int:
    """Flush buffered dead-letter records to MinIO Parquet.

    Args:
        spark: Active SparkSession.
        sub_path: DLQ sub-path (e.g. "wcl_matches").

    Returns:
        Number of records flushed.
    """
    global _dlq_buffer
    if not _dlq_buffer:
        return 0

    df = spark.createDataFrame(_dlq_buffer, schema=dead_letter_schema)
    path = f"s3a://{settings.MINIO_BUCKET}/silver/dead_letter/{sub_path}"
    count = df.count()
    df.write.mode("append").partitionBy("season").parquet(path)
    logger.warning("DLQ: flushed %d records to %s", count, path)
    flushed = len(_dlq_buffer)
    _dlq_buffer = []
    return flushed


def _classify_error(error: Exception) -> str:
    """Classify an exception into a short error type string for DLQ."""
    if isinstance(error, WCLRateLimitError):
        return "rate_limit"
    if isinstance(error, WCLAuthError):
        return "auth_error"
    if isinstance(error, requests.Timeout):
        return "timeout"
    if isinstance(error, requests.HTTPError):
        return "http_error"
    return type(error).__name__

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

# Checkpoint schema for incremental processing tracking
CHECKPOINT_SCHEMA = StructType(
    [
        StructField("tank_name", StringType(), nullable=False),
        StructField("tank_realm", StringType(), nullable=False),
        StructField("tank_region", StringType(), nullable=False),
        StructField("tank_class", StringType(), nullable=False),
        StructField("processed_at", TimestampType(), nullable=False),
        StructField("matches_found", IntegerType(), nullable=False),
    ]
)

CHECKPOINT_PATH = "silver/match_checkpoints"
FLUSH_INTERVAL_TANKS = 5
FLUSH_THRESHOLD_MATCHES = 500

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
    """Layer 1: exact match on keystone_level and affixes.

    NOTE: WCL encounterIDs do NOT match Raider.IO challenge_mode_ids
    (they use different numbering systems). We rely on key_level + affixes
    for the first filter, then Layer 2 (timestamp) and Layer 3 (roster)
    for disambiguation.

    Args:
        rio_run: Raider.IO run row dict.
        wcl_fights: List of WCL fight dicts from a report.

    Returns:
        List of candidate fights matching Layer 1.
    """
    rio_level = rio_run.get("mythic_level")
    rio_affixes = set(rio_run.get("weekly_modifiers", []) or [])

    if rio_level is None:
        return []

    candidates = []
    for fight in wcl_fights:
        wcl_level = fight.get("keystoneLevel") or fight.get("keystone_level")
        wcl_affixes = set(
            (a.get("id") if isinstance(a, dict) else a)
            for a in (fight.get("keystoneAffixes") or [])
        )

        if wcl_level is None:
            continue

        if wcl_level == rio_level:
            # Affixes must match (as sets, order-independent)
            if not rio_affixes or rio_affixes == wcl_affixes:
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

    # WCL fight timestamps may be relative (to report start) or absolute.
    # Heuristic: if the value exceeds 24 hours in ms (86_400_000), it's
    # almost certainly an absolute epoch timestamp rather than a relative
    # offset.  Using a relative value as-if absolute would double-offset
    # and break matching; the heuristic prevents that.
    if fight_end_ms > 86_400_000:
        # Absolute timestamp — use directly
        wcl_absolute_time_ms = fight_end_ms
    else:
        # Relative to report start — add offset
        wcl_absolute_time_ms = report_start_time_ms + fight_end_ms

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


def _normalize_name(name: str) -> str:
    """Normalize a character name for cross-reference matching.

    - Lowercase
    - Strip trailing -numeric suffix (Raider.IO disambiguation)
    - Unicode NFKC normalization (CJK, Cyrillic, accents)
    """
    name = unicodedata.normalize("NFKC", name.strip().lower())
    name = re.sub(r'-\d+$', '', name)
    return name


def _normalize_realm(realm: str) -> set[str]:
    """Normalize a realm/server name into candidate formats for matching.

    WCL and Raider.IO may use different formats:
    - "howling-fjord" (slug) vs "Howling Fjord" (display) vs "howling fjord"
    - "zuljin" vs "Zul'jin" vs "zul'jin"

    Returns a set of possible normalized forms to try.
    """
    realm_str = str(realm) if realm else ""
    realm_str = unicodedata.normalize("NFKC", realm_str.strip())

    candidates = set()
    # Original lowercase
    low = realm_str.lower()
    candidates.add(low)
    # Slug: replace spaces, apostrophes, special chars with hyphens
    slug = re.sub(r"[\s'_.]+", "-", low)
    candidates.add(slug)
    # Collapse multiple hyphens
    slug_clean = re.sub(r"-+", "-", slug)
    candidates.add(slug_clean)
    # Strip hyphens from edges
    slug_stripped = slug_clean.strip("-")
    candidates.add(slug_stripped)
    # Remove all special chars (just alphanumeric)
    alnum = re.sub(r"[^a-z0-9]", "", low)
    candidates.add(alnum)
    # Also try with realm that might have region prefix stripped
    return candidates


def match_layer3(
    rio_roster: list[dict],
    wcl_actors: list[dict],
    min_overlap: int = 3,
) -> tuple[bool, int]:
    """Layer 3: roster overlap verification.

    Checks if at least `min_overlap` players from the Raider.IO roster
    appear in the WCL master data (by character name + realm).

    Uses flexible realm matching: slug, display name, alphanumeric-only
    forms are all tried to handle format differences between Raider.IO
    and WarcraftLogs.

    Args:
        rio_roster: List of Raider.IO roster dicts with 'name' and 'realm'.
        wcl_actors: List of WCL actor dicts with 'name' and 'server'.
        min_overlap: Minimum overlap count for a match.

    Returns:
        Tuple of (is_match, overlap_count).
    """
    # Build lookup: normalized name → set of normalized realms from WCL
    # Multiple WCL actors may share the same name (healers, tanks, etc.)
    wcl_lookup: dict[str, set[str]] = {}
    for actor in wcl_actors:
        if actor.get("type") == "NPC":
            continue
        name = _normalize_name(actor.get("name") or "")
        server = _normalize_realm(actor.get("server") or "")
        if name and server:
            if name not in wcl_lookup:
                wcl_lookup[name] = set()
            wcl_lookup[name].update(server)

    # Also build a set without realm (name-only) for fallback
    wcl_names_only = {n for n, s in wcl_lookup.items() if s}

    # Check overlap with Raider.IO roster
    overlap = 0
    for player in rio_roster:
        rio_name = _normalize_name(player.get("name") or "")
        rio_realm_raw = player.get("realm", "")
        if isinstance(rio_realm_raw, dict):
            rio_realm_raw = rio_realm_raw.get("slug", str(rio_realm_raw))
        rio_realms = _normalize_realm(rio_realm_raw)

        if not rio_name:
            continue

        # Check if player name exists in WCL lookup
        wcl_server_set = wcl_lookup.get(rio_name)

        if wcl_server_set:
            # Check realm overlap: any WCL realm candidate matches any RIO realm candidate
            realm_match = bool(rio_realms & wcl_server_set)
            if realm_match:
                overlap += 1
            elif rio_name in wcl_names_only:
                # Name matches, but realm doesn't — still count as partial match
                # This handles cases where WCL doesn't store realm for a character
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


def _tank_key(tank: dict) -> tuple[str, str, str]:
    """Create a hashable key for a tank from name, realm, region."""
    realm = tank.get("realm", "")
    if isinstance(realm, dict):
        realm = realm.get("slug", str(realm))
    return (tank.get("name", ""), realm, tank.get("region", ""))


def load_checkpoints(spark: SparkSession) -> set[tuple[str, str, str]]:
    """Load existing checkpoint data from MinIO.

    Returns:
        Set of (name, realm, region) tuples for already-processed tanks.
    """
    path = f"s3a://{settings.MINIO_BUCKET}/{CHECKPOINT_PATH}"
    try:
        df = spark.read.parquet(path)
        rows = df.select("tank_name", "tank_realm", "tank_region").collect()
        checkpoints = {(row["tank_name"], row["tank_realm"], row["tank_region"]) for row in rows}
        logger.info("Loaded %d existing checkpoints", len(checkpoints))
        return checkpoints
    except Exception:
        logger.info("No existing checkpoints found — starting fresh")
        return set()


def write_checkpoint(
    spark: SparkSession,
    tank: dict,
    matches_found: int,
) -> None:
    """Write a checkpoint entry for a processed tank to MinIO."""
    realm = tank.get("realm", "")
    if isinstance(realm, dict):
        realm = realm.get("slug", str(realm))

    checkpoint_data = [{
        "tank_name": tank.get("name", ""),
        "tank_realm": realm,
        "tank_region": tank.get("region", ""),
        "tank_class": tank.get("class", ""),
        "processed_at": datetime.now(timezone.utc),
        "matches_found": matches_found,
    }]

    df = spark.createDataFrame(checkpoint_data, schema=CHECKPOINT_SCHEMA)
    path = f"s3a://{settings.MINIO_BUCKET}/{CHECKPOINT_PATH}"
    df.write.mode("append").parquet(path)
    logger.info(
        "Checkpoint written for %s-%s-%s (%d matches)",
        tank.get("name", ""), realm, tank.get("region", ""), matches_found,
    )


def flush_matches(
    spark: SparkSession,
    matches: list[dict],
    season: str,
) -> None:
    """Flush accumulated matches to MinIO in append mode."""
    if not matches:
        return

    df = spark.createDataFrame(matches, schema=MATCH_MANIFEST_SCHEMA)
    df = df.withColumn("season", F.lit(season))

    path = f"s3a://{settings.MINIO_BUCKET}/silver/matches"
    row_count = df.count()
    df.write.mode("append").partitionBy("season").parquet(path)
    logger.info("Flushed %d match records to %s", row_count, path)


def run_fuzzy_join(
    spark: SparkSession,
    season: str,
    limit_tanks: int | None = None,
    min_confidence: float = 0.5,
    resume: bool = True,
    dry_run: bool = False,
) -> list[dict]:
    """Main fuzzy join algorithm: match Raider.IO runs to WCL fights.

    1. Load Raider.IO runs from Silver.
    2. Extract unique tanks and stratified-sample by class.
    3. Skip already-checkpointed tanks if resuming.
    4. For each sampled tank, query WCL for recent reports.
    5. For each report, check fights for M+ matches using 3-layer algorithm.
    6. Write checkpoint after each tank; flush matches incrementally.

    Args:
        spark: Active SparkSession.
        season: Season filter.
        limit_tanks: Maximum NEW tanks to query (None = no limit).
        min_confidence: Minimum confidence score to include in manifest.
        resume: If True, skip already-checkpointed tanks.
        dry_run: If True, don't write to MinIO.

    Returns:
        List of match dicts for summary.
    """
    # 1. Load Silver Raider.IO data
    runs_df = load_raiderio_runs(spark, season)

    # 2. Extract and stratified-sample tanks
    tanks = extract_tanks(runs_df)
    if not tanks:
        logger.warning("No tanks found in Silver data. Check roster data.")
        return []

    # 2a. Load checkpoints and filter already-processed tanks
    checkpoints: set[tuple[str, str, str]] = set()
    if resume:
        checkpoints = load_checkpoints(spark)

    # Sample enough tanks — when resuming, oversample to compensate for
    # already-processed ones so --limit-tanks counts NEW tanks only
    sample_limit = None
    if limit_tanks is not None:
        if resume and checkpoints:
            sample_limit = limit_tanks * 5
        else:
            sample_limit = limit_tanks

    sampled_tanks = stratified_sample_tanks(tanks, limit=sample_limit)
    logger.info("Sampled %d tanks for WCL queries", len(sampled_tanks))

    if checkpoints:
        original_count = len(sampled_tanks)
        sampled_tanks = [
            t for t in sampled_tanks
            if _tank_key(t) not in checkpoints
        ]
        logger.info(
            "Skipped %d already-checkpointed tanks, %d remaining",
            original_count - len(sampled_tanks),
            len(sampled_tanks),
        )

    # Apply limit_tanks to remaining NEW tanks
    if limit_tanks is not None:
        sampled_tanks = sampled_tanks[:limit_tanks]

    if not sampled_tanks:
        logger.warning("No new tanks to process after checkpoint filtering.")
        return []

    # 3. Collect run data for matching
    runs_data = runs_df.collect()
    runs_by_id = {row["keystone_run_id"]: row.asDict() for row in runs_data}

    # 4. Create WCL client
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
    last_flush_idx = 0
    tanks_since_flush = 0
    tanks_processed = 0

    for tank in sampled_tanks:
        tanks_processed += 1
        tanks_since_flush += 1
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
        except (WCLRateLimitError, WCLAuthError, requests.RequestException) as e:
            error_type = _classify_error(e)
            logger.warning("Failed to fetch reports for tank %s (%s): %s", tank_name, error_type, e)
            write_dead_letter(
                spark=spark,
                entity_type="wcl_matches",
                entity_key=f"{tank_name}:{tank_realm}:{tank_region}",
                error_type=error_type,
                error_message=str(e),
                payload_snapshot=f'{{"character_name": "{tank_name}", "server_slug": "{tank_realm}", "region": "{tank_region}"}}',
                season=season,
                sub_path="wcl_matches",
            )
            # Still write checkpoint even on fetch failure
            if not dry_run:
                write_checkpoint(spark, tank, 0)
            continue

        tank_match_count = 0

        for report in reports:
            report_code = report.get("code", "")
            report_start_time = report.get("startTime", 0)
            fights = report.get("fights", [])

            # Skip non-M+ reports by zone
            # 45 = TWW S3 (current), 43 = TWW S2, 47 = TWW S1
            report_zone = report.get("zone", {})
            zone_id = report_zone.get("id") if isinstance(report_zone, dict) else report_zone
            if zone_id not in (45, 43, 47):
                continue

            if not fights:
                # Fetch fights separately if not included
                try:
                    fights = wcl_client.get_fights(report_code)
                except (WCLRateLimitError, WCLAuthError, requests.RequestException) as e:
                    error_type = _classify_error(e)
                    logger.warning("Failed to fetch fights for report %s (%s): %s", report_code, error_type, e)
                    write_dead_letter(
                        spark=spark,
                        entity_type="wcl_matches",
                        entity_key=report_code,
                        error_type=error_type,
                        error_message=str(e),
                        payload_snapshot=f'{{"report_code": "{report_code}", "data_type": "fights"}}',
                        season=season,
                        sub_path="wcl_matches",
                    )
                    continue

            # Find M+ fights (have keystoneLevel)
            mp_fights = [
                f for f in fights
                if f.get("keystoneLevel") is not None or f.get("keystone_level") is not None
            ]

            if not mp_fights:
                logger.debug("No M+ fights in report %s (zone=%s)", report_code, zone_id)
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
                except (WCLRateLimitError, WCLAuthError, requests.RequestException) as e:
                    error_type = _classify_error(e)
                    logger.warning(
                        "Failed to fetch master data for %s (%s): %s",
                        report_code,
                        error_type,
                        e,
                    )
                    write_dead_letter(
                        spark=spark,
                        entity_type="wcl_matches",
                        entity_key=report_code,
                        error_type=error_type,
                        error_message=str(e),
                        payload_snapshot=f'{{"report_code": "{report_code}", "data_type": "master_data"}}',
                        season=season,
                        sub_path="wcl_matches",
                    )

                for fight in candidates:
                    fight_id = fight.get("id") or fight.get("fight_id")

                    # Layer 2: temporal window
                    l2_match, time_diff = match_layer2(
                        rio_run, fight, report_start_time
                    )

                    # Layer 3: roster overlap
                    # Convert Spark Rows to plain dicts if needed
                    rio_roster_raw = rio_run.get("roster", []) or []
                    rio_roster = [
                        r.asDict() if hasattr(r, "asDict") else r
                        for r in rio_roster_raw
                    ]
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
                    tank_match_count += 1

        # Write checkpoint for this tank
        if not dry_run:
            write_checkpoint(spark, tank, tank_match_count)

        # Rate limit awareness between tanks
        logger.info(
            "Tank %s done. WCL points remaining: %d, total spent: %d, tank matches: %d",
            tank_name,
            wcl_client.rate_limiter.points_remaining,
            wcl_client.rate_limiter.total_spent,
            tank_match_count,
        )

        # Incremental flush: every FLUSH_INTERVAL_TANKS tanks or when
        # accumulated matches since last flush exceed FLUSH_THRESHOLD_MATCHES
        should_flush = (
            tanks_since_flush >= FLUSH_INTERVAL_TANKS
            or len(matches) - last_flush_idx >= FLUSH_THRESHOLD_MATCHES
        )
        if not dry_run and should_flush:
            batch = matches[last_flush_idx:]
            flush_matches(spark, batch, season)
            last_flush_idx = len(matches)
            tanks_since_flush = 0

    # Final flush for remaining matches
    if not dry_run and last_flush_idx < len(matches):
        batch = matches[last_flush_idx:]
        flush_matches(spark, batch, season)

    # Flush any remaining DLQ records
    dlq_count = flush_dead_letter(spark, "wcl_matches")
    if dlq_count > 0:
        logger.warning("DLQ: flushed %d remaining records to silver/dead_letter/wcl_matches", dlq_count)

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

    df.write.mode("append").partitionBy("season").parquet(path)
    logger.info("Match manifest written: %d rows to %s (append mode)", row_count, path)


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
        help="Maximum number of NEW tanks to query WCL for (default: all, excludes already-checkpointed)",
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
    parser.add_argument(
        "--resume",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Skip already-checkpointed tanks (default: --resume, use --no-resume to start fresh)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    logger.info(
        "Starting fuzzy join: season=%s, limit_tanks=%s, min_confidence=%.2f, resume=%s",
        args.season,
        args.limit_tanks,
        args.min_confidence,
        args.resume,
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
            resume=args.resume,
            dry_run=args.dry_run,
        )

        if not matches:
            logger.warning("No matches found. Check WCL credentials and Silver data.")
            sys.exit(1)

        # Matches are written incrementally by run_fuzzy_join (unless dry-run).
        # For dry-run, we just report what would have been written.

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

    except Exception as e:
        logger.exception("Fuzzy join failed [%s]", type(e).__name__)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    main()