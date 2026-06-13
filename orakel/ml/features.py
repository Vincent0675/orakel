"""Feature engineering — build the ML feature view from Gold KPIs.

Reads KPI tables and silver_dungeon_runs, joins them on run_id,
computes derived features (log1p transforms, role counts, affix flags,
dungeon one-hot), and writes the feature view to ``gold/features/``.

Z-score normalization is intentionally NOT applied here — it happens
at training time to avoid data leakage from the test set.
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

from orakel.config import settings
from orakel.ml.schemas import (
    AFFIX_COLUMNS,
    AFFIX_IDS,
    DUNGEON_COLUMNS,
    DUNGEON_IDS,
    LOG1P_FEATURES,
    ROLE_COUNT_FEATURES,
    TARGET_COLUMN,
)
from orakel.pipeline.gold import GoldPipeline

logger = logging.getLogger(__name__)


def build_feature_view(spark: SparkSession, season: str) -> DataFrame:
    """Build the ML feature view by joining Gold KPIs and silver_dungeon_runs.

    Steps:
        1. Read silver_dungeon_runs for run-level base data (target, roster, affixes).
        2. Aggregate per-player KPIs (death_clock, healer_deficit, interrupt_rate)
           to run level via groupBy("run_id").avg().
        3. Read gold_kpi_synergy and join on (dungeon_id, key_level, affix_ids, comp_signature).
        4. Derive features: log1p on death_clock_seconds, role counts from roster,
           binary affix flags, dungeon one-hot.
        5. Filter out rows with NULL target.
        6. Write to gold/features/ partitioned by season and key_level.

    Args:
        spark: Active SparkSession.
        season: Season filter (e.g. ``"season-tww-3"``).

    Returns:
        Feature view DataFrame with all model input columns + metadata.
    """
    bucket = settings.MINIO_BUCKET

    # ── 1. Read silver_dungeon_runs (base frame) ──────────────────────────────
    dr = spark.read.parquet(f"s3a://{bucket}/silver/dungeon_runs").filter(
        F.col("season") == season
    )
    row_count = dr.count()
    logger.info("silver_dungeon_runs: %d rows for season %s", row_count, season)

    # Compute target: clear_time_seconds = clear_time_ms / 1000
    dr = dr.withColumn(TARGET_COLUMN, F.col("clear_time_ms") / 1000.0)

    # ── 2. Aggregate per-player KPIs to run level ─────────────────────────────

    # Death Clock: average per run (usually 1 tank, but handle > 1)
    try:
        death_clock = spark.read.parquet(f"s3a://{bucket}/gold/kpi_tank_death_clock")
        dc_agg = death_clock.groupBy("run_id").agg(
            F.avg("death_clock_seconds").alias("death_clock_seconds"),
        )
        dr = dr.join(dc_agg, on="run_id", how="left")
        logger.info("Joined death_clock KPI")
    except Exception as e:
        logger.warning("death_clock KPI not available (%s: %s), using NULLs.", type(e).__name__, e)
        dr = dr.withColumn("death_clock_seconds", F.lit(None).cast(DoubleType()))

    # Healer Deficit: average per run
    try:
        healer_deficit = spark.read.parquet(f"s3a://{bucket}/gold/kpi_healer_deficit")
        hd_agg = healer_deficit.groupBy("run_id").agg(
            F.avg("deficit_ratio").alias("deficit_ratio"),
        )
        dr = dr.join(hd_agg, on="run_id", how="left")
        logger.info("Joined healer_deficit KPI")
    except Exception as e:
        logger.warning("healer_deficit KPI not available (%s: %s), using NULLs.", type(e).__name__, e)
        dr = dr.withColumn("deficit_ratio", F.lit(None).cast(DoubleType()))

    # Interrupt Rate: average per run
    try:
        interrupt_rate = spark.read.parquet(f"s3a://{bucket}/gold/kpi_interrupt_rate")
        ir_agg = interrupt_rate.groupBy("run_id").agg(
            F.avg("interrupts_per_minute").alias("interrupts_per_minute"),
        )
        dr = dr.join(ir_agg, on="run_id", how="left")
        logger.info("Joined interrupt_rate KPI")
    except Exception as e:
        logger.warning("interrupt_rate KPI not available (%s: %s), using NULLs.", type(e).__name__, e)
        dr = dr.withColumn("interrupts_per_minute", F.lit(None).cast(DoubleType()))

    # ── 3. Join synergy KPI (grouped by dungeon+key+affix+comp) ────────────────
    # Build comp_signature from roster for this run, then join to synergy table.
    dr = dr.withColumn(
        "comp_signature",
        GoldPipeline._build_comp_signature(F.col("roster")),
    )

    # Both sides need a hashable representation of affix_ids for joining
    dr = dr.withColumn(
        "affix_ids_key",
        F.expr("concat_ws('|', transform(array_sort(affix_ids), x -> cast(x as string)))"),
    )

    try:
        synergy = spark.read.parquet(f"s3a://{bucket}/gold/kpi_composition_synergy")
        synergy = synergy.withColumn(
            "affix_ids_key",
            F.expr(
                "concat_ws('|', transform(array_sort(affix_ids), x -> cast(x as string)))"
            ),
        ).select(
            F.col("dungeon_id"),
            F.col("key_level"),
            F.col("affix_ids_key"),
            F.col("comp_signature"),
            F.col("synergy_score"),
        )
        # Join on all grouping keys: dungeon_id, key_level, affix_ids, comp_signature
        dr = dr.join(
            synergy,
            on=["dungeon_id", "key_level", "affix_ids_key", "comp_signature"],
            how="left",
        )
        logger.info("Joined synergy KPI")
    except Exception as e:
        logger.warning("synergy KPI not available (%s: %s), using NULLs.", type(e).__name__, e)
        dr = dr.withColumn("synergy_score", F.lit(None).cast(DoubleType()))

    # ── 4. Role count features from roster ──────────────────────────────────────
    dr = dr.withColumn(
        "num_tanks",
        F.expr("size(filter(roster, x -> x.role = 'tank'))"),
    ).withColumn(
        "num_healers",
        F.expr("size(filter(roster, x -> x.role = 'healer'))"),
    ).withColumn(
        "num_dps",
        F.expr("size(filter(roster, x -> x.role = 'dps'))"),
    )

    # ── 5. Log1p transform on death_clock_seconds (skewed) ─────────────────────
    dr = dr.withColumn(
        "death_clock_seconds_log1p",
        F.log1p(F.col("death_clock_seconds")),
    )

    # ── 6. Affix binary flags (multi-hot) ──────────────────────────────────────
    for affix_id in AFFIX_IDS:
        col_name = f"affix_{affix_id}"
        dr = dr.withColumn(
            col_name,
            F.when(
                F.array_contains(F.col("affix_ids"), affix_id), 1
            ).otherwise(0),
        )

    # ── 7. Dungeon one-hot encoding ─────────────────────────────────────────────
    for dungeon_id in DUNGEON_IDS:
        col_name = f"dungeon_{dungeon_id}"
        dr = dr.withColumn(
            col_name,
            F.when(F.col("dungeon_id") == dungeon_id, 1).otherwise(0),
        )

    # ── 8. Filter rows with NULL target ─────────────────────────────────────────
    dr = dr.filter(F.col(TARGET_COLUMN).isNotNull())

    # ── 9. Select final columns ────────────────────────────────────────────────
    select_cols = [
        "run_id",
        "season",
        "key_level",
        "dungeon_id",
        "completed_at",
        TARGET_COLUMN,
        "death_clock_seconds",
        "death_clock_seconds_log1p",
        "deficit_ratio",
        "interrupts_per_minute",
        "synergy_score",
        "num_tanks",
        "num_healers",
        "num_dps",
    ] + AFFIX_COLUMNS + DUNGEON_COLUMNS

    # Only select columns that exist (some might be missing if KPIs were unavailable)
    available_cols = [c for c in select_cols if c in dr.columns]
    result = dr.select(*available_cols)

    # ── 10. Write to gold/features/ partitioned by season and key_level ─────────
    gold_path = f"s3a://{bucket}/gold/features"
    final_count = result.count()
    result.write.mode("overwrite").partitionBy("season", "key_level").parquet(gold_path)
    logger.info("Feature view written: %d rows to %s", final_count, gold_path)

    return result