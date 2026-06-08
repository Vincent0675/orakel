"""Silver pipeline — clean, dedup, type-cast Bronze data, and fuzzy-join.

The Silver layer deduplicates by natural key, enforces typed schemas,
flattens nested structs (realm → realm.slug, region → region.short_name),
and joins Raider.IO runs with WCL combat data via the match manifest.
"""

from __future__ import annotations

import logging
import uuid

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from pyspark.sql.window import Window

from orakel.config import settings
from orakel.models.schemas import (
    silver_dungeon_runs_schema,
    silver_player_performance_schema,
)

logger = logging.getLogger(__name__)


class SilverPipeline:
    """Silver-layer transformations: dedup, type enforcement, struct flattening."""

    @staticmethod
    def clean_raiderio(spark: SparkSession, season: str) -> DataFrame:
        """Read Bronze Raider.IO data, clean and dedup, write Silver Parquet.

        Steps:
            1. Read Bronze Parquet from ``bronze/raiderio/runs/``
            2. Deduplicate by ``keystone_run_id`` keeping latest ``ingested_at``
            3. Enforce column types per Silver schema
            4. Flatten nested ``realm`` struct → ``realm`` string (slug)
            5. Flatten nested ``region`` struct → ``region`` string (short_name)
            6. Write to ``silver/raiderio_runs/`` on MinIO

        Args:
            spark: Active SparkSession.
            season: Season filter (e.g. "season-tww-3").

        Returns:
            Cleaned Silver DataFrame (before write, for row-count reporting).
        """
        bronze_path = f"s3a://{settings.MINIO_BUCKET}/bronze/raiderio/runs"
        logger.info("Reading Bronze Raider.IO data from %s", bronze_path)

        bronze_df = spark.read.parquet(bronze_path)
        bronze_count = bronze_df.count()
        logger.info("Bronze row count (before dedup): %d", bronze_count)

        # ── Dedup by keystone_run_id, keeping latest ingested_at ──────────
        dedup_window = Window.partitionBy("keystone_run_id").orderBy(
            F.desc("ingested_at")
        )
        deduped_df = (
            bronze_df.withColumn("_row_num", F.row_number().over(dedup_window))
            .filter(F.col("_row_num") == 1)
            .drop("_row_num")
        )

        deduped_count = deduped_df.count()
        logger.info(
            "Silver row count (after dedup): %d (removed %d duplicates)",
            deduped_count,
            bronze_count - deduped_count,
        )

        # ── Flatten nested structs ─────────────────────────────────────────
        # roster[].realm is a struct {id, slug, name, ...} → extract slug
        # roster[].region is a struct {name, slug, short_name} → extract short_name
        # On re-execution, these fields may already be strings (pre-flattened),
        # so use the idempotent _flatten_roster helper.
        silver_df = deduped_df.withColumn("roster", SilverPipeline._flatten_roster(deduped_df, F.col("roster")))

        # ── Filter by season ───────────────────────────────────────────────
        silver_df = silver_df.filter(F.col("season") == season)

        # ── Write to Silver Parquet ────────────────────────────────────────
        silver_path = f"s3a://{settings.MINIO_BUCKET}/silver/raiderio_runs"
        logger.info("Writing %d rows to %s", silver_df.count(), silver_path)

        # Re-count after filter for accurate write count
        write_count = silver_df.count()
        silver_df.write.mode("overwrite").partitionBy("season").parquet(
            silver_path
        )

        logger.info(
            "Silver Raider.IO write complete: %d rows to %s",
            write_count,
            silver_path,
        )

        return silver_df

    @staticmethod
    def _flatten_roster(deduped_df: DataFrame, roster_col: F.Column) -> F.Column:
        """Flatten roster structs: extract realm.slug and region.short_name.

        Handles both struct and already-flat string types (idempotent).
        Type check is performed at schema level BEFORE building the
        transformation expression, because PySpark lambdas cannot inspect
        field types at expression-evaluation time.
        """
        roster_array_type = deduped_df.schema["roster"].dataType
        element_type = roster_array_type.elementType

        realm_is_struct = (
            isinstance(element_type, StructType)
            and "realm" in element_type.fieldNames()
            and element_type["realm"].dataType.typeName() == "struct"
        )
        region_is_struct = (
            isinstance(element_type, StructType)
            and "region" in element_type.fieldNames()
            and element_type["region"].dataType.typeName() == "struct"
        )

        if realm_is_struct and region_is_struct:
            # Both still structs — full flatten
            return F.transform(
                roster_col,
                lambda p: p.dropFields("realm", "region")
                .withField("realm", p.getField("realm").getField("slug"))
                .withField("region", p.getField("region").getField("short_name")),
            )
        elif realm_is_struct:
            # Only realm is still a struct
            return F.transform(
                roster_col,
                lambda p: p.dropFields("realm")
                .withField("realm", p.getField("realm").getField("slug")),
            )
        elif region_is_struct:
            # Only region is still a struct
            return F.transform(
                roster_col,
                lambda p: p.dropFields("region")
                .withField("region", p.getField("region").getField("short_name")),
            )
        else:
            # Both already flat strings — nothing to do
            return roster_col

    @staticmethod
    def apply_fuzzy_join(spark: SparkSession, season: str) -> tuple[DataFrame, DataFrame]:
        """Join Raider.IO runs with WCL data using the match manifest.

        Reads the match manifest from ``silver/matches/``, joins with
        both Raider.IO Silver data and WCL Bronze events to produce:
          1. ``silver/dungeon_runs/`` — enriched runs with WCL match metadata
          2. ``silver/player_performance/`` — per-player combat stats

        Args:
            spark: Active SparkSession.
            season: Season filter.

        Returns:
            Tuple of (dungeon_runs_df, player_performance_df).
        """
        logger.info("Starting fuzzy join for season=%s", season)

        # ── 1. Load Silver Raider.IO runs ────────────────────────────────────
        rio_path = f"s3a://{settings.MINIO_BUCKET}/silver/raiderio_runs"
        rio_df = spark.read.parquet(rio_path).filter(F.col("season") == season)
        logger.info("Silver Raider.IO: %d rows", rio_df.count())

        # ── 2. Load match manifest ────────────────────────────────────────────
        matches_path = f"s3a://{settings.MINIO_BUCKET}/silver/matches"
        try:
            matches_df = spark.read.parquet(matches_path).filter(
                F.col("season") == season
            )
            match_count = matches_df.count()
            logger.info("Match manifest: %d records", match_count)
        except Exception:
            logger.warning(
                "No match manifest found at %s. "
                "Run match_reports.py first to create it.",
                matches_path,
            )
            # Return Raider.IO-only runs with WCL columns set to null
            rio_only = rio_df.withColumn("run_id", F.expr("uuid()")) \
                .withColumn("wcl_report_code", F.lit(None).cast("string")) \
                .withColumn("wcl_fight_id", F.lit(None).cast("int")) \
                .withColumn("confidence", F.lit(None).cast("double")) \
                .withColumn("match_method", F.lit("rio_only")) \
                .withColumn("matched_at", F.lit(None).cast("timestamp"))
            return rio_only, spark.createDataFrame([], schema=silver_player_performance_schema)

        if match_count == 0:
            logger.warning("Match manifest is empty. Producing Raider.IO-only Silver.")
            rio_only = rio_df.withColumn("run_id", F.expr("uuid()")) \
                .withColumn("wcl_report_code", F.lit(None).cast("string")) \
                .withColumn("wcl_fight_id", F.lit(None).cast("int")) \
                .withColumn("confidence", F.lit(None).cast("double")) \
                .withColumn("match_method", F.lit("rio_only")) \
                .withColumn("matched_at", F.lit(None).cast("timestamp"))
            return rio_only, spark.createDataFrame([], schema=silver_player_performance_schema)

        # ── 3. Join Raider.IO with matches ───────────────────────────────────
        # The match manifest has: rio_run_id, wcl_report_code, wcl_fight_id,
        # confidence, match_method, matched_at
        joined = rio_df.join(
            matches_df.select(
                F.col("rio_run_id"),
                F.col("wcl_report_code"),
                F.col("wcl_fight_id"),
                F.col("confidence"),
                F.col("match_method"),
                F.col("matched_at"),
            ),
            on=F.col("keystone_run_id") == F.col("rio_run_id"),
            how="left",
        )

        # For unmatched runs, set match_method to rio_only
        joined = joined.withColumn(
            "match_method",
            F.when(F.col("match_method").isNull(), F.lit("rio_only"))
            .otherwise(F.col("match_method")),
        )

        # Generate run_id UUID
        joined = joined.withColumn("run_id", F.expr("uuid()"))

        # ── 4. Build silver/dungeon_runs ──────────────────────────────────────
        # Select and rename columns to match silver_dungeon_runs_schema
        dungeon_runs = joined.select(
            F.col("run_id"),
            F.col("keystone_run_id").alias("rio_run_id"),
            F.col("wcl_report_code"),
            F.col("wcl_fight_id"),
            F.col("dungeon_id"),
            F.col("mythic_level").alias("key_level"),
            F.array_sort(F.col("weekly_modifiers")).alias("affix_ids"),
            F.col("clear_time_ms"),
            F.col("completed_at"),
            F.col("confidence"),
            F.col("match_method"),
            F.col("roster"),
            F.col("season"),
            F.col("matched_at"),
        )

        # Enforce schema types
        dungeon_runs = spark.createDataFrame(
            dungeon_runs.rdd, schema=silver_dungeon_runs_schema
        )

        # ── 5. Build silver/player_performance ────────────────────────────────
        # Try to load WCL Bronze events for combat stats
        # If no WCL data exists yet, create empty player_performance
        try:
            damage_path = f"s3a://{settings.MINIO_BUCKET}/bronze/warcraftlogs/events/damage_taken"
            damage_df = spark.read.parquet(damage_path)

            healing_path = f"s3a://{settings.MINIO_BUCKET}/bronze/warcraftlogs/events/healing"
            healing_df = spark.read.parquet(healing_path)

            # Aggregate damage taken per (report_code, fight_id, player_name)
            # DamageTaken table entries are per-target (the player who took damage),
            # so player_name IS the damage target — this is correct.
            damage_agg = damage_df.groupBy(
                F.col("report_code"),
                F.col("fight_id"),
                F.lower(F.col("player_name")).alias("player_name"),
            ).agg(
                F.sum("damage_amount").alias("total_damage_taken"),
            )

            # Aggregate healing RECEIVED per (report_code, fight_id, target_name).
            # The WCL Healing table groups by source (healer), but new ingestion
            # (Bug #1 fix) stores per-target rows with `target_name` populated.
            # We aggregate by target_name so that each player gets the total
            # healing they received, not the total healing they dealt.
            # Fallback: for older data without target_name, group by player_name.
            has_targets = "target_name" in healing_df.columns
            if has_targets:
                # Prefer target-level aggregation (healing RECEIVED by each player)
                healing_received = healing_df.filter(
                    F.col("target_name").isNotNull()
                ).groupBy(
                    F.col("report_code"),
                    F.col("fight_id"),
                    F.lower(F.col("target_name")).alias("player_name"),
                ).agg(
                    F.sum("damage_amount").alias("total_healing_received"),
                )
            else:
                # Legacy data without target_name: aggregate by source (healer)
                # NOTE: this gives healing DONE, not healing RECEIVED.
                # KPI 2 (Healer Deficit) will be approximated with this.
                healing_received = healing_df.groupBy(
                    F.col("report_code"),
                    F.col("fight_id"),
                    F.lower(F.col("player_name")).alias("player_name"),
                ).agg(
                    F.sum("damage_amount").alias("total_healing_received"),
                )

            # Combine damage + healing received (full outer on name)
            # damage_agg: total damage TAKEN by each player (correct)
            # healing_received: total healing RECEIVED by each player (Bug #1 fix)
            combat_by_player = damage_agg.join(
                healing_received,
                on=["report_code", "fight_id", "player_name"],
                how="full_outer",
            ).fillna(0, subset=["total_damage_taken", "total_healing_received"])

            # Read interrupt events (now resolved with player_name via masterData)
            interrupt_path = f"s3a://{settings.MINIO_BUCKET}/bronze/warcraftlogs/events/interrupts"
            interrupt_df = spark.read.parquet(interrupt_path)

            # Count interrupts per (report_code, fight_id, player_name)
            # NOTE: WCL only returns successful interrupts — there is no "failed
            # interrupt" event.  Both counters would always be identical, so we
            # store a single `interrupts_count` instead of pretending we can
            # distinguish cast vs successful.
            # Normalize player_name to lowercase for matching
            interrupt_agg = interrupt_df.filter(F.col("player_name").isNotNull()).groupBy(
                F.col("report_code"),
                F.col("fight_id"),
                F.lower(F.col("player_name")).alias("player_name"),
            ).agg(
                F.count("*").alias("interrupts_count"),
            )

            # Build player_performance from combat data joined by player_name
            # Use player names from WCL events directly, join with roster flat
            # (avoid struct field ordering issues by reading from source)
            rio_path_flat = f"s3a://{settings.MINIO_BUCKET}/bronze/raiderio/runs"
            rio_raw = spark.read.parquet(rio_path_flat).filter(F.col("season") == season)
            roster_flat = rio_raw.select(
                F.col("keystone_run_id"),
                F.explode(F.col("roster")).alias("p"),
            ).select(
                F.col("keystone_run_id"),
                F.expr("p.name").alias("player_name"),
                F.expr("p.role").alias("role"),
                F.expr("p.class").alias("class_name"),
                F.expr("p.spec").alias("spec_name"),
            ).distinct()

            # Get run metadata from dungeon_runs (which already has match manifest linked)
            run_meta = dungeon_runs.select(
                F.col("run_id"),
                F.col("season"),
                F.col("clear_time_ms").alias("fight_duration_ms"),
                F.col("wcl_report_code"),
                F.col("wcl_fight_id"),
                F.col("rio_run_id"),
                F.col("roster"),
            )

            # Join with flat roster to get player info
            roster_with_meta = roster_flat.join(
                run_meta,
                on=F.col("keystone_run_id") == F.col("rio_run_id"),
                how="inner",
            ).select(
                F.col("run_id"),
                F.col("season"),
                F.col("fight_duration_ms"),
                F.col("wcl_report_code"),
                F.col("wcl_fight_id"),
                F.col("player_name"),
                F.col("role"),
                F.col("class_name"),
                F.col("spec_name"),
                F.lit(None).cast("string").alias("realm"),
                F.lit(None).cast("string").alias("region"),
            )

            # 2. Rename roster cols to match combat_by_player for join
            roster_for_join = roster_with_meta.select(
                F.col("run_id"),
                F.col("season"),
                F.col("fight_duration_ms"),
                F.col("wcl_report_code").alias("report_code"),
                F.col("wcl_fight_id").alias("fight_id"),
                F.lower(F.col("player_name")).alias("player_name"),
                F.col("realm"),
                F.col("region"),
                F.col("class_name"),
                F.col("spec_name"),
                F.col("role").alias("player_role"),
            )

            # 3. Join with combat data (damage, healing) by (report_code, fight_id, player_name) — LEFT
            player_perf_temp = roster_for_join.join(
                combat_by_player,
                on=["report_code", "fight_id", "player_name"],
                how="left",
            )

            # 4. Join with interrupt data by (report_code, fight_id, player_name) — LEFT
            player_perf_temp = player_perf_temp.join(
                interrupt_agg,
                on=["report_code", "fight_id", "player_name"],
                how="left",
            )

            # 5. Select final columns
            player_perf_df = player_perf_temp.select(
                F.col("run_id"),
                F.col("player_name"),
                F.col("realm"),
                F.col("region"),
                F.col("class_name"),
                F.col("spec_name"),
                F.col("player_role").alias("role"),
                F.when(F.col("total_damage_taken").isNotNull(), F.col("total_damage_taken"))
                 .otherwise(F.lit(None).cast("long")).alias("total_damage_taken"),
                F.when(F.col("total_healing_received").isNotNull(), F.col("total_healing_received"))
                 .otherwise(F.lit(None).cast("long")).alias("total_healing_received"),
                F.when(F.col("interrupts_count").isNotNull(), F.col("interrupts_count"))
                 .otherwise(F.lit(None).cast("int")).alias("interrupts_count"),
                F.lit(None).cast("long").alias("max_hp"),
                F.col("fight_duration_ms"),
                F.col("season"),
            )

            # Enforce schema
            player_perf_df = spark.createDataFrame(
                player_perf_df.rdd, schema=silver_player_performance_schema
            )

        except Exception as e:
            logger.warning(
                "Could not build player_performance from WCL data: %s. "
                "Creating empty player_performance from roster data.",
                e,
            )
            # Fallback: create player_performance from roster without combat stats
            player_perf_df = _build_player_perf_from_roster(spark, dungeon_runs)

        # ── 6. Write outputs ──────────────────────────────────────────────────
        dr_path = f"s3a://{settings.MINIO_BUCKET}/silver/dungeon_runs"
        pp_path = f"s3a://{settings.MINIO_BUCKET}/silver/player_performance"

        dr_count = dungeon_runs.count()
        pp_count = player_perf_df.count()

        logger.info("Writing %d dungeon_runs to %s", dr_count, dr_path)
        dungeon_runs.write.mode("overwrite").partitionBy("season").parquet(dr_path)

        logger.info("Writing %d player_performance to %s", pp_count, pp_path)
        player_perf_df.write.mode("overwrite").partitionBy("season").parquet(pp_path)

        logger.info(
            "Silver fuzzy join complete: %d dungeon_runs, %d player_performance",
            dr_count,
            pp_count,
        )

        return dungeon_runs, player_perf_df


def _build_player_perf_from_roster(
    spark: SparkSession, dungeon_runs: DataFrame
) -> DataFrame:
    """Build player_performance from roster data without WCL combat stats.

    Used as a fallback when WCL Bronze data is not available yet.
    Creates rows from the roster with NULL combat stats.

    Args:
        spark: Active SparkSession.
        dungeon_runs: Silver dungeon_runs DataFrame.

    Returns:
        DataFrame matching silver_player_performance_schema with NULL combat stats.
    """
    exploded = dungeon_runs.select(
        F.col("run_id"),
        F.explode(F.col("roster")).alias("player"),
        F.col("season"),
    )

    player_perf = exploded.select(
        F.col("run_id"),
        F.col("player.name").alias("player_name"),
        F.col("player.realm").alias("realm"),
        F.col("player.region").alias("region"),
        F.col("player.class").alias("class_name"),
        F.col("player.spec").alias("spec_name"),
        F.col("player.role").alias("role"),
        F.lit(None).cast("long").alias("total_damage_taken"),
        F.lit(None).cast("long").alias("total_healing_received"),
        F.lit(None).cast("int").alias("interrupts_count"),
        F.lit(None).cast("long").alias("max_hp"),
        F.lit(None).cast("long").alias("fight_duration_ms"),
        F.col("season"),
    )

    return spark.createDataFrame(player_perf.rdd, schema=silver_player_performance_schema)