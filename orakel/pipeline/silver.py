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

            # Aggregate damage taken per (report_code, fight_id, actor_id)
            damage_agg = damage_df.groupBy(
                F.col("report_code"), F.col("fight_id"), F.col("actor_id")
            ).agg(
                F.sum("damage_amount").alias("total_damage_taken"),
                F.count("*").alias("damage_events"),
            )

            # Aggregate healing received per (report_code, fight_id, actor_id)
            healing_agg = healing_df.groupBy(
                F.col("report_code"), F.col("fight_id"), F.col("actor_id")
            ).agg(
                F.sum("damage_amount").alias("total_healing_received"),
                F.count("*").alias("healing_events"),
            )

            # Join damage + healing
            combat_stats = damage_agg.join(
                healing_agg,
                on=["report_code", "fight_id", "actor_id"],
                how="full_outer",
            ).fillna(0, subset=["total_damage_taken", "total_healing_received"])

            # Join with dungeon_runs to get run_id and player info
            # First, we need to join combat_stats with match manifest
            # to get the rio_run_id, then with roster data
            player_perf = combat_stats.join(
                matches_df.select(
                    F.col("wcl_report_code"),
                    F.col("wcl_fight_id"),
                    F.col("rio_run_id"),
                ),
                on=(
                    (combat_stats["report_code"] == matches_df["wcl_report_code"])
                    & (combat_stats["fight_id"] == matches_df["wcl_fight_id"])
                ),
                how="inner",
            )

            # Join with rio_df to get roster + fight duration
            player_perf = player_perf.join(
                rio_df.select(
                    F.col("keystone_run_id"),
                    F.col("clear_time_ms").alias("fight_duration_ms"),
                    F.col("roster"),
                ),
                on=F.col("rio_run_id") == rio_df["keystone_run_id"],
                how="left",
            )

            # Explode roster to match actor_id with player info
            # This is a simplified version — in practice, masterData resolves actor IDs
            # For now, we create player_performance from roster + aggregated combat stats
            performance_rows = dungeon_runs.select(
                F.col("run_id"),
                F.col("rio_run_id"),
                F.col("wcl_report_code"),
                F.col("wcl_fight_id"),
                F.col("fight_duration_ms"),
                F.col("season"),
            ).join(
                rio_df.select(
                    F.col("keystone_run_id"),
                    F.col("clear_time_ms").alias("fight_duration_ms"),
                    F.explode(F.col("roster")).alias("player"),
                ),
                on=dungeon_runs["rio_run_id"] == rio_df["keystone_run_id"],
                how="left",
            ).select(
                dungeon_runs["run_id"],
                F.col("player.name").alias("player_name"),
                F.col("player.realm").alias("realm"),
                F.col("player.region").alias("region"),
                F.col("player.class").alias("class_name"),
                F.col("player.spec").alias("spec_name"),
                F.col("player.role").alias("role"),
                F.lit(None).cast("long").alias("total_damage_taken"),
                F.lit(None).cast("long").alias("total_healing_received"),
                F.lit(None).cast("int").alias("interrupts_cast"),
                F.lit(None).cast("int").alias("interrupts_successful"),
                F.lit(None).cast("long").alias("max_hp"),
                dungeon_runs["fight_duration_ms"],
                dungeon_runs["season"],
            )

            player_perf_df = spark.createDataFrame(
                performance_rows.rdd, schema=silver_player_performance_schema
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
        F.lit(None).cast("int").alias("interrupts_cast"),
        F.lit(None).cast("int").alias("interrupts_successful"),
        F.lit(None).cast("long").alias("max_hp"),
        F.lit(None).cast("long").alias("fight_duration_ms"),
        F.col("season"),
    )

    return spark.createDataFrame(player_perf.rdd, schema=silver_player_performance_schema)