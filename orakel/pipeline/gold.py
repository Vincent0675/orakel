"""Gold pipeline — KPI aggregations and dimension tables.

Computes all 4 KPIs and builds dimension tables:
  - KPI 1: Tank Death Clock (DTPS, HPS, EHP → seconds until death)
  - KPI 2: Healer Deficit (tank DTPS / healer HPS ratio)
  - KPI 3: Interrupt Success Rate (successful / total casts)
  - KPI 4: Composition Synergy Score (comp avg / overall avg)
"""

from __future__ import annotations

import logging

from py4j.protocol import Py4JError
from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from orakel.config import settings
from orakel.models.kpi import compute_death_clock, compute_healer_deficit, compute_interrupt_rate
from orakel.models.schemas import (
    dim_affix_schema,
    dim_dungeon_schema,
    dim_player_schema,
    dim_spec_schema,
    gold_kpi_composition_synergy_schema,
    gold_kpi_healer_deficit_schema,
    gold_kpi_interrupt_rate_schema,
    gold_kpi_tank_death_clock_schema,
)

logger = logging.getLogger(__name__)


# ─── WoW Spec Role Mapping (hardcoded for MVP) ─────────────────────────────

# Source: https://wowpedia.fandom.com/wiki/Specializations
# Maps (class_name, spec_name) → role
_WOW_SPEC_ROLE_MAP: list[dict] = [
    # Death Knight
    {"class_id": 6, "class_name": "Death Knight", "spec_name": "Blood", "role": "tank", "is_healer": False, "is_tank": True, "is_dps": False},
    {"class_id": 6, "class_name": "Death Knight", "spec_name": "Frost", "role": "dps", "is_healer": False, "is_tank": False, "is_dps": True},
    {"class_id": 6, "class_name": "Death Knight", "spec_name": "Unholy", "role": "dps", "is_healer": False, "is_tank": False, "is_dps": True},
    # Demon Hunter
    {"class_id": 12, "class_name": "Demon Hunter", "spec_name": "Havoc", "role": "dps", "is_healer": False, "is_tank": False, "is_dps": True},
    {"class_id": 12, "class_name": "Demon Hunter", "spec_name": "Vengeance", "role": "tank", "is_healer": False, "is_tank": True, "is_dps": False},
    # Druid
    {"class_id": 11, "class_name": "Druid", "spec_name": "Balance", "role": "dps", "is_healer": False, "is_tank": False, "is_dps": True},
    {"class_id": 11, "class_name": "Druid", "spec_name": "Feral", "role": "dps", "is_healer": False, "is_tank": False, "is_dps": True},
    {"class_id": 11, "class_name": "Druid", "spec_name": "Guardian", "role": "tank", "is_healer": False, "is_tank": True, "is_dps": False},
    {"class_id": 11, "class_name": "Druid", "spec_name": "Restoration", "role": "healer", "is_healer": True, "is_tank": False, "is_dps": False},
    # Evoker
    {"class_id": 13, "class_name": "Evoker", "spec_name": "Devastation", "role": "dps", "is_healer": False, "is_tank": False, "is_dps": True},
    {"class_id": 13, "class_name": "Evoker", "spec_name": "Preservation", "role": "healer", "is_healer": True, "is_tank": False, "is_dps": False},
    {"class_id": 13, "class_name": "Evoker", "spec_name": "Augmentation", "role": "dps", "is_healer": False, "is_tank": False, "is_dps": True},
    # Hunter
    {"class_id": 3, "class_name": "Hunter", "spec_name": "Beast Mastery", "role": "dps", "is_healer": False, "is_tank": False, "is_dps": True},
    {"class_id": 3, "class_name": "Hunter", "spec_name": "Marksmanship", "role": "dps", "is_healer": False, "is_tank": False, "is_dps": True},
    {"class_id": 3, "class_name": "Hunter", "spec_name": "Survival", "role": "dps", "is_healer": False, "is_tank": False, "is_dps": True},
    # Mage
    {"class_id": 8, "class_name": "Mage", "spec_name": "Arcane", "role": "dps", "is_healer": False, "is_tank": False, "is_dps": True},
    {"class_id": 8, "class_name": "Mage", "spec_name": "Fire", "role": "dps", "is_healer": False, "is_tank": False, "is_dps": True},
    {"class_id": 8, "class_name": "Mage", "spec_name": "Frost", "role": "dps", "is_healer": False, "is_tank": False, "is_dps": True},
    # Monk
    {"class_id": 10, "class_name": "Monk", "spec_name": "Brewmaster", "role": "tank", "is_healer": False, "is_tank": True, "is_dps": False},
    {"class_id": 10, "class_name": "Monk", "spec_name": "Mistweaver", "role": "healer", "is_healer": True, "is_tank": False, "is_dps": False},
    {"class_id": 10, "class_name": "Monk", "spec_name": "Windwalker", "role": "dps", "is_healer": False, "is_tank": False, "is_dps": True},
    # Paladin
    {"class_id": 2, "class_name": "Paladin", "spec_name": "Holy", "role": "healer", "is_healer": True, "is_tank": False, "is_dps": False},
    {"class_id": 2, "class_name": "Paladin", "spec_name": "Protection", "role": "tank", "is_healer": False, "is_tank": True, "is_dps": False},
    {"class_id": 2, "class_name": "Paladin", "spec_name": "Retribution", "role": "dps", "is_healer": False, "is_tank": False, "is_dps": True},
    # Priest
    {"class_id": 5, "class_name": "Priest", "spec_name": "Discipline", "role": "healer", "is_healer": True, "is_tank": False, "is_dps": False},
    {"class_id": 5, "class_name": "Priest", "spec_name": "Holy", "role": "healer", "is_healer": True, "is_tank": False, "is_dps": False},
    {"class_id": 5, "class_name": "Priest", "spec_name": "Shadow", "role": "dps", "is_healer": False, "is_tank": False, "is_dps": True},
    # Rogue
    {"class_id": 4, "class_name": "Rogue", "spec_name": "Assassination", "role": "dps", "is_healer": False, "is_tank": False, "is_dps": True},
    {"class_id": 4, "class_name": "Rogue", "spec_name": "Outlaw", "role": "dps", "is_healer": False, "is_tank": False, "is_dps": True},
    {"class_id": 4, "class_name": "Rogue", "spec_name": "Subtlety", "role": "dps", "is_healer": False, "is_tank": False, "is_dps": True},
    # Shaman
    {"class_id": 7, "class_name": "Shaman", "spec_name": "Elemental", "role": "dps", "is_healer": False, "is_tank": False, "is_dps": True},
    {"class_id": 7, "class_name": "Shaman", "spec_name": "Enhancement", "role": "dps", "is_healer": False, "is_tank": False, "is_dps": True},
    {"class_id": 7, "class_name": "Shaman", "spec_name": "Restoration", "role": "healer", "is_healer": True, "is_tank": False, "is_dps": False},
    # Warlock
    {"class_id": 9, "class_name": "Warlock", "spec_name": "Affliction", "role": "dps", "is_healer": False, "is_tank": False, "is_dps": True},
    {"class_id": 9, "class_name": "Warlock", "spec_name": "Demonology", "role": "dps", "is_healer": False, "is_tank": False, "is_dps": True},
    {"class_id": 9, "class_name": "Warlock", "spec_name": "Destruction", "role": "dps", "is_healer": False, "is_tank": False, "is_dps": True},
    # Warrior
    {"class_id": 1, "class_name": "Warrior", "spec_name": "Arms", "role": "dps", "is_healer": False, "is_tank": False, "is_dps": True},
    {"class_id": 1, "class_name": "Warrior", "spec_name": "Fury", "role": "dps", "is_healer": False, "is_tank": False, "is_dps": True},
    {"class_id": 1, "class_name": "Warrior", "spec_name": "Protection", "role": "tank", "is_healer": False, "is_tank": True, "is_dps": False},
]

# TWW Season 3 affix data (hardcoded for MVP)
_TWW3_AFFIXES: list[dict] = [
    {"affix_id": 1, "affix_name": "Overflowing", "affix_description": "Healing allies accumulates overhealing on them, which will trigger an explosion of damage at 10% of their max health.", "affix_icon": "https://render.worldofwarcraft.com/us/images/affix/1.jpg", "season": "season-tww-3"},
    {"affix_id": 2, "affix_name": "Skittish", "affix_description": "Enemies no longer have a primary threat target, making tanking more difficult.", "affix_icon": "https://render.worldofwarcraft.com/us/images/affix/2.jpg", "season": "season-tww-3"},
    {"affix_id": 3, "affix_name": "Volcanic", "affix_description": "While in combat, enemies periodically summon volcanic plumes that deal damage.", "affix_icon": "https://render.worldofwarcraft.com/us/images/affix/3.jpg", "season": "season-tww-3"},
    {"affix_id": 4, "affix_name": "Necrotic", "affix_description": "Enemy attacks apply a stacking debuff that reduces healing received.", "affix_icon": "https://render.worldofwarcraft.com/us/images/affix/4.jpg", "season": "season-tww-3"},
    {"affix_id": 6, "affix_name": "Raging", "affix_description": "Enemy health bars always show, and enemies enrage at 30% health.", "affix_icon": "https://render.worldofwarcraft.com/us/images/affix/6.jpg", "season": "season-tww-3"},
    {"affix_id": 7, "affix_name": "Bolstering", "affix_description": "When any non-boss enemy dies, its death empowers nearby enemies.", "affix_icon": "https://render.worldofwarcraft.com/us/images/affix/7.jpg", "season": "season-tww-3"},
    {"affix_id": 8, "affix_name": "Sanguine", "affix_description": "When a non-boss enemy dies, a pool of blood is left that heals enemies and damages players.", "affix_icon": "https://render.worldofwarcraft.com/us/images/affix/8.jpg", "season": "season-tww-3"},
    {"affix_id": 9, "affix_name": "Tyrannical", "affix_description": "Boss enemies have 30% more health and 15% more damage.", "affix_icon": "https://render.worldofwarcraft.com/us/images/affix/9.jpg", "season": "season-tww-3"},
    {"affix_id": 10, "affix_name": "Fortified", "affix_description": "Non-boss enemies have 30% more health and 15% more damage.", "affix_icon": "https://render.worldofwarcraft.com/us/images/affix/10.jpg", "season": "season-tww-3"},
    {"affix_id": 11, "affix_name": "Bursting", "affix_description": "When a non-boss enemy dies, all players receive a stacking damage-over-time debuff.", "affix_icon": "https://render.worldofwarcraft.com/us/images/affix/11.jpg", "season": "season-tww-3"},
    {"affix_id": 12, "affix_name": "Grievous", "affix_description": "While below 90% health, players are afflicted by Grievous Wound.", "affix_icon": "https://render.worldofwarcraft.com/us/images/affix/12.jpg", "season": "season-tww-3"},
    {"affix_id": 13, "affix_name": "Explosive", "affix_description": "Enemies occasionally summon Explosive Orbs that must be destroyed or they detonate.", "affix_icon": "https://render.worldofwarcraft.com/us/images/affix/13.jpg", "season": "season-tww-3"},
    {"affix_id": 14, "affix_name": "Quaking", "affix_description": "Periodically, players must stop casting and move or take damage.", "affix_icon": "https://render.worldofwarcraft.com/us/images/affix/14.jpg", "season": "season-tww-3"},
    {"affix_id": 122, "affix_name": "Xal'atath's Guile", "affix_description": "Xal'atath whispers, empowering enemies with void energy.", "affix_icon": "https://render.worldofwarcraft.com/us/images/affix/122.jpg", "season": "season-tww-3"},
    {"affix_id": 123, "affix_name": "Challengers's Burden", "affix_description": "Slain enemies leave behind orbs that reduce haste and movement speed.", "affix_icon": "https://render.worldofwarcraft.com/us/images/affix/123.jpg", "season": "season-tww-3"},
    {"affix_id": 124, "affix_name": "Pheromone Veil", "affix_description": "Enemies periodically gain immunity to interrupts and crowd control.", "affix_icon": "https://render.worldofwarcraft.com/us/images/affix/124.jpg", "season": "season-tww-3"},
]

# TWW Season 3 dungeon timer data (hardcoded for MVP)
_TWW3_DUNGEONS: list[dict] = [
    {"dungeon_id": 15093, "dungeon_name": "Ara-Kara, City of Echoes", "slug": "ara-kara-city-of-echoes", "keystone_timer_ms": 2100000, "season": "season-tww-3"},
    {"dungeon_id": 16104, "dungeon_name": "Eco-Dome Al'dani", "slug": "eco-dome-aldani", "keystone_timer_ms": 2100000, "season": "season-tww-3"},
    {"dungeon_id": 12831, "dungeon_name": "Halls of Atonement", "slug": "halls-of-atonement", "keystone_timer_ms": 2100000, "season": "season-tww-3"},
    {"dungeon_id": 15452, "dungeon_name": "Operation: Floodgate", "slug": "operation-floodgate", "keystone_timer_ms": 2100000, "season": "season-tww-3"},
    {"dungeon_id": 14954, "dungeon_name": "Priory of the Sacred Flame", "slug": "priory-of-the-sacred-flame", "keystone_timer_ms": 2100000, "season": "season-tww-3"},
    {"dungeon_id": 1000001, "dungeon_name": "Tazavesh: So'leah's Gambit", "slug": "tazavesh-soleahs-gambit", "keystone_timer_ms": 2100000, "season": "season-tww-3"},
    {"dungeon_id": 1000000, "dungeon_name": "Tazavesh: Streets of Wonder", "slug": "tazavesh-streets-of-wonder", "keystone_timer_ms": 2100000, "season": "season-tww-3"},
    {"dungeon_id": 14971, "dungeon_name": "The Dawnbreaker", "slug": "the-dawnbreaker", "keystone_timer_ms": 2100000, "season": "season-tww-3"},
]


class GoldPipeline:
    """Gold-layer transformations: KPI aggregations and dimension tables."""

    @staticmethod
    def _build_comp_signature(roster_col: F.Column) -> F.Column:
        """Build a composition signature string from the roster array.

        Format: sorted "class-spec_role" per player, colon-separated.
        Example: "paladin-protection_tank:warrior-fury_dps:..."

        The roster is an array of structs with fields:
        name, realm, region, class, spec, role
        """
        # Build "class-spec_role" for each player, handling nulls
        player_sig = F.concat_ws(
            "-",
            F.coalesce(F.col("class"), F.lit("unknown")),
            F.concat(
                F.coalesce(F.col("spec"), F.lit("unknown")),
                F.lit("_"),
                F.coalesce(F.col("role"), F.lit("unknown")),
            ),
        )

        # Transform roster array into array of signature strings
        sig_array = F.transform(roster_col, lambda p: F.concat_ws(
            "-",
            F.coalesce(p.getField("class"), F.lit("unknown")),
            F.concat(
                F.coalesce(p.getField("spec"), F.lit("unknown")),
                F.lit("_"),
                F.coalesce(p.getField("role"), F.lit("unknown")),
            ),
        ))

        # Sort the array and join with ":"
        sorted_sig = F.array_sort(sig_array)
        return F.concat_ws(":", sorted_sig)

    @staticmethod
    def compute_kpi_synergy(spark: SparkSession, season: str) -> DataFrame:
        """Compute KPI 4 — Composition Synergy Score.

        Groups Silver dungeon_runs by (dungeon_id, key_level, affix_ids,
        comp_signature) and computes:
            synergy_score = avg_clear_time(comp) / avg_clear_time(all comps in group)

        Requires ≥ 2 samples per comp to compute score; otherwise NULL.

        Args:
            spark: Active SparkSession.
            season: Season filter.

        Returns:
            DataFrame with synergy KPI columns, written to Gold.
        """
        silver_path = f"s3a://{settings.MINIO_BUCKET}/silver/raiderio_runs"
        logger.info("Reading Silver data from %s", silver_path)

        silver_df = spark.read.parquet(silver_path).filter(
            F.col("season") == season
        )

        # ── Build comp_signature from roster ────────────────────────────────
        gold = GoldPipeline
        silver_with_sig = silver_df.withColumn(
            "comp_signature",
            gold._build_comp_signature(F.col("roster")),
        )

        # ── Group by (dungeon, key_level, affix_ids, comp_signature) ──────
        # NOTE: sort affix array so [10, 9, 147] and [9, 10, 147] don't
        # create separate groups for the same affix set.
        group_cols = [
            F.col("dungeon_id"),
            F.col("dungeon_name"),
            F.col("mythic_level").alias("key_level"),
            F.array_sort(F.col("weekly_modifiers")).alias("affix_ids"),
            F.col("comp_signature"),
        ]

        comp_agg = silver_with_sig.groupBy(*group_cols).agg(
            F.avg("clear_time_ms").alias("avg_clear_time_ms"),
            F.count("*").alias("sample_count"),
        )

        # ── Compute overall avg per (dungeon, key_level, affix_ids) ───────
        overall_agg = silver_with_sig.groupBy(
            F.col("dungeon_id"),
            F.col("mythic_level").alias("key_level"),
            F.array_sort(F.col("weekly_modifiers")).alias("affix_ids"),
        ).agg(
            F.avg("clear_time_ms").alias("overall_avg_clear_time_ms"),
        )

        # ── Join comp avg with overall avg ─────────────────────────────────
        joined = comp_agg.join(
            overall_agg,
            on=["dungeon_id", "key_level", "affix_ids"],
            how="left",
        )

        # ── Compute synergy score, NULL for sample_count < 2 ──────────────
        result = joined.withColumn(
            "synergy_score",
            F.when(
                F.col("sample_count") >= 2,
                F.round(F.col("avg_clear_time_ms") / F.col("overall_avg_clear_time_ms"), 4),
            ).otherwise(F.lit(None).cast(DoubleType())),
        )

        # ── Select final columns matching Gold schema ──────────────────────
        result = result.select(
            F.col("dungeon_id"),
            F.col("dungeon_name"),
            F.col("key_level"),
            F.col("affix_ids"),
            F.col("comp_signature"),
            F.col("avg_clear_time_ms"),
            F.col("overall_avg_clear_time_ms"),
            F.col("synergy_score"),
            F.col("sample_count"),
        )

        # ── Write to Gold ──────────────────────────────────────────────────
        gold_path = f"s3a://{settings.MINIO_BUCKET}/gold/kpi_composition_synergy"
        row_count = result.count()
        logger.info("Writing %d rows to %s", row_count, gold_path)

        result.write.mode("overwrite").parquet(gold_path)

        logger.info(
            "KPI 4 (Composition Synergy) written: %d rows to %s",
            row_count,
            gold_path,
        )

        return result

    @staticmethod
    def build_dim_dungeon(spark: SparkSession, season: str) -> DataFrame:
        """Build the dungeon dimension table from Silver data.

        Extracts unique dungeons from Silver raiderio_runs and enriches
        with hardcoded timer data for TWW Season 3.

        Args:
            spark: Active SparkSession.
            season: Season filter.

        Returns:
            DataFrame with dim_dungeon columns, written to Gold.
        """
        silver_path = f"s3a://{settings.MINIO_BUCKET}/silver/raiderio_runs"
        silver_df = spark.read.parquet(silver_path).filter(
            F.col("season") == season
        )

        # Get unique dungeons from data
        dungeons_from_data = silver_df.select(
            F.col("dungeon_id"),
            F.col("dungeon_name"),
        ).distinct()

        # Create hardcoded timer data
        timer_data = spark.createDataFrame(
            _TWW3_DUNGEONS,
            schema=StructType([
                StructField("dungeon_id", IntegerType(), nullable=False),
                StructField("dungeon_name", StringType(), nullable=True),
                StructField("slug", StringType(), nullable=True),
                StructField("keystone_timer_ms", LongType(), nullable=True),
                StructField("season", StringType(), nullable=True),
            ]),
        )

        # Left join data with timers (to get slugs and timer info)
        dim = dungeons_from_data.join(
            timer_data,
            on=["dungeon_id", "dungeon_name"],
            how="left",
        )

        # Fill season for all rows
        dim = dim.withColumn("season", F.lit(season))

        # Select final schema columns
        dim = dim.select(
            F.col("dungeon_id"),
            F.col("dungeon_name"),
            F.col("slug"),
            F.col("keystone_timer_ms"),
            F.col("season"),
        )

        gold_path = f"s3a://{settings.MINIO_BUCKET}/gold/dim_dungeon"
        row_count = dim.count()
        dim.write.mode("overwrite").parquet(gold_path)

        logger.info("dim_dungeon written: %d rows to %s", row_count, gold_path)
        return dim

    @staticmethod
    def build_dim_player(spark: SparkSession, season: str) -> DataFrame:
        """Build the player dimension table from Silver data.

        Explodes the roster array to extract individual players with
        their class, spec, and role information.

        Args:
            spark: Active SparkSession.
            season: Season filter.

        Returns:
            DataFrame with dim_player columns, written to Gold.
        """
        silver_path = f"s3a://{settings.MINIO_BUCKET}/silver/raiderio_runs"
        silver_df = spark.read.parquet(silver_path).filter(
            F.col("season") == season
        )

        # Explode roster to get individual players
        players = silver_df.select(
            F.explode(F.col("roster")).alias("player"),
        ).select(
            F.col("player.name").alias("player_name"),
            F.col("player.realm").alias("realm"),
            F.col("player.region").alias("region"),
            F.col("player.class").alias("class_name"),
            F.col("player.spec").alias("spec_name"),
            F.col("player.role").alias("role"),
        ).distinct()

        # Add class_id via a mapping (hardcoded for MVP)
        # class_id mapping based on WoW data
        class_id_map = {
            "Warrior": 1, "Paladin": 2, "Hunter": 3, "Rogue": 4,
            "Priest": 5, "Death Knight": 6, "Shaman": 7, "Mage": 8,
            "Warlock": 9, "Monk": 10, "Druid": 11, "Demon Hunter": 12,
            "Evoker": 13,
        }
        # Create mapping DataFrame
        class_id_rows = [
            (name, cid) for name, cid in class_id_map.items()
        ]
        class_id_df = spark.createDataFrame(
            class_id_rows,
            schema=StructType([
                StructField("class_name", StringType(), nullable=True),
                StructField("class_id", IntegerType(), nullable=True),
            ]),
        )

        dim = players.join(class_id_df, on="class_name", how="left")
        dim = dim.select(
            F.col("player_name"),
            F.col("realm"),
            F.col("region"),
            F.col("class_id"),
            F.col("class_name"),
            F.col("spec_name"),
            F.col("role"),
        )

        gold_path = f"s3a://{settings.MINIO_BUCKET}/gold/dim_player"
        row_count = dim.count()
        dim.write.mode("overwrite").parquet(gold_path)

        logger.info("dim_player written: %d rows to %s", row_count, gold_path)
        return dim

    @staticmethod
    def build_dim_affix(spark: SparkSession, season: str) -> DataFrame:
        """Build the affix dimension table (hardcoded for MVP).

        Args:
            spark: Active SparkSession.
            season: Season identifier.

        Returns:
            DataFrame with dim_affix columns, written to Gold.
        """
        dim = spark.createDataFrame(_TWW3_AFFIXES, schema=dim_affix_schema)

        gold_path = f"s3a://{settings.MINIO_BUCKET}/gold/dim_affix"
        row_count = dim.count()
        dim.write.mode("overwrite").parquet(gold_path)

        logger.info("dim_affix written: %d rows to %s", row_count, gold_path)
        return dim

    @staticmethod
    def build_dim_spec(spark: SparkSession) -> DataFrame:
        """Build the spec-role mapping dimension table (hardcoded for MVP).

        Args:
            spark: Active SparkSession.

        Returns:
            DataFrame with dim_spec columns, written to Gold.
        """
        dim = spark.createDataFrame(_WOW_SPEC_ROLE_MAP, schema=dim_spec_schema)

        gold_path = f"s3a://{settings.MINIO_BUCKET}/gold/dim_spec"
        row_count = dim.count()
        dim.write.mode("overwrite").parquet(gold_path)

        logger.info("dim_spec written: %d rows to %s", row_count, gold_path)
        return dim

    # ─── KPI 1: Tank Death Clock ───────────────────────────────────────────

    @staticmethod
    def compute_kpi_death_clock(spark: SparkSession, season: str) -> DataFrame:
        """Compute KPI 1 — Tank Death Clock per run.

        Death Clock = EHP / (DTPS - HPS_on_tank)
        Where:
          - DTPS = total_damage_taken / fight_duration_seconds
          - HPS_on_tank = total_healing_received / fight_duration_seconds
          - EHP = max_hp (MVP approximation)
          - If DTPS <= HPS_on_tank → infinite survival → sentinel value -1.0, "safe"

        Reads from Silver player_performance for tanks only.

        Args:
            spark: Active SparkSession.
            season: Season filter.

        Returns:
            DataFrame with Tank Death Clock KPI, written to Gold.
        """
        # Try to read from silver/player_performance if available
        pp_path = f"s3a://{settings.MINIO_BUCKET}/silver/player_performance"
        try:
            player_perf = spark.read.parquet(pp_path).filter(
                F.col("season") == season
            )
        except (AnalysisException, Py4JError, OSError) as e:
            logger.warning(
                "silver/player_performance not found (%s: %s). "
                "Computing KPI 1 from Raider.IO-only Silver data.",
                type(e).__name__,
                e,
            )
            # Fallback: use silver/raiderio_runs with estimated stats
            rio_path = f"s3a://{settings.MINIO_BUCKET}/silver/raiderio_runs"
            rio_df = spark.read.parquet(rio_path).filter(F.col("season") == season)
            return GoldPipeline._compute_death_clock_from_raiderio(spark, rio_df, season)

        # Filter to tanks only
        tanks = player_perf.filter(F.col("role") == "tank")

        tank_count = tanks.count()
        if tank_count == 0:
            logger.warning("No tank data found in player_performance.")
            rio_path = f"s3a://{settings.MINIO_BUCKET}/silver/raiderio_runs"
            rio_df = spark.read.parquet(rio_path).filter(F.col("season") == season)
            return GoldPipeline._compute_death_clock_from_raiderio(spark, rio_df, season)

        # Compute DTPS and HPS for tanks
        # DTPS = total_damage_taken / (fight_duration_ms / 1000)
        # HPS_on_tank = total_healing_received / (fight_duration_ms / 1000)
        tanks_with_stats = tanks.withColumn(
            "fight_duration_seconds",
            F.when(
                F.col("fight_duration_ms") > 0,
                F.col("fight_duration_ms") / 1000.0,
            ).otherwise(F.lit(1.0)),  # Avoid division by zero
        ).withColumn(
            "dtps",
            F.when(
                F.col("total_damage_taken").isNotNull() & (F.col("fight_duration_seconds") > 0),
                F.col("total_damage_taken") / F.col("fight_duration_seconds"),
            ).otherwise(F.lit(0.0)),
        ).withColumn(
            "hps_on_tank",
            F.when(
                F.col("total_healing_received").isNotNull() & (F.col("fight_duration_seconds") > 0),
                F.col("total_healing_received") / F.col("fight_duration_seconds"),
            ).otherwise(F.lit(0.0)),
        )

        # Join with dungeon_runs for dungeon_id, key_level, affix_ids
        dr_path = f"s3a://{settings.MINIO_BUCKET}/silver/dungeon_runs"
        try:
            dungeon_runs = spark.read.parquet(dr_path).filter(F.col("season") == season)
            # Get fight_duration_ms from dungeon_runs clear_time_ms as fallback
            tanks_joined = tanks_with_stats.join(
                dungeon_runs.select(
                    F.col("run_id"),
                    F.col("dungeon_id"),
                    F.col("key_level"),
                    F.col("affix_ids"),
                    F.col("clear_time_ms").alias("run_clear_time_ms"),
                ),
                on="run_id",
                how="left",
            )
        except (AnalysisException, Py4JError, OSError) as e:
            # If no dungeon_runs, use player_perf data directly
            logger.warning(
                "silver/dungeon_runs not found (%s: %s), using raiderio_runs fallback.",
                type(e).__name__,
                e,
            )
            rio_path = f"s3a://{settings.MINIO_BUCKET}/silver/raiderio_runs"
            rio_df = spark.read.parquet(rio_path).filter(F.col("season") == season)
            return GoldPipeline._compute_death_clock_from_raiderio(spark, rio_df, season)

        # Register KPI UDF
        death_clock_udf = F.udf(
            lambda dtps, hps, max_hp: compute_death_clock(dtps, hps, max_hp),
            returnType=StructType([
                StructField("death_clock_seconds", DoubleType(), nullable=True),
                StructField("death_clock_category", StringType(), nullable=True),
            ]),
        )

        # Compute Death Clock
        result = tanks_joined.withColumn(
            "dc_result",
            death_clock_udf(
                F.col("dtps"),
                F.col("hps_on_tank"),
                F.coalesce(F.col("max_hp"), F.lit(600000)),  # Default EHP for MVP
            ),
        ).withColumn(
            "death_clock_seconds", F.col("dc_result.death_clock_seconds"),
        ).withColumn(
            "death_clock_category", F.col("dc_result.death_clock_category"),
        ).withColumn(
            "ehp_estimate", F.coalesce(F.col("max_hp"), F.lit(600000)),
        ).select(
            F.col("run_id"),
            F.coalesce(F.col("dungeon_id"), F.lit(0)).alias("dungeon_id"),
            F.coalesce(F.col("key_level"), F.lit(0)).alias("key_level"),
            F.col("player_name").alias("tank_name"),
            F.col("class_name").alias("tank_class"),
            F.col("spec_name").alias("tank_spec"),
            F.col("dtps"),
            F.col("hps_on_tank"),
            F.col("ehp_estimate"),
            F.col("death_clock_seconds"),
            F.col("death_clock_category"),
            F.coalesce(F.col("fight_duration_ms"), F.col("run_clear_time_ms")).alias("fight_duration_ms"),
            F.col("affix_ids"),
        )

        # Write to Gold
        gold_path = f"s3a://{settings.MINIO_BUCKET}/gold/kpi_tank_death_clock"
        row_count = result.count()
        result.write.mode("overwrite").parquet(gold_path)
        logger.info("KPI 1 (Tank Death Clock): %d rows written to %s", row_count, gold_path)

        return result

    @staticmethod
    def _compute_death_clock_from_raiderio(
        spark: SparkSession, rio_df: DataFrame, season: str
    ) -> DataFrame:
        """Compute a placeholder Death Clock using Raider.IO data only.

        Creates estimated entries from tank stats in roster data,
        with NULL combat metrics (no WCL data available yet).

        Args:
            spark: Active SparkSession.
            rio_df: Silver Raider.IO DataFrame.
            season: Season filter.

        Returns:
            DataFrame with placeholder Death Clock KPI.
        """
        # Explode roster, filter tanks
        tanks = rio_df.select(
            F.expr("uuid()").alias("run_id"),
            F.col("dungeon_id"),
            F.col("mythic_level").alias("key_level"),
            F.explode(F.col("roster")).alias("player"),
            F.col("clear_time_ms").alias("fight_duration_ms"),
            F.array_sort(F.col("weekly_modifiers")).alias("affix_ids"),
            F.col("season"),
        ).filter(F.col("player.role") == "tank")

        result = tanks.select(
            F.col("run_id"),
            F.col("dungeon_id"),
            F.col("key_level"),
            F.col("player.name").alias("tank_name"),
            F.col("player.class").alias("tank_class"),
            F.col("player.spec").alias("tank_spec"),
            F.lit(None).cast("double").alias("dtps"),
            F.lit(None).cast("double").alias("hps_on_tank"),
            F.lit(None).cast("long").alias("ehp_estimate"),
            F.lit(None).cast("double").alias("death_clock_seconds"),
            F.lit(None).cast("string").alias("death_clock_category"),
            F.col("fight_duration_ms"),
            F.col("affix_ids"),
        )

        gold_path = f"s3a://{settings.MINIO_BUCKET}/gold/kpi_tank_death_clock"
        row_count = result.count()
        result.write.mode("overwrite").parquet(gold_path)
        logger.info(
            "KPI 1 (Tank Death Clock, raiderio-only): %d rows written to %s",
            row_count,
            gold_path,
        )
        return result

    # ─── KPI 2: Healer Deficit ─────────────────────────────────────────────

    @staticmethod
    def compute_kpi_healer_deficit(spark: SparkSession, season: str) -> DataFrame:
        """Compute KPI 2 — Healer Deficit per run.

        Deficit = Tank_DTPS / Healer_HPS_on_tank

        Categories:
          - comfortable: ratio < 1.0
          - moderate: ratio 1.0 – 1.2
          - critical: ratio > 1.2

        Args:
            spark: Active SparkSession.
            season: Season filter.

        Returns:
            DataFrame with Healer Deficit KPI, written to Gold.
        """
        pp_path = f"s3a://{settings.MINIO_BUCKET}/silver/player_performance"
        try:
            player_perf = spark.read.parquet(pp_path).filter(
                F.col("season") == season
            )
        except (AnalysisException, Py4JError, OSError) as e:
            logger.warning(
                "silver/player_performance not found (%s: %s). "
                "Computing KPI 2 from Raider.IO-only data.",
                type(e).__name__,
                e,
            )
            rio_path = f"s3a://{settings.MINIO_BUCKET}/silver/raiderio_runs"
            rio_df = spark.read.parquet(rio_path).filter(F.col("season") == season)
            return GoldPipeline._compute_healer_deficit_from_raiderio(spark, rio_df, season)

        # Separate tanks and healers
        # Bug #1 fix: total_healing_received now represents healing RECEIVED by
        # each player (aggregated by target_name).  For KPI 2 we need the HPS
        # that the tank actually received, so we use the TANK's row.
        tanks = player_perf.filter(F.col("role") == "tank").select(
            F.col("run_id").alias("tank_run_id"),
            F.col("player_name").alias("tank_name"),
            F.col("class_name").alias("tank_class"),
            F.col("total_damage_taken").alias("tank_damage_taken"),
            F.col("total_healing_received").alias("tank_healing_received"),
            F.col("fight_duration_ms").alias("tank_fight_duration"),
        )

        healers = player_perf.filter(F.col("role") == "healer").select(
            F.col("run_id").alias("healer_run_id"),
            F.col("player_name").alias("healer_name"),
            F.col("class_name").alias("healer_class"),
            F.col("spec_name").alias("healer_spec"),
        )

        # Join tanks and healers on run_id
        if tanks.count() == 0 or healers.count() == 0:
            logger.warning("No tank or healer data found. Producing empty KPI 2.")
            rio_path = f"s3a://{settings.MINIO_BUCKET}/silver/raiderio_runs"
            rio_df = spark.read.parquet(rio_path).filter(F.col("season") == season)
            return GoldPipeline._compute_healer_deficit_from_raiderio(spark, rio_df, season)

        joined = tanks.join(
            healers,
            on=F.col("tank_run_id") == F.col("healer_run_id"),
            how="inner",
        )

        # Compute DTPS and HPS on tank
        # tank_dtps = damage taken per second by the tank
        # hps_on_tank = healing per second received BY the tank (from all healers)
        joined = joined.withColumn(
            "tank_dtps",
            F.when(
                (F.col("tank_fight_duration") > 0) & F.col("tank_damage_taken").isNotNull(),
                F.col("tank_damage_taken") / (F.col("tank_fight_duration") / 1000.0),
            ).otherwise(F.lit(0.0)),
        ).withColumn(
            "healer_hps_on_tank",
            F.when(
                (F.col("tank_fight_duration") > 0) & F.col("tank_healing_received").isNotNull(),
                F.col("tank_healing_received") / (F.col("tank_fight_duration") / 1000.0),
            ).otherwise(F.lit(0.0)),
        )

        # Register KPI UDF
        healer_deficit_udf = F.udf(
            lambda tank_dtps, healer_hps: compute_healer_deficit(tank_dtps, healer_hps),
            returnType=StructType([
                StructField("deficit_ratio", DoubleType(), nullable=True),
                StructField("deficit_category", StringType(), nullable=True),
            ]),
        )

        # Compute Deficit
        result = joined.withColumn(
            "hd_result",
            healer_deficit_udf(F.col("tank_dtps"), F.col("healer_hps_on_tank")),
        ).withColumn(
            "deficit_ratio", F.col("hd_result.deficit_ratio"),
        ).withColumn(
            "deficit_category", F.col("hd_result.deficit_category"),
        ).select(
            F.col("tank_run_id").alias("run_id"),
            F.col("healer_name"),
            F.col("healer_class"),
            F.col("healer_spec"),
            F.col("tank_dtps"),
            F.col("healer_hps_on_tank"),
            F.col("deficit_ratio"),
            F.col("deficit_category"),
        )

        # Join with dungeon_runs for affix_ids
        dr_path = f"s3a://{settings.MINIO_BUCKET}/silver/dungeon_runs"
        try:
            dungeon_runs = spark.read.parquet(dr_path).filter(F.col("season") == season)
            result = result.join(
                dungeon_runs.select(F.col("run_id"), F.col("affix_ids")),
                on="run_id",
                how="left",
            )
        except (AnalysisException, Py4JError, OSError) as e:
            logger.warning(
                "silver/dungeon_runs not found for affix join (%s: %s), using null affix_ids.",
                type(e).__name__,
                e,
            )
            result = result.withColumn("affix_ids", F.lit(None).cast("array<int>"))

        gold_path = f"s3a://{settings.MINIO_BUCKET}/gold/kpi_healer_deficit"
        row_count = result.count()
        result.write.mode("overwrite").parquet(gold_path)
        logger.info("KPI 2 (Healer Deficit): %d rows written to %s", row_count, gold_path)

        return result

    @staticmethod
    def _compute_healer_deficit_from_raiderio(
        spark: SparkSession, rio_df: DataFrame, season: str
    ) -> DataFrame:
        """Compute placeholder Healer Deficit from Raider.IO data only."""
        healers = rio_df.select(
            F.expr("uuid()").alias("run_id"),
            F.explode(F.col("roster")).alias("player"),
            F.array_sort(F.col("weekly_modifiers")).alias("affix_ids"),
        ).filter(F.col("player.role") == "healer")

        result = healers.select(
            F.col("run_id"),
            F.col("player.name").alias("healer_name"),
            F.col("player.class").alias("healer_class"),
            F.col("player.spec").alias("healer_spec"),
            F.lit(None).cast("double").alias("tank_dtps"),
            F.lit(None).cast("double").alias("healer_hps_on_tank"),
            F.lit(None).cast("double").alias("deficit_ratio"),
            F.lit(None).cast("string").alias("deficit_category"),
            F.col("affix_ids"),
        )

        gold_path = f"s3a://{settings.MINIO_BUCKET}/gold/kpi_healer_deficit"
        row_count = result.count()
        result.write.mode("overwrite").parquet(gold_path)
        logger.info(
            "KPI 2 (Healer Deficit, raiderio-only): %d rows written to %s",
            row_count,
            gold_path,
        )
        return result

    # ─── KPI 3: Interrupt Success Rate ──────────────────────────────────────

    @staticmethod
    def compute_kpi_interrupt_rate(spark: SparkSession, season: str) -> DataFrame:
        """Compute KPI 3 — Interrupt Success Rate per player per run.

        ISR is computed from ``interrupts_count``, which records the number
        of successful interrupts per player per fight.  WCL only returns
        successful interrupt events — there is no "failed interrupt" event —
        so ISR will always be 100% when data is available.  When
        ``interrupts_count`` is 0 or NULL (player didn't attempt any
        interrupts), ISR is NULL (not 0, which would imply they failed).

        Args:
            spark: Active SparkSession.
            season: Season filter.

        Returns:
            DataFrame with Interrupt Rate KPI, written to Gold.
        """
        pp_path = f"s3a://{settings.MINIO_BUCKET}/silver/player_performance"
        try:
            player_perf = spark.read.parquet(pp_path).filter(
                F.col("season") == season
            )
        except (AnalysisException, Py4JError, OSError) as e:
            logger.warning(
                "silver/player_performance not found (%s: %s). "
                "Computing KPI 3 from Raider.IO-only data.",
                type(e).__name__,
                e,
            )
            rio_path = f"s3a://{settings.MINIO_BUCKET}/silver/raiderio_runs"
            rio_df = spark.read.parquet(rio_path).filter(F.col("season") == season)
            return GoldPipeline._compute_interrupt_rate_from_raiderio(spark, rio_df, season)

        # Register KPI UDF — since WCL only provides successful interrupts,
        # ISR = successful / total, where both are the same count.
        interrupt_rate_udf = F.udf(
            lambda successful, total: compute_interrupt_rate(successful, total),
            returnType=DoubleType(),
        )

        # Compute ISR per player per run
        result = player_perf.withColumn(
            "interrupt_success_rate",
            interrupt_rate_udf(
                F.coalesce(F.col("interrupts_count"), F.lit(0)),
                F.coalesce(F.col("interrupts_count"), F.lit(0)),
            ),
        ).select(
            F.col("run_id"),
            F.col("player_name"),
            F.col("class_name").alias("player_class"),
            F.col("spec_name").alias("player_spec"),
            F.col("role").alias("player_role"),
            F.coalesce(F.col("interrupts_count"), F.lit(0)).alias("interrupts_count"),
            F.col("interrupt_success_rate"),
            F.lit(None).cast("int").alias("dangerous_enemy_casts"),
            F.lit(None).cast("double").alias("interrupt_coverage"),
        )

        # Join with dungeon_runs for affix_ids
        dr_path = f"s3a://{settings.MINIO_BUCKET}/silver/dungeon_runs"
        try:
            # Join just for affix_ids
            dr = spark.read.parquet(dr_path).filter(F.col("season") == season)
            result = result.join(
                dr.select(F.col("run_id"), F.col("affix_ids")).distinct(),
                on="run_id",
                how="left",
            )
        except (AnalysisException, Py4JError, OSError) as e:
            logger.warning(
                "silver/dungeon_runs not found for interrupt join (%s: %s), "
                "using null dangerous_enemy_casts and interrupt_coverage.",
                type(e).__name__,
                e,
            )
            result = result.withColumn(
                "dangerous_enemy_casts",
                F.lit(None).cast("int"),
            ).withColumn(
                "interrupt_coverage",
                F.lit(None).cast("double"),
            )

        # Fill dangerous_enemy_casts and interrupt_coverage with NULLs if no WCL data
        # These require WCL event-level data which may not be available
        gold_path = f"s3a://{settings.MINIO_BUCKET}/gold/kpi_interrupt_success"
        row_count = result.count()
        result.write.mode("overwrite").parquet(gold_path)
        logger.info("KPI 3 (Interrupt Rate): %d rows written to %s", row_count, gold_path)

        return result

    @staticmethod
    def _compute_interrupt_rate_from_raiderio(
        spark: SparkSession, rio_df: DataFrame, season: str
    ) -> DataFrame:
        """Compute placeholder Interrupt Rate from Raider.IO data only."""
        players = rio_df.select(
            F.expr("uuid()").alias("run_id"),
            F.explode(F.col("roster")).alias("player"),
        )

        result = players.select(
            F.col("run_id"),
            F.col("player.name").alias("player_name"),
            F.col("player.class").alias("player_class"),
            F.col("player.spec").alias("player_spec"),
            F.col("player.role").alias("player_role"),
            F.lit(None).cast("int").alias("interrupts_count"),
            F.lit(None).cast("double").alias("interrupt_success_rate"),
            F.lit(None).cast("int").alias("dangerous_enemy_casts"),
            F.lit(None).cast("double").alias("interrupt_coverage"),
        )

        gold_path = f"s3a://{settings.MINIO_BUCKET}/gold/kpi_interrupt_success"
        row_count = result.count()
        result.write.mode("overwrite").parquet(gold_path)
        logger.info(
            "KPI 3 (Interrupt Rate, raiderio-only): %d rows written to %s",
            row_count,
            gold_path,
        )
        return result