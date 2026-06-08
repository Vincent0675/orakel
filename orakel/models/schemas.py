"""PySpark StructType schemas for all Medallion layers (Bronze/Silver/Gold)."""

from __future__ import annotations

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ─── Nested Structs for Raider.IO roster ─────────────────────────────────────

realm_struct = StructType([
    StructField("id", IntegerType(), nullable=True),
    StructField("connectedRealmId", IntegerType(), nullable=True),
    StructField("wowRealmId", IntegerType(), nullable=True),
    StructField("wowConnectedRealmId", IntegerType(), nullable=True),
    StructField("name", StringType(), nullable=True),
    StructField("slug", StringType(), nullable=True),
    StructField("locale", StringType(), nullable=True),
])

region_struct = StructType([
    StructField("name", StringType(), nullable=True),
    StructField("slug", StringType(), nullable=True),
    StructField("short_name", StringType(), nullable=True),
])

# ─── Bronze Layer ─────────────────────────────────────────────────────────────

bronze_raiderio_schema = StructType(
    [
        StructField("source", StringType(), nullable=False),
        StructField("keystone_run_id", LongType(), nullable=False),
        StructField("dungeon_id", IntegerType(), nullable=False),
        StructField("challenge_mode_id", IntegerType(), nullable=True),
        StructField("dungeon_name", StringType(), nullable=True),
        StructField("mythic_level", IntegerType(), nullable=False),
        StructField("clear_time_ms", LongType(), nullable=True),
        StructField("keystone_time_ms", LongType(), nullable=True),
        StructField("completed_at", TimestampType(), nullable=True),
        StructField(
            "weekly_modifiers", ArrayType(IntegerType()), nullable=True
        ),
        StructField(
            "roster",
            ArrayType(
                StructType(
                    [
                        StructField("name", StringType(), nullable=True),
                        StructField("class", StringType(), nullable=True),
                        StructField("spec", StringType(), nullable=True),
                        StructField("role", StringType(), nullable=True),
                        StructField("realm", realm_struct, nullable=True),
                        StructField("region", region_struct, nullable=True),
                    ]
                )
            ),
            nullable=True,
        ),
        StructField("score", DoubleType(), nullable=True),
        StructField("rank", IntegerType(), nullable=True),
        StructField("season", StringType(), nullable=False),
        StructField("ingested_at", TimestampType(), nullable=False),
    ]
)

bronze_wcl_reports_schema = StructType(
    [
        StructField("source", StringType(), nullable=False),
        StructField("report_code", StringType(), nullable=False),
        StructField("report_title", StringType(), nullable=True),
        StructField("zone_id", IntegerType(), nullable=True),
        StructField("start_time", LongType(), nullable=True),
        StructField("end_time", LongType(), nullable=True),
        StructField("owner_name", StringType(), nullable=True),
        StructField("guild_name", StringType(), nullable=True),
        StructField("visibility", StringType(), nullable=True),
        StructField(
            "fights",
            ArrayType(
                StructType(
                    [
                        StructField("fight_id", IntegerType(), nullable=True),
                        StructField(
                            "encounter_id", IntegerType(), nullable=True
                        ),
                        StructField(
                            "keystone_level", IntegerType(), nullable=True
                        ),
                        StructField(
                            "keystone_affixes",
                            ArrayType(IntegerType()),
                            nullable=True,
                        ),
                        StructField(
                            "keystone_time_ms", LongType(), nullable=True
                        ),
                        StructField("kill", StringType(), nullable=True),
                        StructField(
                            "start_time_ms", LongType(), nullable=True
                        ),
                        StructField("end_time_ms", LongType(), nullable=True),
                    ]
                )
            ),
            nullable=True,
        ),
        StructField("ingested_at", TimestampType(), nullable=False),
    ]
)

bronze_wcl_events_schema = StructType(
    [
        StructField("timestamp", LongType(), nullable=False),
        StructField("actor_id", IntegerType(), nullable=False),
        StructField("source_id", IntegerType(), nullable=True),
        StructField("ability_id", IntegerType(), nullable=True),
        StructField("ability_name", StringType(), nullable=True),
        StructField("damage_amount", LongType(), nullable=True),
        StructField("damage_type", StringType(), nullable=True),
        StructField("fight_id", IntegerType(), nullable=False),
        StructField("report_code", StringType(), nullable=False),
    ]
)

# ─── Silver Layer ─────────────────────────────────────────────────────────────

silver_dungeon_runs_schema = StructType(
    [
        StructField("run_id", StringType(), nullable=False),
        StructField("rio_run_id", LongType(), nullable=True),
        StructField("wcl_report_code", StringType(), nullable=True),
        StructField("wcl_fight_id", IntegerType(), nullable=True),
        StructField("dungeon_id", IntegerType(), nullable=False),
        StructField("key_level", IntegerType(), nullable=False),
        StructField(
            "affix_ids", ArrayType(IntegerType()), nullable=True
        ),
        StructField("clear_time_ms", LongType(), nullable=True),
        StructField("completed_at", TimestampType(), nullable=True),
        StructField("confidence", DoubleType(), nullable=True),
        StructField("match_method", StringType(), nullable=True),
        StructField(
            "roster",
            ArrayType(
                StructType(
                    [
                        StructField("name", StringType(), nullable=True),
                        StructField("realm", StringType(), nullable=True),
                        StructField("region", StringType(), nullable=True),
                        StructField("class", StringType(), nullable=True),
                        StructField("spec", StringType(), nullable=True),
                        StructField("role", StringType(), nullable=True),
                    ]
                )
            ),
            nullable=True,
        ),
        StructField("season", StringType(), nullable=False),
        StructField("matched_at", TimestampType(), nullable=True),
    ]
)

silver_player_performance_schema = StructType(
    [
        StructField("run_id", StringType(), nullable=False),
        StructField("player_name", StringType(), nullable=False),
        StructField("realm", StringType(), nullable=True),
        StructField("region", StringType(), nullable=True),
        StructField("class_name", StringType(), nullable=True),
        StructField("spec_name", StringType(), nullable=True),
        StructField("role", StringType(), nullable=True),
        StructField("total_damage_taken", LongType(), nullable=True),
        StructField("total_healing_received", LongType(), nullable=True),
        StructField("interrupts_cast", IntegerType(), nullable=True),
        StructField("interrupts_successful", IntegerType(), nullable=True),
        StructField("max_hp", LongType(), nullable=True),
        StructField("fight_duration_ms", LongType(), nullable=True),
        StructField("season", StringType(), nullable=False),
    ]
)

# ─── Gold KPI Schemas ─────────────────────────────────────────────────────────

gold_kpi_tank_death_clock_schema = StructType(
    [
        StructField("run_id", StringType(), nullable=False),
        StructField("dungeon_id", IntegerType(), nullable=True),
        StructField("key_level", IntegerType(), nullable=True),
        StructField("tank_name", StringType(), nullable=True),
        StructField("tank_class", StringType(), nullable=True),
        StructField("tank_spec", StringType(), nullable=True),
        StructField("dtps", DoubleType(), nullable=True),
        StructField("hps_on_tank", DoubleType(), nullable=True),
        StructField("ehp_estimate", LongType(), nullable=True),
        StructField("death_clock_seconds", DoubleType(), nullable=True),
        StructField("death_clock_category", StringType(), nullable=True),
        StructField("fight_duration_ms", LongType(), nullable=True),
        StructField(
            "affix_ids", ArrayType(IntegerType()), nullable=True
        ),
    ]
)

gold_kpi_healer_deficit_schema = StructType(
    [
        StructField("run_id", StringType(), nullable=False),
        StructField("healer_name", StringType(), nullable=True),
        StructField("healer_class", StringType(), nullable=True),
        StructField("healer_spec", StringType(), nullable=True),
        StructField("tank_dtps", DoubleType(), nullable=True),
        StructField("healer_hps_on_tank", DoubleType(), nullable=True),
        StructField("deficit_ratio", DoubleType(), nullable=True),
        StructField("deficit_category", StringType(), nullable=True),
        StructField(
            "affix_ids", ArrayType(IntegerType()), nullable=True
        ),
    ]
)

gold_kpi_interrupt_rate_schema = StructType(
    [
        StructField("run_id", StringType(), nullable=False),
        StructField("player_name", StringType(), nullable=True),
        StructField("player_class", StringType(), nullable=True),
        StructField("player_spec", StringType(), nullable=True),
        StructField("player_role", StringType(), nullable=True),
        StructField("total_interrupt_casts", IntegerType(), nullable=True),
        StructField("successful_interrupts", IntegerType(), nullable=True),
        StructField(
            "interrupt_success_rate", DoubleType(), nullable=True
        ),
        StructField(
            "dangerous_enemy_casts", IntegerType(), nullable=True
        ),
        StructField(
            "interrupt_coverage", DoubleType(), nullable=True
        ),
    ]
)

gold_kpi_composition_synergy_schema = StructType(
    [
        StructField("dungeon_id", IntegerType(), nullable=True),
        StructField("dungeon_name", StringType(), nullable=True),
        StructField("key_level", IntegerType(), nullable=True),
        StructField(
            "affix_ids", ArrayType(IntegerType()), nullable=True
        ),
        StructField("comp_signature", StringType(), nullable=True),
        StructField("avg_clear_time_ms", DoubleType(), nullable=True),
        StructField(
            "overall_avg_clear_time_ms", DoubleType(), nullable=True
        ),
        StructField("synergy_score", DoubleType(), nullable=True),
        StructField("sample_count", IntegerType(), nullable=True),
    ]
)

# ─── Gold Dimension Table Schemas ─────────────────────────────────────────────

dim_dungeon_schema = StructType(
    [
        StructField("dungeon_id", IntegerType(), nullable=False),
        StructField("dungeon_name", StringType(), nullable=True),
        StructField("slug", StringType(), nullable=True),
        StructField("keystone_timer_ms", LongType(), nullable=True),
        StructField("season", StringType(), nullable=True),
    ]
)

dim_player_schema = StructType(
    [
        StructField("player_name", StringType(), nullable=True),
        StructField("realm", StringType(), nullable=True),
        StructField("region", StringType(), nullable=True),
        StructField("class_id", IntegerType(), nullable=True),
        StructField("class_name", StringType(), nullable=True),
        StructField("spec_name", StringType(), nullable=True),
        StructField("role", StringType(), nullable=True),
    ]
)

dim_affix_schema = StructType(
    [
        StructField("affix_id", IntegerType(), nullable=False),
        StructField("affix_name", StringType(), nullable=True),
        StructField("affix_description", StringType(), nullable=True),
        StructField("affix_icon", StringType(), nullable=True),
        StructField("season", StringType(), nullable=True),
    ]
)

dim_spec_schema = StructType(
    [
        StructField("class_id", IntegerType(), nullable=True),
        StructField("class_name", StringType(), nullable=True),
        StructField("spec_name", StringType(), nullable=True),
        StructField("role", StringType(), nullable=True),
        StructField("is_healer", BooleanType(), nullable=True),
        StructField("is_tank", BooleanType(), nullable=True),
        StructField("is_dps", BooleanType(), nullable=True),
    ]
)