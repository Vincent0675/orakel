"""Tests for orakel.pipeline.gold — KPI computations and dimension tables (Tier 2).

Covers all Gold-layer outputs: death_clock, healer_deficit, interrupt_rate,
synergy, and dimension tables (dungeon, player, affix, spec).

Uses class-level DataFrameReader.parquet patching because spark.read
creates a new DataFrameReader on each call.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch

import pytest
from pyspark.sql import DataFrameReader, DataFrameWriter
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

import chispa
from orakel.models.schemas import (
    dim_affix_schema,
    dim_spec_schema,
    silver_dungeon_runs_schema,
    silver_player_performance_schema,
)
from orakel.pipeline.gold import GoldPipeline


# ─── Silver rio schema (post-clean_raiderio — flat roster strings) ─────────

_ROSTER_FLAT_STRUCT = ArrayType(StructType([
    StructField("name", StringType(), nullable=True),
    StructField("realm", StringType(), nullable=True),
    StructField("region", StringType(), nullable=True),
    StructField("class", StringType(), nullable=True),
    StructField("spec", StringType(), nullable=True),
    StructField("role", StringType(), nullable=True),
]))

SILVER_RAIDERIO_SCHEMA = StructType([
    StructField("source", StringType(), nullable=False),
    StructField("keystone_run_id", LongType(), nullable=False),
    StructField("dungeon_id", IntegerType(), nullable=False),
    StructField("challenge_mode_id", IntegerType(), nullable=True),
    StructField("dungeon_name", StringType(), nullable=True),
    StructField("mythic_level", IntegerType(), nullable=False),
    StructField("clear_time_ms", LongType(), nullable=True),
    StructField("keystone_time_ms", LongType(), nullable=True),
    StructField("completed_at", TimestampType(), nullable=True),
    StructField("weekly_modifiers", ArrayType(IntegerType()), nullable=True),
    StructField("roster", _ROSTER_FLAT_STRUCT, nullable=True),
    StructField("score", DoubleType(), nullable=True),
    StructField("rank", IntegerType(), nullable=True),
    StructField("season", StringType(), nullable=False),
    StructField("ingested_at", TimestampType(), nullable=False),
])


# ─── Test data factories ──────────────────────────────────────────────────

def _silver_dungeon_runs_row(run_id="test-run-1", **overrides):
    """Create a row matching silver_dungeon_runs_schema."""
    base = {
        "run_id": run_id,
        "rio_run_id": 100001,
        "wcl_report_code": "abc123",
        "wcl_fight_id": 1,
        "dungeon_id": 15093,  # Matches _TWW3_DUNGEONS entry
        "key_level": 10,
        "affix_ids": [9, 10],
        "clear_time_ms": 1800000,
        "completed_at": datetime(2025, 1, 15, 20, 30, tzinfo=timezone.utc),
        "confidence": 0.95,
        "match_method": "exact",
        "roster": [
            {"name": "Thrall", "realm": "azjol-nerub", "region": "us",
             "class": "Warrior", "spec": "Arms", "role": "tank"},
            {"name": "Jaina", "realm": "azjol-nerub", "region": "us",
             "class": "Mage", "spec": "Frost", "role": "dps"},
        ],
        "season": "season-tww-3",
        "matched_at": datetime(2025, 1, 15, 20, 35, tzinfo=timezone.utc),
    }
    base.update(overrides)
    return base


def _silver_player_performance_row(player_name="thrall", **overrides):
    """Create a row matching silver_player_performance_schema."""
    base = {
        "run_id": "test-run-1",
        "player_name": player_name,
        "realm": "azjol-nerub",
        "region": "us",
        "class_name": "Warrior",
        "spec_name": "Arms",
        "role": "tank",
        "total_damage_taken": 500000,
        "total_healing_received": 200000,
        "interrupts_count": 12,
        "max_hp": 600000,
        "fight_duration_ms": 300000,
        "season": "season-tww-3",
    }
    base.update(overrides)
    return base


def _silver_raiderio_runs_row(keystone_run_id=100001, **overrides):
    """Create a row matching the Silver raiderio_runs schema."""
    base = {
        "source": "raiderio",
        "keystone_run_id": keystone_run_id,
        "dungeon_id": 15093,  # Matches _TWW3_DUNGEONS entry for slug/timer join
        "challenge_mode_id": 401,
        "dungeon_name": "Ara-Kara, City of Echoes",
        "mythic_level": 10,
        "clear_time_ms": 1800000,
        "keystone_time_ms": 2100000,
        "completed_at": datetime(2025, 1, 15, 20, 30, tzinfo=timezone.utc),
        "weekly_modifiers": [9, 10],
        "roster": [
            {"name": "Thrall", "realm": "azjol-nerub", "region": "us",
             "class": "Warrior", "spec": "Arms", "role": "tank"},
        ],
        "score": 150.5,
        "rank": 1,
        "season": "season-tww-3",
        "ingested_at": datetime(2025, 1, 15, 21, 0, tzinfo=timezone.utc),
    }
    base.update(overrides)
    return base


def _patch_reads(path_to_df):
    """Patch DataFrameReader.parquet at the CLASS level.

    This is necessary because ``spark.read`` creates a new DataFrameReader
    on each call, so ``patch.object(spark_session.read, "parquet")`` won't
    work — the mock only affects the one instance, not the new one created
    inside pipeline methods.

    Returns the original method for use in the mock's fallback path.
    """
    from pyspark.errors import AnalysisException

    original_parquet = DataFrameReader.parquet
    from pyspark.errors import AnalysisException as _AE

    def mock_parquet(self, path, *args, **kwargs):
        for substring, df in path_to_df.items():
            if substring in path:
                return df
        # Unmatched paths: raise AnalysisException (simulates missing data)
        raise AnalysisException(f"No mock data for path: {path}")

    return mock_parquet, original_parquet


# ─── KPI 1: Death Clock ────────────────────────────────────────────────────

@pytest.mark.spark
class TestComputeKpiDeathClock:
    """Gold compute_kpi_death_clock: tank with WCL combat data."""

    def test_tank_with_data_produces_death_clock(self, spark_session):
        """Tank with damage/healing data produces non-null death_clock_seconds and category."""
        tank_row = _silver_player_performance_row(
            player_name="thrall",
            role="tank",
            class_name="Warrior",
            spec_name="Protection",
            total_damage_taken=500000,
            total_healing_received=200000,
            max_hp=600000,
            fight_duration_ms=300000,
        )
        pp_df = spark_session.createDataFrame(
            [tank_row], schema=silver_player_performance_schema
        )
        pp_df.cache()

        dr_rows = [_silver_dungeon_runs_row()]
        dr_df = spark_session.createDataFrame(dr_rows, schema=silver_dungeon_runs_schema)
        dr_df.cache()

        mock_fn, _ = _patch_reads({
            "silver/player_performance": pp_df,
            "silver/dungeon_runs": dr_df,
        })

        with patch.object(DataFrameReader, "parquet", mock_fn), \
             patch("orakel.pipeline.gold.settings") as mock_settings, \
             patch.object(DataFrameWriter, "parquet"):
            mock_settings.MINIO_BUCKET = "test-bucket"
            result = GoldPipeline.compute_kpi_death_clock(spark_session, "season-tww-3")

        assert result.count() == 1
        row = result.collect()[0]
        assert row["death_clock_seconds"] is not None
        assert row["death_clock_seconds"] > 0
        assert row["death_clock_category"] in ("safe", "moderate", "critical")

    def test_no_tank_falls_back_to_raiderio(self, spark_session):
        """When player_performance has no tanks, falls back to Raider.IO-only data."""
        pp_df = spark_session.createDataFrame([], schema=silver_player_performance_schema)
        rio_row = _silver_raiderio_runs_row()
        rio_df = spark_session.createDataFrame([rio_row], schema=SILVER_RAIDERIO_SCHEMA)
        rio_df.cache()

        mock_fn, original_parquet = _patch_reads({
            "silver/player_performance": pp_df,
            "silver/raiderio_runs": rio_df,
        })

        with patch.object(DataFrameReader, "parquet", mock_fn), \
             patch("orakel.pipeline.gold.settings") as mock_settings, \
             patch.object(DataFrameWriter, "parquet"):
            mock_settings.MINIO_BUCKET = "test-bucket"
            result = GoldPipeline.compute_kpi_death_clock(spark_session, "season-tww-3")

        # Fallback produces rows with null combat metrics
        assert result.count() >= 1
        row = result.collect()[0]
        assert row["death_clock_seconds"] is None
        assert row["death_clock_category"] is None


# ─── KPI 2: Healer Deficit ─────────────────────────────────────────────────

@pytest.mark.spark
class TestComputeKpiHealerDeficit:
    """Gold compute_kpi_healer_deficit: tank+healer joined."""

    def test_tank_healer_joined_produces_deficit(self, spark_session):
        """Tank + healer in same run produces deficit_ratio and deficit_category."""
        tank_row = _silver_player_performance_row(
            player_name="thrall", role="tank",
            class_name="Warrior", spec_name="Protection",
            total_damage_taken=100000,
            total_healing_received=50000,
            fight_duration_ms=300000,
        )
        healer_row = _silver_player_performance_row(
            player_name="jaina", role="healer",
            class_name="Priest", spec_name="Holy",
            total_damage_taken=0,
            total_healing_received=0,
            fight_duration_ms=300000,
        )
        pp_df = spark_session.createDataFrame(
            [tank_row, healer_row], schema=silver_player_performance_schema
        )
        pp_df.cache()

        dr_rows = [_silver_dungeon_runs_row()]
        dr_df = spark_session.createDataFrame(dr_rows, schema=silver_dungeon_runs_schema)
        dr_df.cache()

        mock_fn, _ = _patch_reads({
            "silver/player_performance": pp_df,
            "silver/dungeon_runs": dr_df,
        })

        with patch.object(DataFrameReader, "parquet", mock_fn), \
             patch("orakel.pipeline.gold.settings") as mock_settings, \
             patch.object(DataFrameWriter, "parquet"):
            mock_settings.MINIO_BUCKET = "test-bucket"
            result = GoldPipeline.compute_kpi_healer_deficit(spark_session, "season-tww-3")

        assert result.count() == 1
        row = result.collect()[0]
        assert row["deficit_ratio"] is not None
        assert row["deficit_category"] in ("comfortable", "moderate", "critical")

    def test_no_tank_falls_back_to_raiderio(self, spark_session):
        """When player_performance has no tanks, falls back to raiderio-only."""
        rio_row = _silver_raiderio_runs_row(
            roster=[
                {"name": "Thrall", "realm": "azjol-nerub", "region": "us",
                 "class": "Warrior", "spec": "Protection", "role": "tank"},
                {"name": "Liadrin", "realm": "azjol-nerub", "region": "us",
                 "class": "Paladin", "spec": "Holy", "role": "healer"},
            ],
        )
        rio_df = spark_session.createDataFrame([rio_row], schema=SILVER_RAIDERIO_SCHEMA)
        rio_df.cache()

        # Player perf fails → fallback to rio_runs
        from pyspark.errors import AnalysisException

        original_parquet = DataFrameReader.parquet

        def mock_parquet(self, path, *args, **kwargs):
            if "silver/player_performance" in path:
                raise AnalysisException("Path not found")
            if "silver/raiderio_runs" in path:
                return rio_df
            raise AnalysisException(f"No mock for: {path}")

        with patch.object(DataFrameReader, "parquet", mock_parquet), \
             patch("orakel.pipeline.gold.settings") as mock_settings, \
             patch.object(DataFrameWriter, "parquet"):
            mock_settings.MINIO_BUCKET = "test-bucket"
            result = GoldPipeline.compute_kpi_healer_deficit(spark_session, "season-tww-3")

        # Fallback should produce rows for healers from roster
        assert result.count() >= 1


# ─── KPI 3: Interrupt Rate ─────────────────────────────────────────────────

@pytest.mark.spark
class TestComputeKpiInterruptRate:
    """Gold compute_kpi_interrupt_rate: normalizes by duration."""

    def test_interrupts_per_minute(self, spark_session):
        """12 interrupts in 5 minutes → 2.4 interrupts per minute."""
        row = _silver_player_performance_row(
            player_name="thrall",
            role="tank",
            interrupts_count=12,
            fight_duration_ms=300000,  # 5 minutes
        )
        pp_df = spark_session.createDataFrame([row], schema=silver_player_performance_schema)
        pp_df.cache()

        dr_rows = [_silver_dungeon_runs_row()]
        dr_df = spark_session.createDataFrame(dr_rows, schema=silver_dungeon_runs_schema)
        dr_df.cache()

        mock_fn, _ = _patch_reads({
            "silver/player_performance": pp_df,
            "silver/dungeon_runs": dr_df,
        })

        with patch.object(DataFrameReader, "parquet", mock_fn), \
             patch("orakel.pipeline.gold.settings") as mock_settings, \
             patch.object(DataFrameWriter, "parquet"):
            mock_settings.MINIO_BUCKET = "test-bucket"
            result = GoldPipeline.compute_kpi_interrupt_rate(spark_session, "season-tww-3")

        assert result.count() == 1
        row = result.collect()[0]
        # 12 interrupts / 5 minutes = 2.4 per minute
        assert abs(row["interrupts_per_minute"] - 2.4) < 0.01
        assert row["interrupts_count"] == 12


# ─── KPI 4: Synergy Score ──────────────────────────────────────────────────

@pytest.mark.spark
class TestComputeKpiSynergy:
    """Gold compute_kpi_synergy: comp score calculation."""

    def test_synergy_score_computed(self, spark_session):
        """Comp with ≥2 samples produces a non-null synergy_score."""
        rows = [
            _silver_raiderio_runs_row(
                keystone_run_id=100001,
                roster=[
                    {"name": "Thrall", "realm": "azjol-nerub", "region": "us",
                     "class": "Warrior", "spec": "Arms", "role": "tank"},
                ],
                clear_time_ms=1800000,
            ),
            _silver_raiderio_runs_row(
                keystone_run_id=100002,
                roster=[
                    {"name": "Thrall", "realm": "azjol-nerub", "region": "us",
                     "class": "Warrior", "spec": "Arms", "role": "tank"},
                ],
                clear_time_ms=2000000,
            ),
            _silver_raiderio_runs_row(
                keystone_run_id=100003,
                roster=[
                    {"name": "Valeera", "realm": "azjol-nerub", "region": "us",
                     "class": "Rogue", "spec": "Assassination", "role": "dps"},
                ],
                clear_time_ms=2200000,
            ),
        ]
        rio_df = spark_session.createDataFrame(rows, schema=SILVER_RAIDERIO_SCHEMA)
        rio_df.cache()

        mock_fn, _ = _patch_reads({
            "silver/raiderio_runs": rio_df,
        })

        with patch.object(DataFrameReader, "parquet", mock_fn), \
             patch("orakel.pipeline.gold.settings") as mock_settings, \
             patch.object(DataFrameWriter, "parquet"):
            mock_settings.MINIO_BUCKET = "test-bucket"
            result = GoldPipeline.compute_kpi_synergy(spark_session, "season-tww-3")

        # Should produce at least one row with non-null synergy for the 2-sample comp
        assert result.count() >= 1
        non_null_scores = [row for row in result.collect()
                          if row["synergy_score"] is not None]
        assert len(non_null_scores) >= 1


# ─── Dimension tables ──────────────────────────────────────────────────────

@pytest.mark.spark
class TestBuildDimDungeon:
    """Gold build_dim_dungeon: dedup unique dungeons."""

    def test_dedup_unique_dungeons(self, spark_session):
        """Multiple runs for same dungeon → one row per unique (dungeon_id, dungeon_name)."""
        rows = [
            _silver_raiderio_runs_row(keystone_run_id=100001),
            _silver_raiderio_runs_row(keystone_run_id=100002),
        ]
        rio_df = spark_session.createDataFrame(rows, schema=SILVER_RAIDERIO_SCHEMA)
        rio_df.cache()

        mock_fn, _ = _patch_reads({
            "silver/raiderio_runs": rio_df,
        })

        with patch.object(DataFrameReader, "parquet", mock_fn), \
             patch("orakel.pipeline.gold.settings") as mock_settings, \
             patch.object(DataFrameWriter, "parquet"):
            mock_settings.MINIO_BUCKET = "test-bucket"
            result = GoldPipeline.build_dim_dungeon(spark_session, "season-tww-3")

        # Two runs for same dungeon → 1 unique dungeon row
        assert result.count() == 1
        row = result.collect()[0]
        # Default row factory uses dungeon_id=15093 (Ara-Kara)
        assert row["dungeon_id"] == 15093
        assert row["dungeon_name"] == "Ara-Kara, City of Echoes"
        # Hardcoded timer data should be populated
        assert row["slug"] is not None
        assert row["keystone_timer_ms"] is not None


@pytest.mark.spark
class TestBuildDimPlayer:
    """Gold build_dim_player: explodes roster into individual players."""

    def test_explodes_roster_to_players(self, spark_session):
        """One run with 1-player roster → 1 player row in dim_player."""
        rows = [_silver_raiderio_runs_row()]
        rio_df = spark_session.createDataFrame(rows, schema=SILVER_RAIDERIO_SCHEMA)
        rio_df.cache()

        mock_fn, _ = _patch_reads({
            "silver/raiderio_runs": rio_df,
        })

        with patch.object(DataFrameReader, "parquet", mock_fn), \
             patch("orakel.pipeline.gold.settings") as mock_settings, \
             patch.object(DataFrameWriter, "parquet"):
            mock_settings.MINIO_BUCKET = "test-bucket"
            result = GoldPipeline.build_dim_player(spark_session, "season-tww-3")

        # 1 run with 1 player in roster → 1 unique player
        assert result.count() == 1
        row = result.collect()[0]
        assert row["player_name"] == "Thrall"
        assert row["class_name"] == "Warrior"
        # class_id should be populated from hardcoded mapping
        assert row["class_id"] == 1  # Warrior → 1


@pytest.mark.spark
class TestBuildDimAffix:
    """Gold build_dim_affix: returns hardcoded TWW Season 3 affix data."""

    def test_returns_affix_rows(self, spark_session):
        """dim_affix has rows matching dim_affix_schema."""
        # build_dim_affix doesn't read from parquet — it uses hardcoded data
        with patch.object(DataFrameWriter, "parquet"):
            result = GoldPipeline.build_dim_affix(spark_session, "season-tww-3")

        # Should have rows for TWW Season 3 affixes
        assert result.count() > 0
        # Verify schema matches
        assert set(f.name for f in result.schema.fields) == set(f.name for f in dim_affix_schema.fields)


@pytest.mark.spark
class TestBuildDimSpec:
    """Gold build_dim_spec: returns spec-role mapping (39 specs)."""

    def test_returns_spec_mapping(self, spark_session):
        """dim_spec has 39 rows with correct schema fields."""
        # build_dim_spec doesn't read from parquet — it uses hardcoded data
        with patch.object(DataFrameWriter, "parquet"):
            result = GoldPipeline.build_dim_spec(spark_session)

        # Should have 39 WoW specs (matching _WOW_SPEC_ROLE_MAP)
        assert result.count() == 39
        # Verify schema matches
        assert set(f.name for f in result.schema.fields) == set(f.name for f in dim_spec_schema.fields)
        # Verify some known specs
        rows = result.collect()
        spec_names = [row["spec_name"] for row in rows]
        assert "Blood" in spec_names  # Death Knight tank
        assert "Holy" in spec_names  # Paladin/Priest healer