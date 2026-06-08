"""Tests for orakel.pipeline.silver — SparkSession + chispa pipeline tests (Tier 2).

Covers clean_raiderio (dedup, struct flattening, idempotent flatten)
and apply_fuzzy_join (match join, empty manifest fallback).

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
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

import chispa
from orakel.models.schemas import bronze_raiderio_schema
from orakel.pipeline.silver import SilverPipeline


# ─── Test data factories ──────────────────────────────────────────────────

_ROSTER_FLAT_STRUCT = ArrayType(StructType([
    StructField("name", StringType(), nullable=True),
    StructField("class", StringType(), nullable=True),
    StructField("spec", StringType(), nullable=True),
    StructField("role", StringType(), nullable=True),
    StructField("realm", StringType(), nullable=True),
    StructField("region", StringType(), nullable=True),
]))


def _bronze_row(**overrides):
    """Create a Bronze row dict with struct roster (matching bronze_raiderio_schema).

    Uses struct realm/region as the raw API returns. The clean_raiderio function
    will flatten these to strings.
    """
    roster = [
        {
            "name": "Thrall",
            "class": "Warrior",
            "spec": "Arms",
            "role": "tank",
            "realm": {"id": 1, "connectedRealmId": 11, "wowRealmId": 111, "wowConnectedRealmId": 1111, "name": "Azjol-Nerub", "slug": "azjol-nerub", "locale": "enUS"},
            "region": {"name": "US", "slug": "us", "short_name": "us"},
        }
    ]
    base = {
        "source": "raiderio",
        "keystone_run_id": 100001,
        "dungeon_id": 390,
        "challenge_mode_id": 401,
        "dungeon_name": "The Rookery",
        "mythic_level": 10,
        "clear_time_ms": 1800000,
        "keystone_time_ms": 2100000,
        "completed_at": datetime(2025, 1, 15, 20, 30, tzinfo=timezone.utc),
        "weekly_modifiers": [9, 10],
        "roster": roster,
        "score": 150.5,
        "rank": 1,
        "season": "season-tww-3",
        "ingested_at": datetime(2025, 1, 15, 21, 0, tzinfo=timezone.utc),
    }
    base.update(overrides)
    return base


def _create_bronze_df(spark, rows, schema=bronze_raiderio_schema):
    """Create a Bronze DataFrame from a list of row dicts."""
    return spark.createDataFrame(rows, schema=schema)


def _mock_parquet_read(path_to_df):
    """Create a mock for DataFrameReader.parquet that returns DataFrames by path.

    Patches at the class level so ALL DataFrameReader instances use the mock,
    including ones created by spark.read inside pipeline methods.
    """
    from pyspark.errors import AnalysisException

    original_parquet = DataFrameReader.parquet

    def mock_parquet(self, path, *args, **kwargs):
        for substring, df in path_to_df.items():
            if substring in path:
                return df
        # For unmatched paths, call original (may raise AnalysisException)
        return original_parquet(self, path, *args, **kwargs)

    return mock_parquet, original_parquet


# ─── Silver schema (flat roster — _after_ clean_raiderio flatten) ─────────

_SILVER_SCHEMA = StructType([
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


@pytest.mark.spark
class TestCleanRaiderioDedup:
    """Silver clean_raiderio: dedup by keystone_run_id, keeping latest ingested_at."""

    def test_dedup_keeps_latest_ingested(self, spark_session):
        """Two rows with same keystone_run_id → keep the one with latest ingested_at."""
        earlier = datetime(2025, 1, 10, 12, 0, tzinfo=timezone.utc)
        later = datetime(2025, 1, 11, 12, 0, tzinfo=timezone.utc)

        bronze_df = _create_bronze_df(spark_session, [
            _bronze_row(keystone_run_id=100001, ingested_at=earlier),
            _bronze_row(keystone_run_id=100001, ingested_at=later),
        ])
        bronze_df.cache()

        mock_fn, original = _mock_parquet_read({"bronze/raiderio": bronze_df})
        with patch.object(DataFrameReader, "parquet", mock_fn), \
             patch("orakel.pipeline.silver.settings") as mock_settings, \
             patch.object(DataFrameWriter, "parquet"):
            mock_settings.MINIO_BUCKET = "test-bucket"
            result = SilverPipeline.clean_raiderio(spark_session, "season-tww-3")

        # After dedup + season filter: 1 row
        assert result.count() == 1
        row = result.collect()[0]
        # PySpark collect() returns naive datetime in local JVM tz;
        # convert to UTC before comparing
        ingested_utc = row["ingested_at"].astimezone(timezone.utc).replace(tzinfo=None)
        assert ingested_utc == later.replace(tzinfo=None)

    def test_dedup_preserves_unique_runs(self, spark_session):
        """Different keystone_run_ids are kept as separate rows."""
        bronze_df = _create_bronze_df(spark_session, [
            _bronze_row(keystone_run_id=100001),
            _bronze_row(keystone_run_id=100002),
        ])
        bronze_df.cache()

        mock_fn, original = _mock_parquet_read({"bronze/raiderio": bronze_df})
        with patch.object(DataFrameReader, "parquet", mock_fn), \
             patch("orakel.pipeline.silver.settings") as mock_settings, \
             patch.object(DataFrameWriter, "parquet"):
            mock_settings.MINIO_BUCKET = "test-bucket"
            result = SilverPipeline.clean_raiderio(spark_session, "season-tww-3")

        assert result.count() == 2


@pytest.mark.spark
class TestCleanRaiderioFlatten:
    """Silver clean_raiderio: flatten nested realm/region structs to strings."""

    def test_struct_flatten_realm_region(self, spark_session):
        """roster[].realm struct → slug string, region struct → short_name string."""
        # Use the full Bronze schema with nested structs
        from orakel.models.schemas import realm_struct, region_struct

        roster_struct = ArrayType(StructType([
            StructField("name", StringType(), nullable=True),
            StructField("class", StringType(), nullable=True),
            StructField("spec", StringType(), nullable=True),
            StructField("role", StringType(), nullable=True),
            StructField("realm", realm_struct, nullable=True),
            StructField("region", region_struct, nullable=True),
        ]))

        bronze_schema_nested = StructType([
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
            StructField("roster", roster_struct, nullable=True),
            StructField("score", DoubleType(), nullable=True),
            StructField("rank", IntegerType(), nullable=True),
            StructField("season", StringType(), nullable=False),
            StructField("ingested_at", TimestampType(), nullable=False),
        ])

        row = _bronze_row(flat_roster=False)
        bronze_df = spark_session.createDataFrame([row], schema=bronze_schema_nested)
        bronze_df.cache()

        mock_fn, _ = _mock_parquet_read({"bronze/raiderio": bronze_df})
        with patch.object(DataFrameReader, "parquet", mock_fn), \
             patch("orakel.pipeline.silver.settings") as mock_settings, \
             patch.object(DataFrameWriter, "parquet"):
            mock_settings.MINIO_BUCKET = "test-bucket"
            result = SilverPipeline.clean_raiderio(spark_session, "season-tww-3")

        assert result.count() == 1
        roster = result.collect()[0]["roster"]
        # After flattening, realm should be the slug string, region the short_name
        assert roster[0]["realm"] == "azjol-nerub"
        assert roster[0]["region"] == "us"

    def test_idempotent_flatten_already_flat(self, spark_session):
        """If realm/region are already strings, flatten is a no-op.

        This scenario occurs when Silver data is re-processed: after the first
        clean_raiderio pass, realm/region are strings. On re-ingestion, the
        _flatten_roster method must be a no-op.
        """
        # Use the Silver schema (flat strings) directly
        row_flat = {
            "source": "raiderio",
            "keystone_run_id": 100001,
            "dungeon_id": 390,
            "challenge_mode_id": 401,
            "dungeon_name": "The Rookery",
            "mythic_level": 10,
            "clear_time_ms": 1800000,
            "keystone_time_ms": 2100000,
            "completed_at": datetime(2025, 1, 15, 20, 30, tzinfo=timezone.utc),
            "weekly_modifiers": [9, 10],
            "roster": [{"name": "Thrall", "realm": "azjol-nerub", "region": "us", "class": "Warrior", "spec": "Arms", "role": "tank"}],
            "score": 150.5,
            "rank": 1,
            "season": "season-tww-3",
            "ingested_at": datetime(2025, 1, 15, 21, 0, tzinfo=timezone.utc),
        }
        # Create a DataFrame with the Silver schema (flat strings for roster)
        silver_flat_df = spark_session.createDataFrame([row_flat], schema=_SILVER_SCHEMA)
        silver_flat_df.cache()

        # Re-read as if it's Bronze data (but it's already flattened)
        # This won't actually go through clean_raiderio for the idempotent test,
        # because clean_raiderio reads Bronze parquet. Instead, we test the
        # _flatten_roster helper directly on already-flat data.
        from pyspark.sql import functions as F
        result = silver_flat_df.withColumn(
            "roster", SilverPipeline._flatten_roster(silver_flat_df, F.col("roster"))
        )

        assert result.count() == 1
        roster = result.collect()[0]["roster"]
        # Flat strings should remain unchanged
        assert roster[0]["realm"] == "azjol-nerub"
        assert roster[0]["region"] == "us"


@pytest.mark.spark
class TestApplyFuzzyJoinManifest:
    """Silver apply_fuzzy_join: match manifest join and fallbacks."""

    def _create_silver_rio_df(self, spark_session):
        """Create a minimal Silver raiderio_runs DataFrame for fuzzy join tests."""
        rows = [{
            "source": "raiderio",
            "keystone_run_id": 100001,
            "dungeon_id": 390,
            "challenge_mode_id": 401,
            "dungeon_name": "The Rookery",
            "mythic_level": 10,
            "clear_time_ms": 1800000,
            "keystone_time_ms": 2100000,
            "completed_at": datetime(2025, 1, 15, 20, 30, tzinfo=timezone.utc),
            "weekly_modifiers": [9, 10],
            "roster": [{"name": "Thrall", "realm": "azjol-nerub", "region": "us", "class": "Warrior", "spec": "Arms", "role": "tank"}],
            "score": 150.5,
            "rank": 1,
            "season": "season-tww-3",
            "ingested_at": datetime(2025, 1, 15, 21, 0, tzinfo=timezone.utc),
        }]
        return spark_session.createDataFrame(rows, schema=_SILVER_SCHEMA)

    def test_empty_manifest_falls_back_to_rio_only(self, spark_session):
        """When match manifest is missing, match_method='rio_only' for all rows."""
        from pyspark.errors import AnalysisException

        rio_df = self._create_silver_rio_df(spark_session)
        rio_df.cache()

        def mock_parquet(self, path, *args, **kwargs):
            if "silver/raiderio_runs" in path:
                return rio_df
            if "silver/matches" in path:
                raise AnalysisException("Path does not exist")
            if "bronze/warcraftlogs" in path:
                raise AnalysisException("Path not found")
            if "silver/dungeon_runs" in path:
                raise AnalysisException("Path not found")
            return original_parquet(self, path, *args, **kwargs)

        original_parquet = DataFrameReader.parquet

        with patch.object(DataFrameReader, "parquet", mock_parquet), \
             patch("orakel.pipeline.silver.settings") as mock_settings, \
             patch.object(DataFrameWriter, "parquet"):
            mock_settings.MINIO_BUCKET = "test-bucket"
            dungeon_runs, player_perf = SilverPipeline.apply_fuzzy_join(spark_session, "season-tww-3")

        # dungeon_runs should have match_method = "rio_only"
        assert dungeon_runs.count() == 1
        row = dungeon_runs.collect()[0]
        assert row["match_method"] == "rio_only"
        # player_performance should be empty
        assert player_perf.count() == 0

    def test_empty_match_list_falls_back(self, spark_session):
        """When match manifest has 0 rows, falls back to rio_only."""
        rio_df = self._create_silver_rio_df(spark_session)
        rio_df.cache()

        match_schema = StructType([
            StructField("rio_run_id", LongType(), nullable=False),
            StructField("wcl_report_code", StringType(), nullable=True),
            StructField("wcl_fight_id", IntegerType(), nullable=True),
            StructField("confidence", DoubleType(), nullable=True),
            StructField("match_method", StringType(), nullable=True),
            StructField("matched_at", TimestampType(), nullable=True),
            StructField("season", StringType(), nullable=False),
        ])
        empty_matches = spark_session.createDataFrame([], schema=match_schema)

        original_parquet = DataFrameReader.parquet

        def mock_parquet(self, path, *args, **kwargs):
            from pyspark.errors import AnalysisException
            if "silver/raiderio_runs" in path:
                return rio_df
            if "silver/matches" in path:
                return empty_matches
            if "bronze/warcraftlogs" in path:
                raise AnalysisException("Path not found")
            if "silver/dungeon_runs" in path:
                raise AnalysisException("Path not found")
            return original_parquet(self, path, *args, **kwargs)

        with patch.object(DataFrameReader, "parquet", mock_parquet), \
             patch("orakel.pipeline.silver.settings") as mock_settings, \
             patch.object(DataFrameWriter, "parquet"):
            mock_settings.MINIO_BUCKET = "test-bucket"
            dungeon_runs, player_perf = SilverPipeline.apply_fuzzy_join(spark_session, "season-tww-3")

        assert dungeon_runs.count() == 1
        row = dungeon_runs.collect()[0]
        assert row["match_method"] == "rio_only"