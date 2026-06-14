"""Tests for cross-layer @asset_check wrappers (PR 2).

Covers the three new wrapper modules in ``orakel.pipeline.assets``:

  * ``checks_referential``  — 5 RI wrappers
  * ``checks_completeness`` — 5 completeness ratio wrappers
  * ``checks_schema``       — 7 schema-drift wrappers

Pattern follows ``test_checks.py`` (PR 1): class-level
``DataFrameReader.parquet`` patching because ``spark.read`` returns a
fresh ``DataFrameReader`` on every call.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from pyspark.sql import DataFrameReader
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from orakel.pipeline.assets import checks_completeness, checks_referential, checks_schema
from orakel.pipeline.assets.checks_referential import (
    ri_gold_dim_dungeon_to_silver_check,
    ri_gold_features_to_silver_check,
    ri_gold_kpi_death_clock_check,
    ri_gold_kpi_healer_deficit_check,
    ri_gold_kpi_interrupt_rate_check,
    ri_silver_dungeon_runs_to_bronze_check,
    ri_silver_raiderio_to_bronze_check,
)
from orakel.pipeline.assets.checks_completeness import (
    cr_bronze_to_silver_dungeon_runs_check,
    cr_bronze_to_silver_rio_check,
    cr_bronze_wcl_to_silver_player_perf_check,
    cr_silver_dungeon_runs_to_gold_features_check,
    cr_silver_rio_to_gold_kpis_composite_check,
)
from orakel.pipeline.assets.checks_schema import (
    sd_bronze_rio_check,
    sd_bronze_wcl_check,
    sd_gold_features_check,
    sd_gold_kpis_composite_check,
    sd_silver_dungeon_runs_check,
    sd_silver_player_performance_check,
    sd_silver_raiderio_check,
)


# ─── Helpers ─────────────────────────────────────────────────────────────


def _patch_parquet(path_to_df):
    """Patch DataFrameReader.parquet at the class level.

    Mirrors the helper used in test_checks.py — necessary because
    ``spark.read`` constructs a new reader per call.
    """
    from pyspark.errors import AnalysisException

    def mock_parquet(self, path, *args, **kwargs):
        for substring, df in path_to_df.items():
            if substring in path:
                return df
        raise AnalysisException(f"No mock data for path: {path}")

    return mock_parquet


# ─── Referential Integrity Wrappers ──────────────────────────────────────


@pytest.mark.spark
class TestReferentialIntegrityWrappers:
    """Each RI wrapper must call check_referential_integrity with the
    expected (upstream_path, downstream_path, join_key) tuple.

    The 7 KPIs case has 4 actual assets + 1 placeholder path for KPIs
    that don't exist as assets yet; we verify the 4 that exist.
    """

    def test_ri_silver_raiderio_to_bronze_passes(self, spark_session):
        upstream = spark_session.createDataFrame(
            [(1,), (2,), (3,)], ["keystone_run_id"]
        )
        downstream = spark_session.createDataFrame(
            [(1,), (2,)], ["keystone_run_id"]
        )
        mock_fn = _patch_parquet({
            "bronze/raiderio": upstream,
            "silver/raiderio_runs": downstream,
        })

        with patch.object(DataFrameReader, "parquet", mock_fn), \
             patch.object(checks_referential.settings, "CHECK_RI_ENABLED", True), \
             patch.object(checks_referential.settings, "MINIO_BUCKET", "test"):
            result = ri_silver_raiderio_to_bronze_check()

        assert result.passed is True
        assert result.metadata["orphan_count"] == 0
        assert result.metadata["join_key"] == "keystone_run_id"

    def test_ri_silver_dungeon_runs_to_bronze_passes(self, spark_session):
        upstream = spark_session.createDataFrame(
            [(1,), (2,)], ["keystone_run_id"]
        )
        downstream = spark_session.createDataFrame(
            [(1,)], ["keystone_run_id"]
        )
        mock_fn = _patch_parquet({
            "bronze/raiderio": upstream,
            "silver/dungeon_runs": downstream,
        })

        with patch.object(DataFrameReader, "parquet", mock_fn), \
             patch.object(checks_referential.settings, "CHECK_RI_ENABLED", True), \
             patch.object(checks_referential.settings, "MINIO_BUCKET", "test"):
            result = ri_silver_dungeon_runs_to_bronze_check()

        assert result.passed is True
        assert result.metadata["join_key"] == "keystone_run_id"

    def test_ri_gold_kpi_death_clock_passes(self, spark_session):
        upstream = spark_session.createDataFrame(
            [("r1",), ("r2",)], ["run_id"]
        )
        downstream = spark_session.createDataFrame(
            [("r1",), ("r2",)], ["run_id"]
        )
        mock_fn = _patch_parquet({
            "silver/dungeon_runs": upstream,
            "gold/kpi_tank_death_clock": downstream,
        })

        with patch.object(DataFrameReader, "parquet", mock_fn), \
             patch.object(checks_referential.settings, "CHECK_RI_ENABLED", True), \
             patch.object(checks_referential.settings, "MINIO_BUCKET", "test"):
            result = ri_gold_kpi_death_clock_check()

        assert result.passed is True
        assert result.metadata["join_key"] == "run_id"

    def test_ri_gold_kpi_healer_deficit_uses_player_performance(
        self, spark_session
    ):
        """healer_deficit upstream is silver/player_performance (not dungeon_runs)."""
        upstream = spark_session.createDataFrame(
            [("r1",)], ["run_id"]
        )
        downstream = spark_session.createDataFrame(
            [("r1",)], ["run_id"]
        )
        mock_fn = _patch_parquet({
            "silver/player_performance": upstream,
            "gold/kpi_healer_deficit": downstream,
        })

        with patch.object(DataFrameReader, "parquet", mock_fn), \
             patch.object(checks_referential.settings, "CHECK_RI_ENABLED", True), \
             patch.object(checks_referential.settings, "MINIO_BUCKET", "test"):
            result = ri_gold_kpi_healer_deficit_check()

        assert result.passed is True

    def test_ri_gold_kpi_interrupt_rate_uses_player_performance(
        self, spark_session
    ):
        """interrupt_rate upstream is silver/player_performance.

        Verifies the wrapper is wired correctly (this is the KPI the
        spec mentions but PR 1 didn't cover).
        """
        upstream = spark_session.createDataFrame(
            [("r1",)], ["run_id"]
        )
        downstream = spark_session.createDataFrame(
            [("r1",)], ["run_id"]
        )
        mock_fn = _patch_parquet({
            "silver/player_performance": upstream,
            "gold/kpi_interrupt_rate": downstream,
        })

        with patch.object(DataFrameReader, "parquet", mock_fn), \
             patch.object(checks_referential.settings, "CHECK_RI_ENABLED", True), \
             patch.object(checks_referential.settings, "MINIO_BUCKET", "test"):
            result = ri_gold_kpi_interrupt_rate_check()

        assert result.passed is True

    def test_ri_gold_features_uses_dungeon_runs(self, spark_session):
        upstream = spark_session.createDataFrame(
            [("r1",), ("r2",)], ["run_id"]
        )
        downstream = spark_session.createDataFrame(
            [("r1",)], ["run_id"]
        )
        mock_fn = _patch_parquet({
            "silver/dungeon_runs": upstream,
            "gold/features": downstream,
        })

        with patch.object(DataFrameReader, "parquet", mock_fn), \
             patch.object(checks_referential.settings, "CHECK_RI_ENABLED", True), \
             patch.object(checks_referential.settings, "MINIO_BUCKET", "test"):
            result = ri_gold_features_to_silver_check()

        assert result.passed is True

    def test_ri_gold_dim_dungeon_uses_dungeon_id(self, spark_session):
        """RI-5: dim_dungeon.dungeon_id must trace to silver_dungeon_runs.dungeon_id."""
        upstream = spark_session.createDataFrame(
            [(1,), (2,), (3,)], ["dungeon_id"]
        )
        downstream = spark_session.createDataFrame(
            [(1,), (2,)], ["dungeon_id"]
        )
        mock_fn = _patch_parquet({
            "silver/dungeon_runs": upstream,
            "gold/dim_dungeon": downstream,
        })

        with patch.object(DataFrameReader, "parquet", mock_fn), \
             patch.object(checks_referential.settings, "CHECK_RI_ENABLED", True), \
             patch.object(checks_referential.settings, "MINIO_BUCKET", "test"):
            result = ri_gold_dim_dungeon_to_silver_check()

        assert result.passed is True
        assert result.metadata["join_key"] == "dungeon_id"

    def test_ri_wrapper_disabled_returns_passed_true(self, spark_session):
        """CHECK_RI_ENABLED=False -> passed=True with disabled=True."""
        with patch.object(DataFrameReader, "parquet"), \
             patch.object(checks_referential.settings, "CHECK_RI_ENABLED", False), \
             patch.object(checks_referential.settings, "MINIO_BUCKET", "test"):
            result = ri_gold_kpi_death_clock_check()

        assert result.passed is True
        assert result.metadata["disabled"] is True

    def test_ri_orphan_detected_through_wrapper(self, spark_session):
        """Orphans at the wrapper level propagate as passed=False."""
        upstream = spark_session.createDataFrame(
            [("r1",)], ["run_id"]
        )
        downstream = spark_session.createDataFrame(
            [("r1",), ("orphan",)], ["run_id"]
        )
        mock_fn = _patch_parquet({
            "silver/dungeon_runs": upstream,
            "gold/kpi_tank_death_clock": downstream,
        })

        with patch.object(DataFrameReader, "parquet", mock_fn), \
             patch.object(checks_referential.settings, "CHECK_RI_ENABLED", True), \
             patch.object(checks_referential.settings, "MINIO_BUCKET", "test"):
            result = ri_gold_kpi_death_clock_check()

        assert result.passed is False
        assert result.metadata["orphan_count"] == 1


# ─── Completeness Ratio Wrappers ────────────────────────────────────────


@pytest.mark.spark
class TestCompletenessRatioWrappers:
    """Each CR wrapper must call check_completeness_ratio with the
    expected (upstream_path, downstream_path, min_ratio) tuple.
    """

    def test_cr_bronze_to_silver_rio_passes(self, spark_session):
        upstream = spark_session.createDataFrame(
            [("r%d" % i,) for i in range(100)], ["run_id"]
        )
        downstream = spark_session.createDataFrame(
            [("r%d" % i,) for i in range(80)], ["run_id"]
        )
        mock_fn = _patch_parquet({
            "bronze/raiderio": upstream,
            "silver/raiderio_runs": downstream,
        })

        with patch.object(DataFrameReader, "parquet", mock_fn), \
             patch.object(checks_completeness.settings, "CHECK_COMPLETENESS_ENABLED", True), \
             patch.object(checks_completeness.settings, "MINIO_BUCKET", "test"):
            result = cr_bronze_to_silver_rio_check()

        assert result.passed is True
        assert result.metadata["ratio"] == 0.8
        assert result.metadata["min_ratio"] == 0.5

    def test_cr_bronze_to_silver_dungeon_runs_uses_0_3_threshold(
        self, spark_session
    ):
        """CR-2 threshold is 0.3 (not 0.5) because fuzzy join is lossy."""
        upstream = spark_session.createDataFrame(
            [("r%d" % i,) for i in range(100)], ["run_id"]
        )
        # 40/100 = 0.4, above the 0.3 threshold
        downstream = spark_session.createDataFrame(
            [("r%d" % i,) for i in range(40)], ["run_id"]
        )
        mock_fn = _patch_parquet({
            "bronze/raiderio": upstream,
            "silver/dungeon_runs": downstream,
        })

        with patch.object(DataFrameReader, "parquet", mock_fn), \
             patch.object(checks_completeness.settings, "CHECK_COMPLETENESS_ENABLED", True), \
             patch.object(checks_completeness.settings, "MINIO_BUCKET", "test"):
            result = cr_bronze_to_silver_dungeon_runs_check()

        assert result.passed is True
        assert result.metadata["min_ratio"] == 0.3

    def test_cr_bronze_wcl_to_player_perf(self, spark_session):
        upstream = spark_session.createDataFrame(
            [("rc%d" % i,) for i in range(10)], ["report_code"]
        )
        downstream = spark_session.createDataFrame(
            [("r%d" % i,) for i in range(50)], ["run_id"]
        )
        mock_fn = _patch_parquet({
            "bronze/wcl": upstream,
            "silver/player_performance": downstream,
        })

        with patch.object(DataFrameReader, "parquet", mock_fn), \
             patch.object(checks_completeness.settings, "CHECK_COMPLETENESS_ENABLED", True), \
             patch.object(checks_completeness.settings, "MINIO_BUCKET", "test"):
            result = cr_bronze_wcl_to_silver_player_perf_check()

        # 50/10 = 5.0 ratio, well above 0.5
        assert result.passed is True
        assert result.metadata["ratio"] == 5.0

    def test_cr_silver_to_gold_features_uses_0_8_threshold(
        self, spark_session
    ):
        upstream = spark_session.createDataFrame(
            [("r%d" % i,) for i in range(100)], ["run_id"]
        )
        downstream = spark_session.createDataFrame(
            [("r%d" % i,) for i in range(85)], ["run_id"]
        )
        mock_fn = _patch_parquet({
            "silver/dungeon_runs": upstream,
            "gold/features": downstream,
        })

        with patch.object(DataFrameReader, "parquet", mock_fn), \
             patch.object(checks_completeness.settings, "CHECK_COMPLETENESS_ENABLED", True), \
             patch.object(checks_completeness.settings, "MINIO_BUCKET", "test"):
            result = cr_silver_dungeon_runs_to_gold_features_check()

        assert result.passed is True
        assert result.metadata["min_ratio"] == 0.8
        assert result.metadata["ratio"] == 0.85

    def test_cr_silver_rio_to_gold_kpis_composite(self, spark_session):
        """Composite: 4 KPIs all >= threshold, averaged."""
        upstream = spark_session.createDataFrame(
            [("r%d" % i,) for i in range(100)], ["run_id"]
        )
        # Each KPI: 80/100 = 0.8 -> composite 0.8, meets 0.8 threshold
        downstream = spark_session.createDataFrame(
            [("r%d" % i,) for i in range(80)], ["run_id"]
        )
        mock_fn = _patch_parquet({
            "silver/raiderio_runs": upstream,
            "gold/kpi_tank_death_clock": downstream,
            "gold/kpi_healer_deficit": downstream,
            "gold/kpi_interrupt_rate": downstream,
            "gold/kpi_composition_synergy": downstream,
        })

        with patch.object(DataFrameReader, "parquet", mock_fn), \
             patch.object(checks_completeness.settings, "CHECK_COMPLETENESS_ENABLED", True), \
             patch.object(checks_completeness.settings, "MINIO_BUCKET", "test"):
            result = cr_silver_rio_to_gold_kpis_composite_check()

        assert result.passed is True
        assert "kpis" in result.metadata
        assert "death_clock" in result.metadata["kpis"]

    def test_cr_composite_upstream_empty_passes(self, spark_session):
        """When upstream is empty, the composite passes with upstream_empty=True."""
        upstream = spark_session.createDataFrame([], "run_id: string")
        downstream = spark_session.createDataFrame(
            [("r1",)], ["run_id"]
        )
        mock_fn = _patch_parquet({
            "silver/raiderio_runs": upstream,
            "gold/kpi_tank_death_clock": downstream,
            "gold/kpi_healer_deficit": downstream,
            "gold/kpi_interrupt_rate": downstream,
            "gold/kpi_composition_synergy": downstream,
        })

        with patch.object(DataFrameReader, "parquet", mock_fn), \
             patch.object(checks_completeness.settings, "CHECK_COMPLETENESS_ENABLED", True), \
             patch.object(checks_completeness.settings, "MINIO_BUCKET", "test"):
            result = cr_silver_rio_to_gold_kpis_composite_check()

        assert result.passed is True
        assert result.metadata["upstream_empty"] is True

    def test_cr_wrapper_disabled_returns_passed_true(self, spark_session):
        with patch.object(DataFrameReader, "parquet"), \
             patch.object(checks_completeness.settings, "CHECK_COMPLETENESS_ENABLED", False), \
             patch.object(checks_completeness.settings, "MINIO_BUCKET", "test"):
            result = cr_bronze_to_silver_rio_check()

        assert result.passed is True
        assert result.metadata["disabled"] is True

    def test_cr_below_threshold_fails(self, spark_session):
        """30/100 = 0.3, below 0.5 threshold -> fail."""
        upstream = spark_session.createDataFrame(
            [("r%d" % i,) for i in range(100)], ["run_id"]
        )
        downstream = spark_session.createDataFrame(
            [("r%d" % i,) for i in range(30)], ["run_id"]
        )
        mock_fn = _patch_parquet({
            "bronze/raiderio": upstream,
            "silver/raiderio_runs": downstream,
        })

        with patch.object(DataFrameReader, "parquet", mock_fn), \
             patch.object(checks_completeness.settings, "CHECK_COMPLETENESS_ENABLED", True), \
             patch.object(checks_completeness.settings, "MINIO_BUCKET", "test"):
            result = cr_bronze_to_silver_rio_check()

        assert result.passed is False
        assert result.metadata["ratio"] == 0.3


# ─── Schema Drift Wrappers ──────────────────────────────────────────────


@pytest.mark.spark
class TestSchemaDriftWrappers:
    """Each SD wrapper must call check_schema_drift with the correct
    (path, expected_schema) tuple.  Superset mode by default.
    """

    def test_sd_bronze_rio_uses_bronze_raiderio_schema(self, spark_session):
        expected = StructType([
            StructField("source", StringType()),
            StructField("keystone_run_id", IntegerType()),
        ])
        actual = StructType([
            StructField("source", StringType()),
            StructField("keystone_run_id", IntegerType()),
            StructField("extra", StringType()),
        ])
        df = spark_session.createDataFrame([], schema=actual)
        mock_fn = _patch_parquet({"bronze/raiderio": df})

        with patch.object(DataFrameReader, "parquet", mock_fn), \
             patch.object(checks_schema.settings, "CHECK_SCHEMA_DRIFT_ENABLED", True), \
             patch.object(checks_schema.settings, "MINIO_BUCKET", "test"):
            result = sd_bronze_rio_check()

        assert result.passed is True  # superset tolerates extra cols

    def test_sd_bronze_wcl_uses_wcl_reports_schema(self, spark_session):
        actual = StructType([
            StructField("source", StringType()),
            StructField("report_code", StringType()),
        ])
        df = spark_session.createDataFrame([], schema=actual)
        mock_fn = _patch_parquet({"bronze/wcl": df})

        with patch.object(DataFrameReader, "parquet", mock_fn), \
             patch.object(checks_schema.settings, "CHECK_SCHEMA_DRIFT_ENABLED", True), \
             patch.object(checks_schema.settings, "MINIO_BUCKET", "test"):
            result = sd_bronze_wcl_check()

        assert result.passed is True

    def test_sd_silver_raiderio(self, spark_session):
        from orakel.models.schemas import silver_raiderio_schema

        df = spark_session.createDataFrame([], schema=silver_raiderio_schema)
        mock_fn = _patch_parquet({"silver/raiderio_runs": df})

        with patch.object(DataFrameReader, "parquet", mock_fn), \
             patch.object(checks_schema.settings, "CHECK_SCHEMA_DRIFT_ENABLED", True), \
             patch.object(checks_schema.settings, "MINIO_BUCKET", "test"):
            result = sd_silver_raiderio_check()

        assert result.passed is True

    def test_sd_silver_dungeon_runs(self, spark_session):
        from orakel.models.schemas import silver_dungeon_runs_schema

        df = spark_session.createDataFrame([], schema=silver_dungeon_runs_schema)
        mock_fn = _patch_parquet({"silver/dungeon_runs": df})

        with patch.object(DataFrameReader, "parquet", mock_fn), \
             patch.object(checks_schema.settings, "CHECK_SCHEMA_DRIFT_ENABLED", True), \
             patch.object(checks_schema.settings, "MINIO_BUCKET", "test"):
            result = sd_silver_dungeon_runs_check()

        assert result.passed is True

    def test_sd_silver_player_performance(self, spark_session):
        from orakel.models.schemas import silver_player_performance_schema

        df = spark_session.createDataFrame(
            [], schema=silver_player_performance_schema
        )
        mock_fn = _patch_parquet({"silver/player_performance": df})

        with patch.object(DataFrameReader, "parquet", mock_fn), \
             patch.object(checks_schema.settings, "CHECK_SCHEMA_DRIFT_ENABLED", True), \
             patch.object(checks_schema.settings, "MINIO_BUCKET", "test"):
            result = sd_silver_player_performance_check()

        assert result.passed is True

    def test_sd_gold_kpis_composite_all_match(self, spark_session):
        """All 4 gold KPIs match their expected schemas -> composite passes."""
        from orakel.models.schemas import (
            gold_kpi_composition_synergy_schema,
            gold_kpi_healer_deficit_schema,
            gold_kpi_interrupt_rate_schema,
            gold_kpi_tank_death_clock_schema,
        )

        mock_fn = _patch_parquet({
            "gold/kpi_tank_death_clock": spark_session.createDataFrame(
                [], schema=gold_kpi_tank_death_clock_schema
            ),
            "gold/kpi_healer_deficit": spark_session.createDataFrame(
                [], schema=gold_kpi_healer_deficit_schema
            ),
            "gold/kpi_interrupt_rate": spark_session.createDataFrame(
                [], schema=gold_kpi_interrupt_rate_schema
            ),
            "gold/kpi_composition_synergy": spark_session.createDataFrame(
                [], schema=gold_kpi_composition_synergy_schema
            ),
        })

        with patch.object(DataFrameReader, "parquet", mock_fn), \
             patch.object(checks_schema.settings, "CHECK_SCHEMA_DRIFT_ENABLED", True), \
             patch.object(checks_schema.settings, "MINIO_BUCKET", "test"):
            result = sd_gold_kpis_composite_check()

        assert result.passed is True
        assert "kpi_tank_death_clock" in result.metadata["kpis"]

    def test_sd_gold_kpis_composite_one_missing_col_fails(
        self, spark_session
    ):
        """One KPI missing a required col -> composite fails."""
        from orakel.models.schemas import (
            gold_kpi_composition_synergy_schema,
            gold_kpi_healer_deficit_schema,
            gold_kpi_interrupt_rate_schema,
            gold_kpi_tank_death_clock_schema,
        )

        # death_clock missing death_clock_seconds column
        death_clock_actual = StructType([
            StructField("run_id", StringType(), nullable=False),
            StructField("dungeon_id", IntegerType(), nullable=True),
            StructField("key_level", IntegerType(), nullable=True),
            # death_clock_seconds intentionally missing
        ])
        mock_fn = _patch_parquet({
            "gold/kpi_tank_death_clock": spark_session.createDataFrame(
                [], schema=death_clock_actual
            ),
            "gold/kpi_healer_deficit": spark_session.createDataFrame(
                [], schema=gold_kpi_healer_deficit_schema
            ),
            "gold/kpi_interrupt_rate": spark_session.createDataFrame(
                [], schema=gold_kpi_interrupt_rate_schema
            ),
            "gold/kpi_composition_synergy": spark_session.createDataFrame(
                [], schema=gold_kpi_composition_synergy_schema
            ),
        })

        with patch.object(DataFrameReader, "parquet", mock_fn), \
             patch.object(checks_schema.settings, "CHECK_SCHEMA_DRIFT_ENABLED", True), \
             patch.object(checks_schema.settings, "MINIO_BUCKET", "test"):
            result = sd_gold_kpis_composite_check()

        assert result.passed is False
        assert "kpi_tank_death_clock=False" in result.metadata["per_kpi_passed"]

    def test_sd_gold_features_uses_gold_features_schema(self, spark_session):
        from orakel.models.schemas import gold_features_schema

        df = spark_session.createDataFrame([], schema=gold_features_schema)
        mock_fn = _patch_parquet({"gold/features": df})

        with patch.object(DataFrameReader, "parquet", mock_fn), \
             patch.object(checks_schema.settings, "CHECK_SCHEMA_DRIFT_ENABLED", True), \
             patch.object(checks_schema.settings, "MINIO_BUCKET", "test"):
            result = sd_gold_features_check()

        assert result.passed is True

    def test_sd_wrapper_disabled_returns_passed_true(self, spark_session):
        with patch.object(DataFrameReader, "parquet"), \
             patch.object(checks_schema.settings, "CHECK_SCHEMA_DRIFT_ENABLED", False), \
             patch.object(checks_schema.settings, "MINIO_BUCKET", "test"):
            result = sd_bronze_rio_check()

        assert result.passed is True
        assert result.metadata["disabled"] is True

    def test_sd_path_not_found_passes(self, spark_session):
        """First-run scenario: path doesn't exist -> pass with warning."""
        from pyspark.errors import AnalysisException

        def raise_an(self, path, *args, **kwargs):
            raise AnalysisException(f"Path not found: {path}")

        with patch.object(DataFrameReader, "parquet", raise_an), \
             patch.object(checks_schema.settings, "CHECK_SCHEMA_DRIFT_ENABLED", True), \
             patch.object(checks_schema.settings, "MINIO_BUCKET", "test"):
            result = sd_bronze_rio_check()

        assert result.passed is True
        assert result.metadata["path_not_found"] is True


# ─── Schema additions (smoke tests for the new StructTypes) ────────────


class TestAddedSchemas:
    """Smoke tests for the new schemas added in PR 2.

    silver_raiderio_schema and gold_features_schema were added to
    anchor the new schema-drift wrappers.  These tests verify the
    schemas import and have a stable shape.
    """

    def test_silver_raiderio_schema_has_keystone_run_id(self):
        from orakel.models.schemas import silver_raiderio_schema

        names = {f.name for f in silver_raiderio_schema.fields}
        assert "keystone_run_id" in names
        assert "dungeon_id" in names
        assert "season" in names

    def test_gold_features_schema_has_run_id(self):
        from orakel.models.schemas import gold_features_schema

        names = {f.name for f in gold_features_schema.fields}
        assert "run_id" in names
        assert "clear_time_seconds" in names
        assert "season" in names

    def test_silver_raiderio_schema_dungeon_id_is_int(self):
        """Defensive: dungeon_id should remain IntegerType (per task 1.1)."""
        from orakel.models.schemas import silver_raiderio_schema

        dungeon_id = next(
            f for f in silver_raiderio_schema.fields if f.name == "dungeon_id"
        )
        assert "Integer" in str(dungeon_id.dataType)
