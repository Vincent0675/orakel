"""Tests for orakel.pipeline.assets.checks — core reusable check functions
and per-asset @asset_check decorators (SDD: Verification-Dagster-Orchestation).

Covers:

  * check_referential_integrity  — orphan detection, sampling, missing path
  * check_completeness_ratio     — threshold pass/fail, zero-division guard
  * check_schema_drift           — exact / superset modes, missing path
  * 6 per-asset row-count checks — graceful (WCL, manifest) and strict (dims)

Pattern follows ``test_gold.py``: class-level ``DataFrameReader.parquet``
patching because ``spark.read`` returns a fresh ``DataFrameReader`` on
every call.  Pure unit tests (no Spark) live alongside the @spark
integration tests for symmetry.
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

from orakel.pipeline.assets.checks import (
    _schema_fingerprint,
    bronze_wcl_checks,
    check_completeness_ratio,
    check_referential_integrity,
    check_schema_drift,
    gold_dim_affix_checks,
    gold_dim_dungeon_checks,
    gold_dim_player_checks,
    gold_dim_spec_checks,
    match_manifest_checks,
)


# ─── Helpers ─────────────────────────────────────────────────────────────


def _patch_parquet(path_to_df):
    """Patch DataFrameReader.parquet at the class level.

    Mirrors the helper used in test_gold.py — necessary because
    ``spark.read`` constructs a new reader per call.
    """
    from pyspark.errors import AnalysisException

    def mock_parquet(self, path, *args, **kwargs):
        for substring, df in path_to_df.items():
            if substring in path:
                return df
        raise AnalysisException(f"No mock data for path: {path}")

    return mock_parquet


# ─── Pure unit tests (no Spark) ──────────────────────────────────────────


class TestSchemaFingerprint:
    """_schema_fingerprint is deterministic and order-independent."""

    def test_fingerprint_is_stable_across_calls(self):
        schema = StructType([
            StructField("a", IntegerType()),
            StructField("b", StringType()),
        ])
        assert _schema_fingerprint(schema) == _schema_fingerprint(schema)

    def test_fingerprint_ignores_field_order(self):
        a = StructType([
            StructField("a", IntegerType()),
            StructField("b", StringType()),
        ])
        b = StructType([
            StructField("b", StringType()),
            StructField("a", IntegerType()),
        ])
        assert _schema_fingerprint(a) == _schema_fingerprint(b)

    def test_fingerprint_detects_type_change(self):
        a = StructType([StructField("a", IntegerType())])
        b = StructType([StructField("a", StringType())])
        assert _schema_fingerprint(a) != _schema_fingerprint(b)

    def test_fingerprint_detects_added_column(self):
        a = StructType([StructField("a", IntegerType())])
        b = StructType([
            StructField("a", IntegerType()),
            StructField("c", StringType()),
        ])
        assert _schema_fingerprint(a) != _schema_fingerprint(b)


# ─── Tier 2: Spark integration tests for core functions ────────────────


@pytest.mark.spark
class TestCheckReferentialIntegrity:
    """check_referential_integrity: left-anti join semantics."""

    def test_no_orphans_passes(self, spark_session):
        """All downstream keys exist upstream -> passed=True, orphan_count=0."""
        upstream = spark_session.createDataFrame(
            [("r1",), ("r2",), ("r3",)], ["run_id"]
        )
        downstream = spark_session.createDataFrame(
            [("r1",), ("r2",)], ["run_id"]
        )
        mock_fn = _patch_parquet({
            "silver/upstream": upstream,
            "gold/downstream": downstream,
        })

        with patch.object(DataFrameReader, "parquet", mock_fn):
            result = check_referential_integrity(
                spark_session,
                "s3a://bucket/silver/upstream",
                "s3a://bucket/gold/downstream",
                "run_id",
            )

        assert result.passed is True
        assert result.metadata["orphan_count"] == 0
        assert result.metadata["total_downstream"] == 2
        assert result.metadata["join_key"] == "run_id"

    def test_orphans_fail(self, spark_session):
        """Downstream contains keys not in upstream -> passed=False, orphan_count>0."""
        upstream = spark_session.createDataFrame(
            [("r1",), ("r2",)], ["run_id"]
        )
        downstream = spark_session.createDataFrame(
            [("r1",), ("r2",), ("orphan1",), ("orphan2",)], ["run_id"]
        )
        mock_fn = _patch_parquet({
            "silver/upstream": upstream,
            "gold/downstream": downstream,
        })

        with patch.object(DataFrameReader, "parquet", mock_fn):
            result = check_referential_integrity(
                spark_session,
                "s3a://bucket/silver/upstream",
                "s3a://bucket/gold/downstream",
                "run_id",
            )

        assert result.passed is False
        assert result.metadata["orphan_count"] == 2
        assert result.metadata["total_downstream"] == 2  # distinct keys

    def test_empty_downstream_passes(self, spark_session):
        """Downstream has 0 rows -> passed=True, orphan_count=0."""
        upstream = spark_session.createDataFrame(
            [("r1",), ("r2",)], ["run_id"]
        )
        downstream = spark_session.createDataFrame([], "run_id: string")
        mock_fn = _patch_parquet({
            "silver/upstream": upstream,
            "gold/downstream": downstream,
        })

        with patch.object(DataFrameReader, "parquet", mock_fn):
            result = check_referential_integrity(
                spark_session,
                "s3a://bucket/silver/upstream",
                "s3a://bucket/gold/downstream",
                "run_id",
            )

        assert result.passed is True
        assert result.metadata["orphan_count"] == 0
        assert result.metadata["total_downstream"] == 0

    def test_missing_path_returns_warning(self, spark_session):
        """Missing path -> passed=False with error metadata."""
        from pyspark.errors import AnalysisException

        original = DataFrameReader.parquet

        def raise_an(self, path, *args, **kwargs):
            raise AnalysisException(f"Path not found: {path}")

        with patch.object(DataFrameReader, "parquet", raise_an):
            result = check_referential_integrity(
                spark_session,
                "s3a://bucket/missing",
                "s3a://bucket/missing2",
                "run_id",
            )

        assert result.passed is False
        assert "error" in result.metadata

    def test_season_filter_applied(self, spark_session):
        """season filter is applied when supplied and schema has a 'season' col."""
        upstream = spark_session.createDataFrame(
            [
                ("r1", "season-A"),
                ("r2", "season-B"),
            ],
            ["run_id", "season"],
        )
        downstream = spark_session.createDataFrame(
            [("r1", "season-A")],
            ["run_id", "season"],
        )
        mock_fn = _patch_parquet({
            "silver/upstream": upstream,
            "gold/downstream": downstream,
        })

        with patch.object(DataFrameReader, "parquet", mock_fn):
            # Filter to season-A: r1 exists -> no orphans
            result = check_referential_integrity(
                spark_session,
                "s3a://bucket/silver/upstream",
                "s3a://bucket/gold/downstream",
                "run_id",
                season="season-A",
            )

        assert result.passed is True
        assert result.metadata["orphan_count"] == 0


@pytest.mark.spark
class TestCheckCompletenessRatio:
    """check_completeness_ratio: ratio math and zero-division guard."""

    def test_ratio_above_threshold_passes(self, spark_session):
        """80/100 = 0.80 >= 0.5 -> pass."""
        upstream = spark_session.createDataFrame(
            [("r%d" % i,) for i in range(100)], ["run_id"]
        )
        downstream = spark_session.createDataFrame(
            [("r%d" % i,) for i in range(80)], ["run_id"]
        )
        mock_fn = _patch_parquet({
            "upstream": upstream,
            "downstream": downstream,
        })

        with patch.object(DataFrameReader, "parquet", mock_fn):
            result = check_completeness_ratio(
                spark_session,
                "s3a://upstream",
                "s3a://downstream",
                min_ratio=0.5,
            )

        assert result.passed is True
        assert result.metadata["ratio"] == 0.8
        assert result.metadata["upstream_count"] == 100
        assert result.metadata["downstream_count"] == 80

    def test_ratio_below_threshold_fails(self, spark_session):
        """30/100 = 0.30 < 0.5 -> fail."""
        upstream = spark_session.createDataFrame(
            [("r%d" % i,) for i in range(100)], ["run_id"]
        )
        downstream = spark_session.createDataFrame(
            [("r%d" % i,) for i in range(30)], ["run_id"]
        )
        mock_fn = _patch_parquet({
            "upstream": upstream,
            "downstream": downstream,
        })

        with patch.object(DataFrameReader, "parquet", mock_fn):
            result = check_completeness_ratio(
                spark_session,
                "s3a://upstream",
                "s3a://downstream",
                min_ratio=0.5,
            )

        assert result.passed is False
        assert result.metadata["ratio"] == 0.3
        assert result.metadata["min_ratio"] == 0.5

    def test_empty_upstream_passes_with_warning(self, spark_session):
        """upstream=0 -> pass with upstream_empty=True, ratio=0.0."""
        upstream = spark_session.createDataFrame([], "run_id: string")
        downstream = spark_session.createDataFrame(
            [("r1",), ("r2",)], ["run_id"]
        )
        mock_fn = _patch_parquet({
            "upstream": upstream,
            "downstream": downstream,
        })

        with patch.object(DataFrameReader, "parquet", mock_fn):
            result = check_completeness_ratio(
                spark_session,
                "s3a://upstream",
                "s3a://downstream",
                min_ratio=0.5,
            )

        # No-data is a warning, not a failure
        assert result.passed is True
        assert result.metadata["upstream_empty"] is True
        assert result.metadata["ratio"] == 0.0
        assert result.metadata["upstream_count"] == 0
        assert result.metadata["downstream_count"] == 2

    def test_default_min_ratio_is_0_5(self, spark_session):
        """Default min_ratio=0.5 (from settings) is applied when not passed."""
        upstream = spark_session.createDataFrame(
            [("r%d" % i,) for i in range(100)], ["run_id"]
        )
        downstream = spark_session.createDataFrame(
            [("r%d" % i,) for i in range(50)], ["run_id"]
        )
        mock_fn = _patch_parquet({
            "upstream": upstream,
            "downstream": downstream,
        })

        with patch.object(DataFrameReader, "parquet", mock_fn):
            result = check_completeness_ratio(
                spark_session,
                "s3a://upstream",
                "s3a://downstream",
            )

        # 50/100 = 0.5 meets default 0.5
        assert result.passed is True
        assert result.metadata["min_ratio"] == 0.5


@pytest.mark.spark
class TestCheckSchemaDrift:
    """check_schema_drift: exact vs superset modes."""

    EXPECTED = StructType([
        StructField("run_id", StringType()),
        StructField("dungeon_id", IntegerType()),
    ])

    def test_exact_match_passes(self, spark_session):
        """Actual schema equals expected -> pass, drift_detected=False."""
        actual = StructType([
            StructField("run_id", StringType()),
            StructField("dungeon_id", IntegerType()),
        ])
        df = spark_session.createDataFrame([], schema=actual)
        mock_fn = _patch_parquet({"path": df})

        with patch.object(DataFrameReader, "parquet", mock_fn):
            result = check_schema_drift(
                spark_session, "s3a://path", self.EXPECTED, mode="exact"
            )

        assert result.passed is True
        assert result.metadata["missing_columns"] == ""
        assert result.metadata["type_mismatches"] == ""

    def test_exact_mode_fails_on_extra_column(self, spark_session):
        """Extra column in actual -> fail in exact mode."""
        actual = StructType([
            StructField("run_id", StringType()),
            StructField("dungeon_id", IntegerType()),
            StructField("extra", StringType()),
        ])
        df = spark_session.createDataFrame([], schema=actual)
        mock_fn = _patch_parquet({"path": df})

        with patch.object(DataFrameReader, "parquet", mock_fn):
            result = check_schema_drift(
                spark_session, "s3a://path", self.EXPECTED, mode="exact"
            )

        assert result.passed is False
        assert "extra" in result.metadata["extra_columns"]

    def test_superset_mode_tolerates_extra_column(self, spark_session):
        """Extra column in actual -> pass in superset mode."""
        actual = StructType([
            StructField("run_id", StringType()),
            StructField("dungeon_id", IntegerType()),
            StructField("extra", StringType()),
        ])
        df = spark_session.createDataFrame([], schema=actual)
        mock_fn = _patch_parquet({"path": df})

        with patch.object(DataFrameReader, "parquet", mock_fn):
            result = check_schema_drift(
                spark_session, "s3a://path", self.EXPECTED, mode="superset"
            )

        assert result.passed is True
        # extra cols ignored in superset mode but still listed for traceability
        assert "extra" in result.metadata["extra_columns"]

    def test_missing_column_fails(self, spark_session):
        """Missing expected column -> fail in any mode."""
        actual = StructType([StructField("run_id", StringType())])
        df = spark_session.createDataFrame([], schema=actual)
        mock_fn = _patch_parquet({"path": df})

        with patch.object(DataFrameReader, "parquet", mock_fn):
            result = check_schema_drift(
                spark_session, "s3a://path", self.EXPECTED, mode="superset"
            )

        assert result.passed is False
        assert "dungeon_id" in result.metadata["missing_columns"]

    def test_type_mismatch_fails(self, spark_session):
        """Column type changed -> fail."""
        actual = StructType([
            StructField("run_id", StringType()),
            StructField("dungeon_id", StringType()),  # was Integer
        ])
        df = spark_session.createDataFrame([], schema=actual)
        mock_fn = _patch_parquet({"path": df})

        with patch.object(DataFrameReader, "parquet", mock_fn):
            result = check_schema_drift(
                spark_session, "s3a://path", self.EXPECTED, mode="superset"
            )

        assert result.passed is False
        assert "dungeon_id" in result.metadata["type_mismatches"]

    def test_path_not_found_passes(self, spark_session):
        """Missing Parquet path -> pass with path_not_found=True."""
        from pyspark.errors import AnalysisException

        def raise_an(self, path, *args, **kwargs):
            raise AnalysisException(f"Path not found: {path}")

        with patch.object(DataFrameReader, "parquet", raise_an):
            result = check_schema_drift(
                spark_session, "s3a://missing", self.EXPECTED
            )

        assert result.passed is True
        assert result.metadata["path_not_found"] is True

    def test_invalid_mode_fails(self, spark_session):
        """Unknown mode -> fail with error metadata."""
        with patch.object(DataFrameReader, "parquet"):
            result = check_schema_drift(
                spark_session, "s3a://path", self.EXPECTED, mode="banana"
            )

        assert result.passed is False
        assert "invalid mode" in result.metadata["error"]

    def test_fingerprints_in_metadata(self, spark_session):
        """Both actual and expected fingerprints are surfaced."""
        df = spark_session.createDataFrame([], schema=self.EXPECTED)
        mock_fn = _patch_parquet({"path": df})

        with patch.object(DataFrameReader, "parquet", mock_fn):
            result = check_schema_drift(
                spark_session, "s3a://path", self.EXPECTED
            )

        assert "actual_schema_fingerprint" in result.metadata
        assert "expected_schema_fingerprint" in result.metadata
        # Same schema -> same fingerprint
        assert (
            result.metadata["actual_schema_fingerprint"]
            == result.metadata["expected_schema_fingerprint"]
        )


# ─── Per-asset @asset_check tests ───────────────────────────────────────


@pytest.mark.spark
class TestPerAssetChecks:
    """Each per-asset check returns an AssetCheckResult with the expected
    metadata shape.  All checks are warning-only; an empty dim triggers
    a passed=False with a descriptive error, but a graceful check (WCL,
    manifest) passes even with 0 rows.
    """

    def _row_count_metadata(self, result, expected_count):
        assert result.metadata["row_count"] == expected_count

    def test_bronze_wcl_zero_rows_passes(self, spark_session):
        """Graceful: 0 rows is OK."""
        df = spark_session.createDataFrame([], "report_code: string")
        mock_fn = _patch_parquet({"bronze/wcl": df})

        with patch.object(DataFrameReader, "parquet", mock_fn), \
             patch("orakel.pipeline.assets.checks.settings") as ms:
            ms.MINIO_BUCKET = "test"
            result = bronze_wcl_checks()

        assert result.passed is True
        self._row_count_metadata(result, 0)
        assert result.metadata["graceful"] is True

    def test_bronze_wcl_with_data_passes(self, spark_session):
        df = spark_session.createDataFrame(
            [("rc1",), ("rc2",)], ["report_code"]
        )
        mock_fn = _patch_parquet({"bronze/wcl": df})

        with patch.object(DataFrameReader, "parquet", mock_fn), \
             patch("orakel.pipeline.assets.checks.settings") as ms:
            ms.MINIO_BUCKET = "test"
            result = bronze_wcl_checks()

        assert result.passed is True
        self._row_count_metadata(result, 2)

    def test_bronze_wcl_missing_path_fails(self, spark_session):
        from pyspark.errors import AnalysisException

        def raise_an(self, path, *args, **kwargs):
            raise AnalysisException(f"Missing: {path}")

        with patch.object(DataFrameReader, "parquet", raise_an), \
             patch("orakel.pipeline.assets.checks.settings") as ms:
            ms.MINIO_BUCKET = "test"
            result = bronze_wcl_checks()

        assert result.passed is False
        assert "not found" in result.metadata["error"]

    def test_match_manifest_zero_rows_passes(self, spark_session):
        df = spark_session.createDataFrame([], "run_id: string")
        mock_fn = _patch_parquet({"match_manifest": df})

        with patch.object(DataFrameReader, "parquet", mock_fn), \
             patch("orakel.pipeline.assets.checks.settings") as ms:
            ms.MINIO_BUCKET = "test"
            result = match_manifest_checks()

        assert result.passed is True
        assert result.metadata["graceful"] is True

    def test_match_manifest_missing_path_fails(self, spark_session):
        from pyspark.errors import AnalysisException

        def raise_an(self, path, *args, **kwargs):
            raise AnalysisException(f"Missing: {path}")

        with patch.object(DataFrameReader, "parquet", raise_an), \
             patch("orakel.pipeline.assets.checks.settings") as ms:
            ms.MINIO_BUCKET = "test"
            result = match_manifest_checks()

        assert result.passed is False
        assert "not found" in result.metadata["error"]

    def test_gold_dim_dungeon_empty_fails(self, spark_session):
        """Strict: 0 rows -> fail with descriptive error."""
        df = spark_session.createDataFrame([], "dungeon_id: int")
        mock_fn = _patch_parquet({"gold/dim_dungeon": df})

        with patch.object(DataFrameReader, "parquet", mock_fn), \
             patch("orakel.pipeline.assets.checks.settings") as ms:
            ms.MINIO_BUCKET = "test"
            result = gold_dim_dungeon_checks()

        assert result.passed is False
        self._row_count_metadata(result, 0)
        assert "Empty dim_dungeon" in result.metadata["error"]

    def test_gold_dim_dungeon_with_data_passes(self, spark_session):
        df = spark_session.createDataFrame(
            [(1,), (2,), (3,)], ["dungeon_id"]
        )
        mock_fn = _patch_parquet({"gold/dim_dungeon": df})

        with patch.object(DataFrameReader, "parquet", mock_fn), \
             patch("orakel.pipeline.assets.checks.settings") as ms:
            ms.MINIO_BUCKET = "test"
            result = gold_dim_dungeon_checks()

        assert result.passed is True
        self._row_count_metadata(result, 3)
        assert result.metadata["error"] == ""

    def test_gold_dim_player_empty_fails(self, spark_session):
        df = spark_session.createDataFrame([], "player_name: string")
        mock_fn = _patch_parquet({"gold/dim_player": df})

        with patch.object(DataFrameReader, "parquet", mock_fn), \
             patch("orakel.pipeline.assets.checks.settings") as ms:
            ms.MINIO_BUCKET = "test"
            result = gold_dim_player_checks()

        assert result.passed is False
        assert "Empty dim_player" in result.metadata["error"]

    def test_gold_dim_player_with_data_passes(self, spark_session):
        df = spark_session.createDataFrame(
            [("Thrall",), ("Jaina",)], ["player_name"]
        )
        mock_fn = _patch_parquet({"gold/dim_player": df})

        with patch.object(DataFrameReader, "parquet", mock_fn), \
             patch("orakel.pipeline.assets.checks.settings") as ms:
            ms.MINIO_BUCKET = "test"
            result = gold_dim_player_checks()

        assert result.passed is True
        self._row_count_metadata(result, 2)

    def test_gold_dim_affix_empty_fails(self, spark_session):
        df = spark_session.createDataFrame([], "affix_id: int")
        mock_fn = _patch_parquet({"gold/dim_affix": df})

        with patch.object(DataFrameReader, "parquet", mock_fn), \
             patch("orakel.pipeline.assets.checks.settings") as ms:
            ms.MINIO_BUCKET = "test"
            result = gold_dim_affix_checks()

        assert result.passed is False
        assert "Empty dim_affix" in result.metadata["error"]

    def test_gold_dim_affix_with_data_passes(self, spark_session):
        df = spark_session.createDataFrame(
            [(1,), (2,), (3,), (4,)], ["affix_id"]
        )
        mock_fn = _patch_parquet({"gold/dim_affix": df})

        with patch.object(DataFrameReader, "parquet", mock_fn), \
             patch("orakel.pipeline.assets.checks.settings") as ms:
            ms.MINIO_BUCKET = "test"
            result = gold_dim_affix_checks()

        assert result.passed is True
        self._row_count_metadata(result, 4)

    def test_gold_dim_spec_empty_fails(self, spark_session):
        df = spark_session.createDataFrame([], "spec_name: string")
        mock_fn = _patch_parquet({"gold/dim_spec": df})

        with patch.object(DataFrameReader, "parquet", mock_fn), \
             patch("orakel.pipeline.assets.checks.settings") as ms:
            ms.MINIO_BUCKET = "test"
            result = gold_dim_spec_checks()

        assert result.passed is False
        assert "Empty dim_spec" in result.metadata["error"]

    def test_gold_dim_spec_with_data_passes(self, spark_session):
        df = spark_session.createDataFrame(
            [("Arms",), ("Frost",), ("Holy",)], ["spec_name"]
        )
        mock_fn = _patch_parquet({"gold/dim_spec": df})

        with patch.object(DataFrameReader, "parquet", mock_fn), \
             patch("orakel.pipeline.assets.checks.settings") as ms:
            ms.MINIO_BUCKET = "test"
            result = gold_dim_spec_checks()

        assert result.passed is True
        self._row_count_metadata(result, 3)
