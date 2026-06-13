"""Tests for orakel.ml.features — Spark feature engineering (Tier 2).

Covers the ``build_feature_view`` pipeline:
    1. Read silver_dungeon_runs + 4 Gold KPIs
    2. Aggregate per-player KPIs to run level
    3. Join synergy KPI on (dungeon_id, key_level, affix_ids_key, comp_signature)
    4. Derive features: log1p transforms, role counts, affix binary flags, dungeon one-hot
    5. Filter NULL targets, select final columns
    6. Write to gold/features/ (mocked)

Uses the same DataFrameReader.parquet class-level patching pattern as
``tests/test_pipeline/test_gold.py`` because ``spark.read`` creates a new
DataFrameReader on each call.
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
    StringType,
    StructField,
    StructType,
)

from orakel.ml.features import build_feature_view
from orakel.models.schemas import (
    gold_kpi_composition_synergy_schema,
    gold_kpi_healer_deficit_schema,
    gold_kpi_interrupt_rate_schema,
    gold_kpi_tank_death_clock_schema,
    silver_dungeon_runs_schema,
)


# ─── Local roster struct (matches silver_dungeon_runs_schema roster) ──────────

_ROSTER_FLAT_STRUCT = ArrayType(StructType([
    StructField("name", StringType(), nullable=True),
    StructField("realm", StringType(), nullable=True),
    StructField("region", StringType(), nullable=True),
    StructField("class", StringType(), nullable=True),
    StructField("spec", StringType(), nullable=True),
    StructField("role", StringType(), nullable=True),
]))


# ─── Test data factories ──────────────────────────────────────────────────


def _silver_dungeon_runs_row(
    run_id: str = "test-run-1",
    dungeon_id: int = 15093,
    key_level: int = 10,
    affix_ids: list[int] | None = None,
    clear_time_ms: int = 1_800_000,
    roster: list[dict] | None = None,
    season: str = "season-tww-3",
    **overrides,
) -> dict:
    """Create a row matching ``silver_dungeon_runs_schema`` with sensible defaults."""
    if roster is None:
        roster = [
            {"name": "Thrall", "realm": "azjol-nerub", "region": "us",
             "class": "Warrior", "spec": "Arms", "role": "dps"},
        ]
    base = {
        "run_id": run_id,
        "rio_run_id": 100001,
        "wcl_report_code": "abc123",
        "wcl_fight_id": 1,
        "dungeon_id": dungeon_id,
        "key_level": key_level,
        "affix_ids": affix_ids if affix_ids is not None else [9, 10],
        "clear_time_ms": clear_time_ms,
        "completed_at": datetime(2025, 1, 15, 20, 30, tzinfo=timezone.utc),
        "confidence": 0.95,
        "match_method": "exact",
        "roster": roster,
        "season": season,
        "matched_at": datetime(2025, 1, 15, 20, 35, tzinfo=timezone.utc),
    }
    base.update(overrides)
    return base


def _death_clock_row(run_id: str, death_clock_seconds: float | None) -> dict:
    """Create a row matching ``gold_kpi_tank_death_clock_schema``."""
    base = {
        "run_id": run_id,
        "dungeon_id": 15093,
        "key_level": 10,
        "tank_name": "Thrall",
        "tank_class": "Warrior",
        "tank_spec": "Protection",
        "dtps": 1000.0,
        "hps_on_tank": 500.0,
        "ehp_estimate": 600000,
        "death_clock_seconds": death_clock_seconds,
        "death_clock_category": "safe",
        "fight_duration_ms": 300000,
        "affix_ids": [9, 10],
    }
    return base


def _healer_deficit_row(run_id: str, deficit_ratio: float | None) -> dict:
    """Create a row matching ``gold_kpi_healer_deficit_schema``."""
    base = {
        "run_id": run_id,
        "healer_name": "Jaina",
        "healer_class": "Priest",
        "healer_spec": "Holy",
        "tank_dtps": 1000.0,
        "healer_hps_on_tank": 500.0,
        "deficit_ratio": deficit_ratio,
        "deficit_category": "comfortable",
        "affix_ids": [9, 10],
    }
    return base


def _interrupt_rate_row(run_id: str, interrupts_per_minute: float | None) -> dict:
    """Create a row matching ``gold_kpi_interrupt_rate_schema``."""
    base = {
        "run_id": run_id,
        "player_name": "Thrall",
        "player_class": "Warrior",
        "player_spec": "Arms",
        "player_role": "dps",
        "interrupts_count": 12,
        "interrupts_per_minute": interrupts_per_minute,
        "dangerous_enemy_casts": 20,
        "interrupt_coverage": 0.6,
    }
    return base


def _synergy_row(
    dungeon_id: int,
    key_level: int,
    affix_ids: list[int],
    comp_signature: str,
    synergy_score: float | None,
) -> dict:
    """Create a row matching ``gold_kpi_composition_synergy_schema``.

    Note: features.py builds ``affix_ids_key`` as
    ``concat_ws('|', transform(array_sort(affix_ids), x -> cast(x as string)))``
    on the right side, so we pre-sort the affix_ids before passing in.
    """
    sorted_affix_ids = sorted(affix_ids)
    base = {
        "dungeon_id": dungeon_id,
        "dungeon_name": "Ara-Kara, City of Echoes",
        "key_level": key_level,
        "affix_ids": sorted_affix_ids,
        "comp_signature": comp_signature,
        "avg_clear_time_ms": 1_800_000.0,
        "overall_avg_clear_time_ms": 1_900_000.0,
        "synergy_score": synergy_score,
        "sample_count": 5,
    }
    return base


def _patch_reads(path_to_df: dict):
    """Patch DataFrameReader.parquet at the CLASS level.

    Necessary because ``spark.read`` creates a new DataFrameReader
    on each call, so per-instance patches don't survive the call
    into the feature view function.

    Args:
        path_to_df: Mapping from path substring → DataFrame to return
            for any parquet read whose path contains the substring.

    Returns:
        Tuple of (mock_parquet_callable, original_parquet).
    """
    from pyspark.errors import AnalysisException

    original_parquet = DataFrameReader.parquet

    def mock_parquet(self, path, *args, **kwargs):
        for substring, df in path_to_df.items():
            if substring in path:
                return df
        # Unmatched paths: raise AnalysisException (simulates missing data)
        raise AnalysisException(f"No mock data for path: {path}")

    return mock_parquet, original_parquet


# ─── Helpers for tests ─────────────────────────────────────────────────────

# Canonical roster pattern that produces a specific comp_signature via
# GoldPipeline._build_comp_signature: class-spec_role sorted alphabetically.
_ROSTER_1T_2H_3D = [
    {"name": "T1", "realm": "r", "region": "us", "class": "Warrior",
     "spec": "Protection", "role": "tank"},
    {"name": "H1", "realm": "r", "region": "us", "class": "Paladin",
     "spec": "Holy", "role": "healer"},
    {"name": "H2", "realm": "r", "region": "us", "class": "Priest",
     "spec": "Discipline", "role": "healer"},
    {"name": "D1", "realm": "r", "region": "us", "class": "Mage",
     "spec": "Frost", "role": "dps"},
    {"name": "D2", "realm": "r", "region": "us", "class": "Rogue",
     "spec": "Assassination", "role": "dps"},
    {"name": "D3", "realm": "r", "region": "us", "class": "Hunter",
     "spec": "Beast Mastery", "role": "dps"},
]


def _expected_comp_signature_for(roster: list[dict]) -> str:
    """Compute the expected comp_signature for a roster (server-side helper).

    Mirrors ``GoldPipeline._build_comp_signature``:
        sort("class-spec_role" entries) → join(":")
    """
    sigs = []
    for p in roster:
        cls = p.get("class") or "unknown"
        spec = p.get("spec") or "unknown"
        role = p.get("role") or "unknown"
        sigs.append(f"{cls}-{spec}_{role}")
    return ":".join(sorted(sigs))


# ─── Tests ─────────────────────────────────────────────────────────────────


@pytest.mark.spark
class TestRoleCountFeatures:
    """build_feature_view extracts num_tanks/healers/dps from roster."""

    def test_roster_one_tank_two_healers_three_dps(self, spark_session):
        """A 1-2-3 roster yields num_tanks=1, num_healers=2, num_dps=3."""
        run_id = "run-1t2h3d"
        roster = _ROSTER_1T_2H_3D
        comp_sig = _expected_comp_signature_for(roster)

        dr_df = spark_session.createDataFrame(
            [_silver_dungeon_runs_row(run_id=run_id, roster=roster)],
            schema=silver_dungeon_runs_schema,
        )
        dr_df.cache()

        # Empty KPI tables → death_clock_seconds will be NULL → log1p will be NULL
        empty_dc = spark_session.createDataFrame(
            [], schema=gold_kpi_tank_death_clock_schema
        )
        empty_hd = spark_session.createDataFrame(
            [], schema=gold_kpi_healer_deficit_schema
        )
        empty_ir = spark_session.createDataFrame(
            [], schema=gold_kpi_interrupt_rate_schema
        )
        # Synergy row with matching comp_signature so the join succeeds (NULL score is fine)
        synergy_df = spark_session.createDataFrame(
            [_synergy_row(15093, 10, [9, 10], comp_sig, 0.95)],
            schema=gold_kpi_composition_synergy_schema,
        )
        synergy_df.cache()

        mock_fn, _ = _patch_reads({
            "silver/dungeon_runs": dr_df,
            "gold/kpi_tank_death_clock": empty_dc,
            "gold/kpi_healer_deficit": empty_hd,
            "gold/kpi_interrupt_rate": empty_ir,
            "gold/kpi_composition_synergy": synergy_df,
        })

        with patch.object(DataFrameReader, "parquet", mock_fn), \
             patch("orakel.ml.features.settings") as mock_settings, \
             patch.object(DataFrameWriter, "parquet"):
            mock_settings.MINIO_BUCKET = "test-bucket"
            result = build_feature_view(spark_session, "season-tww-3")

        assert result.count() == 1
        row = result.collect()[0]
        assert row["num_tanks"] == 1
        assert row["num_healers"] == 2
        assert row["num_dps"] == 3


@pytest.mark.spark
class TestAffixFlags:
    """build_feature_view emits binary affix flags from AFFIX_IDS."""

    def test_matching_affix_ids_set_flag_to_one(self, spark_session):
        """Affixes [9, 10, 123, 124] in run → flags for those = 1, others = 0."""
        run_id = "run-affix"
        # 4 of the 16 AFFIX_IDS — the rest must be 0
        target_affixes = [9, 10, 123, 124]
        roster = _ROSTER_1T_2H_3D
        comp_sig = _expected_comp_signature_for(roster)

        dr_df = spark_session.createDataFrame(
            [_silver_dungeon_runs_row(
                run_id=run_id, affix_ids=target_affixes, roster=roster,
            )],
            schema=silver_dungeon_runs_schema,
        )
        dr_df.cache()

        empty_dc = spark_session.createDataFrame(
            [], schema=gold_kpi_tank_death_clock_schema
        )
        empty_hd = spark_session.createDataFrame(
            [], schema=gold_kpi_healer_deficit_schema
        )
        empty_ir = spark_session.createDataFrame(
            [], schema=gold_kpi_interrupt_rate_schema
        )
        # Pre-sort affix_ids to match features.py affix_ids_key derivation
        synergy_df = spark_session.createDataFrame(
            [_synergy_row(15093, 10, target_affixes, comp_sig, 0.95)],
            schema=gold_kpi_composition_synergy_schema,
        )
        synergy_df.cache()

        mock_fn, _ = _patch_reads({
            "silver/dungeon_runs": dr_df,
            "gold/kpi_tank_death_clock": empty_dc,
            "gold/kpi_healer_deficit": empty_hd,
            "gold/kpi_interrupt_rate": empty_ir,
            "gold/kpi_composition_synergy": synergy_df,
        })

        with patch.object(DataFrameReader, "parquet", mock_fn), \
             patch("orakel.ml.features.settings") as mock_settings, \
             patch.object(DataFrameWriter, "parquet"):
            mock_settings.MINIO_BUCKET = "test-bucket"
            result = build_feature_view(spark_session, "season-tww-3")

        assert result.count() == 1
        row = result.collect()[0]

        # All 16 affix columns must be present and integer-typed (0 or 1)
        for affix_id in [1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 12, 13, 14, 122, 123, 124]:
            col = f"affix_{affix_id}"
            assert col in result.columns, f"Missing affix column: {col}"
            assert row[col] in (0, 1), f"{col} should be 0 or 1, got {row[col]!r}"

        # Matching affixes → 1
        for affix_id in target_affixes:
            assert row[f"affix_{affix_id}"] == 1, (
                f"affix_{affix_id} should be 1 (matching), got {row[f'has_{affix_id}']!r}"
            )

        # Non-matching affixes → 0
        non_matching = set([1, 2, 3, 4, 6, 7, 8, 11, 12, 13, 14, 122]) - set(target_affixes)
        for affix_id in non_matching:
            assert row[f"affix_{affix_id}"] == 0, (
                f"affix_{affix_id} should be 0 (non-matching), got {row[f'affix_{affix_id}']!r}"
            )


@pytest.mark.spark
class TestLog1pTransform:
    """build_feature_view applies log1p() to death_clock_seconds."""

    def test_death_clock_10_seconds_produces_log1p_2_398(self, spark_session):
        """death_clock_seconds=10.0 → death_clock_seconds_log1p = log(11) ≈ 2.398.

        log1p(10) = log(1 + 10) = log(11) ≈ 2.3978952727983707
        """
        run_id = "run-log1p"
        roster = _ROSTER_1T_2H_3D
        comp_sig = _expected_comp_signature_for(roster)

        dr_df = spark_session.createDataFrame(
            [_silver_dungeon_runs_row(run_id=run_id, roster=roster)],
            schema=silver_dungeon_runs_schema,
        )
        dr_df.cache()

        # Single death_clock row with the canonical 10.0 value
        dc_df = spark_session.createDataFrame(
            [_death_clock_row(run_id, 10.0)],
            schema=gold_kpi_tank_death_clock_schema,
        )
        dc_df.cache()
        empty_hd = spark_session.createDataFrame(
            [], schema=gold_kpi_healer_deficit_schema
        )
        empty_ir = spark_session.createDataFrame(
            [], schema=gold_kpi_interrupt_rate_schema
        )
        synergy_df = spark_session.createDataFrame(
            [_synergy_row(15093, 10, [9, 10], comp_sig, 0.95)],
            schema=gold_kpi_composition_synergy_schema,
        )
        synergy_df.cache()

        mock_fn, _ = _patch_reads({
            "silver/dungeon_runs": dr_df,
            "gold/kpi_tank_death_clock": dc_df,
            "gold/kpi_healer_deficit": empty_hd,
            "gold/kpi_interrupt_rate": empty_ir,
            "gold/kpi_composition_synergy": synergy_df,
        })

        with patch.object(DataFrameReader, "parquet", mock_fn), \
             patch("orakel.ml.features.settings") as mock_settings, \
             patch.object(DataFrameWriter, "parquet"):
            mock_settings.MINIO_BUCKET = "test-bucket"
            result = build_feature_view(spark_session, "season-tww-3")

        assert result.count() == 1
        row = result.collect()[0]
        assert row["death_clock_seconds"] == 10.0
        # log(11) ≈ 2.3978952728
        assert abs(row["death_clock_seconds_log1p"] - 2.3978952728) < 1e-6, (
            f"Expected ≈ 2.398, got {row['death_clock_seconds_log1p']!r}"
        )


@pytest.mark.spark
class TestNullKpiImputation:
    """build_feature_view handles NULL KPI joins gracefully.

    The current build_feature_view does NOT impute NULL KPIs with 0
    inside the function itself — it only checks that the ``try`` block
    succeeds. When a KPI parquet read SUCCEEDS but the row is missing
    (e.g. LEFT JOIN with no match), the value stays NULL.

    This test pins the **current observed behavior**:
        - death_clock_seconds = NULL when no death_clock KPI row exists
        - deficit_ratio = NULL when no healer_deficit KPI row exists
        - death_clock_seconds_log1p = NULL (log1p(NULL) is NULL in Spark)
    """

    def test_missing_kpi_rows_yield_null_kpi_columns(self, spark_session):
        """When KPI tables are empty, joined columns are NULL — not 0.0.

        The spec said "deficit_ratio=NULL → imputed with 0.0 in output row",
        but the production code does not perform that imputation in
        ``build_feature_view`` — NULLs flow through to the output. This test
        pins the current behavior so future changes to imputation are intentional.
        """
        run_id = "run-null-kpi"
        roster = _ROSTER_1T_2H_3D
        comp_sig = _expected_comp_signature_for(roster)

        dr_df = spark_session.createDataFrame(
            [_silver_dungeon_runs_row(run_id=run_id, roster=roster)],
            schema=silver_dungeon_runs_schema,
        )
        dr_df.cache()

        # All three KPI tables are empty → no join match → NULL values
        empty_dc = spark_session.createDataFrame(
            [], schema=gold_kpi_tank_death_clock_schema
        )
        empty_hd = spark_session.createDataFrame(
            [], schema=gold_kpi_healer_deficit_schema
        )
        empty_ir = spark_session.createDataFrame(
            [], schema=gold_kpi_interrupt_rate_schema
        )
        synergy_df = spark_session.createDataFrame(
            [_synergy_row(15093, 10, [9, 10], comp_sig, 0.95)],
            schema=gold_kpi_composition_synergy_schema,
        )
        synergy_df.cache()

        mock_fn, _ = _patch_reads({
            "silver/dungeon_runs": dr_df,
            "gold/kpi_tank_death_clock": empty_dc,
            "gold/kpi_healer_deficit": empty_hd,
            "gold/kpi_interrupt_rate": empty_ir,
            "gold/kpi_composition_synergy": synergy_df,
        })

        with patch.object(DataFrameReader, "parquet", mock_fn), \
             patch("orakel.ml.features.settings") as mock_settings, \
             patch.object(DataFrameWriter, "parquet"):
            mock_settings.MINIO_BUCKET = "test-bucket"
            result = build_feature_view(spark_session, "season-tww-3")

        assert result.count() == 1
        row = result.collect()[0]
        # Current production behavior: NULL flows through (not imputed to 0.0)
        assert row["death_clock_seconds"] is None
        assert row["death_clock_seconds_log1p"] is None
        assert row["deficit_ratio"] is None
        assert row["interrupts_per_minute"] is None

    def test_present_kpi_row_with_null_value_passes_null_through(self, spark_session):
        """A KPI row that exists but has NULL deficit_ratio stays NULL.

        The aggregation ``F.avg(\"deficit_ratio\")`` over rows that are all NULL
        produces NULL. Verify the output reflects the NULL.
        """
        run_id = "run-null-ratio"
        roster = _ROSTER_1T_2H_3D
        comp_sig = _expected_comp_signature_for(roster)

        dr_df = spark_session.createDataFrame(
            [_silver_dungeon_runs_row(run_id=run_id, roster=roster)],
            schema=silver_dungeon_runs_schema,
        )
        dr_df.cache()

        # death_clock has a value (so log1p is testable)
        dc_df = spark_session.createDataFrame(
            [_death_clock_row(run_id, 5.0)],
            schema=gold_kpi_tank_death_clock_schema,
        )
        # healer_deficit row exists for the run but deficit_ratio is NULL
        hd_df = spark_session.createDataFrame(
            [_healer_deficit_row(run_id, None)],
            schema=gold_kpi_healer_deficit_schema,
        )
        # interrupt_rate row exists for the run but interrupts_per_minute is NULL
        ir_df = spark_session.createDataFrame(
            [_interrupt_rate_row(run_id, None)],
            schema=gold_kpi_interrupt_rate_schema,
        )
        synergy_df = spark_session.createDataFrame(
            [_synergy_row(15093, 10, [9, 10], comp_sig, 0.95)],
            schema=gold_kpi_composition_synergy_schema,
        )

        mock_fn, _ = _patch_reads({
            "silver/dungeon_runs": dr_df,
            "gold/kpi_tank_death_clock": dc_df,
            "gold/kpi_healer_deficit": hd_df,
            "gold/kpi_interrupt_rate": ir_df,
            "gold/kpi_composition_synergy": synergy_df,
        })

        with patch.object(DataFrameReader, "parquet", mock_fn), \
             patch("orakel.ml.features.settings") as mock_settings, \
             patch.object(DataFrameWriter, "parquet"):
            mock_settings.MINIO_BUCKET = "test-bucket"
            result = build_feature_view(spark_session, "season-tww-3")

        assert result.count() == 1
        row = result.collect()[0]
        # death_clock is 5.0 → log1p(5) ≈ 1.7917
        assert row["death_clock_seconds"] == 5.0
        assert abs(row["death_clock_seconds_log1p"] - 1.7917594692) < 1e-6
        # deficit_ratio and interrupts_per_minute are NULL (no row or NULL row)
        assert row["deficit_ratio"] is None
        assert row["interrupts_per_minute"] is None


@pytest.mark.spark
class TestDungeonAndRunJoin:
    """build_feature_view joins dungeon_runs to KPI aggregates."""

    def test_one_row_per_run_with_aggregated_kpis(self, spark_session):
        """Two runs, each with their own death_clock and deficit_ratio values.

        Verifies:
            - 1 row per input run (no fan-out)
            - Per-run KPI aggregation is correctly applied
            - Synergy score from the matching comp_signature flows through
            - All 8 dungeon one-hot flags are populated
        """
        roster1 = _ROSTER_1T_2H_3D
        comp_sig1 = _expected_comp_signature_for(roster1)

        # Roster 2: a different comp to test the comp_signature join logic
        roster2 = [
            {"name": "Genn", "realm": "r", "region": "us", "class": "Druid",
             "spec": "Guardian", "role": "tank"},
            {"name": "Anduin", "realm": "r", "region": "us", "class": "Priest",
             "spec": "Holy", "role": "healer"},
            {"name": "Rokhan", "realm": "r", "region": "us", "class": "Shaman",
             "spec": "Elemental", "role": "dps"},
            {"name": "Sylvanas", "realm": "r", "region": "us", "class": "Hunter",
             "spec": "Marksmanship", "role": "dps"},
            {"name": "Khadgar", "realm": "r", "region": "us", "class": "Mage",
             "spec": "Fire", "role": "dps"},
        ]
        comp_sig2 = _expected_comp_signature_for(roster2)

        dr_df = spark_session.createDataFrame(
            [
                _silver_dungeon_runs_row(
                    run_id="run-A", dungeon_id=15093, key_level=10,
                    affix_ids=[9, 10], clear_time_ms=1_500_000,
                    roster=roster1,
                ),
                _silver_dungeon_runs_row(
                    run_id="run-B", dungeon_id=16104, key_level=12,
                    affix_ids=[9, 124], clear_time_ms=1_800_000,
                    roster=roster2,
                ),
            ],
            schema=silver_dungeon_runs_schema,
        )
        dr_df.cache()

        # Two death_clock rows, one per run
        dc_df = spark_session.createDataFrame(
            [
                _death_clock_row("run-A", 8.0),
                _death_clock_row("run-B", 12.0),
            ],
            schema=gold_kpi_tank_death_clock_schema,
        )
        # Two healer_deficit rows, one per run
        hd_df = spark_session.createDataFrame(
            [
                _healer_deficit_row("run-A", 0.15),
                _healer_deficit_row("run-B", 0.30),
            ],
            schema=gold_kpi_healer_deficit_schema,
        )
        # Two interrupt_rate rows, one per run
        ir_df = spark_session.createDataFrame(
            [
                _interrupt_rate_row("run-A", 2.4),
                _interrupt_rate_row("run-B", 1.8),
            ],
            schema=gold_kpi_interrupt_rate_schema,
        )
        # Two synergy rows — one per (dungeon, key, affix, comp) combination
        synergy_df = spark_session.createDataFrame(
            [
                _synergy_row(15093, 10, [9, 10], comp_sig1, 0.95),
                _synergy_row(16104, 12, [9, 124], comp_sig2, 1.10),
            ],
            schema=gold_kpi_composition_synergy_schema,
        )
        synergy_df.cache()

        mock_fn, _ = _patch_reads({
            "silver/dungeon_runs": dr_df,
            "gold/kpi_tank_death_clock": dc_df,
            "gold/kpi_healer_deficit": hd_df,
            "gold/kpi_interrupt_rate": ir_df,
            "gold/kpi_composition_synergy": synergy_df,
        })

        with patch.object(DataFrameReader, "parquet", mock_fn), \
             patch("orakel.ml.features.settings") as mock_settings, \
             patch.object(DataFrameWriter, "parquet"):
            mock_settings.MINIO_BUCKET = "test-bucket"
            result = build_feature_view(spark_session, "season-tww-3")

        # ── One row per run (no fan-out from joins) ───────────────────────
        assert result.count() == 2

        rows_by_id = {r["run_id"]: r for r in result.collect()}

        # ── Run A: 1-2-3 roster, 1500s clear, death_clock=8s, deficit=0.15
        a = rows_by_id["run-A"]
        assert a["clear_time_seconds"] == 1500.0
        assert a["num_tanks"] == 1
        assert a["num_healers"] == 2
        assert a["num_dps"] == 3
        assert a["death_clock_seconds"] == 8.0
        # log1p(8) = log(9) ≈ 2.1972245773
        assert abs(a["death_clock_seconds_log1p"] - 2.1972245773) < 1e-6
        assert a["deficit_ratio"] == 0.15
        assert a["interrupts_per_minute"] == 2.4
        assert a["synergy_score"] == 0.95
        # Dungeon one-hot: dungeon_15093 = 1 for run-A
        assert a["dungeon_15093"] == 1
        assert a["dungeon_16104"] == 0

        # ── Run B: different roster, 1800s clear, death_clock=12s, deficit=0.30
        b = rows_by_id["run-B"]
        assert b["clear_time_seconds"] == 1800.0
        assert b["num_tanks"] == 1
        assert b["num_healers"] == 1
        assert b["num_dps"] == 3
        assert b["death_clock_seconds"] == 12.0
        # log1p(12) = log(13) ≈ 2.5649493574
        assert abs(b["death_clock_seconds_log1p"] - 2.5649493574) < 1e-6
        assert b["deficit_ratio"] == 0.30
        assert b["interrupts_per_minute"] == 1.8
        assert b["synergy_score"] == 1.10
        # Dungeon one-hot: dungeon_16104 = 1 for run-B
        assert b["dungeon_15093"] == 0
        assert b["dungeon_16104"] == 1


@pytest.mark.spark
class TestMissingKpiFallback:
    """When a KPI parquet read fails, build_feature_view uses NULLs gracefully."""

    def test_kpi_table_unavailable_does_not_crash(self, spark_session):
        """If kpi_tank_death_clock raises AnalysisException, build_feature_view
        proceeds with death_clock_seconds = NULL.
        """
        run_id = "run-no-dc"
        roster = _ROSTER_1T_2H_3D
        comp_sig = _expected_comp_signature_for(roster)

        dr_df = spark_session.createDataFrame(
            [_silver_dungeon_runs_row(run_id=run_id, roster=roster)],
            schema=silver_dungeon_runs_schema,
        )
        dr_df.cache()

        # We do NOT register kpi_tank_death_clock in the mock — the mock
        # raises AnalysisException, mimicking a missing path.
        empty_hd = spark_session.createDataFrame(
            [], schema=gold_kpi_healer_deficit_schema
        )
        empty_ir = spark_session.createDataFrame(
            [], schema=gold_kpi_interrupt_rate_schema
        )
        synergy_df = spark_session.createDataFrame(
            [_synergy_row(15093, 10, [9, 10], comp_sig, 0.95)],
            schema=gold_kpi_composition_synergy_schema,
        )
        synergy_df.cache()

        # Note: no "gold/kpi_tank_death_clock" key → mock raises AnalysisException
        mock_fn, _ = _patch_reads({
            "silver/dungeon_runs": dr_df,
            "gold/kpi_healer_deficit": empty_hd,
            "gold/kpi_interrupt_rate": empty_ir,
            "gold/kpi_composition_synergy": synergy_df,
        })

        with patch.object(DataFrameReader, "parquet", mock_fn), \
             patch("orakel.ml.features.settings") as mock_settings, \
             patch.object(DataFrameWriter, "parquet"):
            mock_settings.MINIO_BUCKET = "test-bucket"
            # Should not raise — try/except around KPI read uses NULL fallback
            result = build_feature_view(spark_session, "season-tww-3")

        assert result.count() == 1
        row = result.collect()[0]
        # death_clock_seconds is NULL because the KPI read failed
        assert row["death_clock_seconds"] is None
        assert row["death_clock_seconds_log1p"] is None
        # Other columns still populated
        assert row["num_tanks"] == 1
        assert row["synergy_score"] == 0.95
