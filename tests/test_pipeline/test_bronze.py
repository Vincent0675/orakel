"""Tests for orakel.pipeline.bronze — dict transform (Tier 1) and Spark ingest (Tier 2).

Tier 1: _run_to_row pure function tests (no Spark).
Tier 2: ingest_raiderio_runs Spark integration tests (requires SparkSession).
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import DataFrameReader, DataFrameWriter

from orakel.models.schemas import bronze_raiderio_schema
from orakel.pipeline.bronze import _run_to_row, ingest_raiderio_runs


def _make_run(**overrides):
    """Factory for a minimal valid Raider.IO run dict.

    Accepts keyword overrides for any field. Defaults provide a complete
    run with all fields populated.
    """
    base = {
        "keystone_run_id": 99999,
        "dungeon": {"id": 390, "name": "The Rookery", "map_challenge_mode_id": 401},
        "mythic_level": 10,
        "clear_time_ms": 1800000,
        "keystone_time_ms": 2100000,
        "completed_at": "2025-01-15T20:30:00Z",
        "weekly_modifiers": [{"id": 9}, 10],
        "score": 150.5,
        "rank": 1,
        "roster": [
            {
                "character": {
                    "name": "Thrall",
                    "class": {"name": "Warrior"},
                    "spec": {"name": "Arms"},
                    "realm": {"slug": "azjol-nerub"},
                    "region": {"short_name": "us"},
                },
                "role": "tank",
            },
        ],
    }
    base.update(overrides)
    return base


class TestRunToRowFull:
    """Test _run_to_row with a full API response."""

    def test_full_response_all_fields_present(self):
        """Full API response: all expected fields, source='raiderio', roster flattened."""
        run = _make_run()
        result = _run_to_row(run, "season-tww-3")

        assert result["source"] == "raiderio"
        assert result["keystone_run_id"] == 99999
        assert result["dungeon_id"] == 390
        assert result["dungeon_name"] == "The Rookery"
        assert result["mythic_level"] == 10
        assert result["clear_time_ms"] == 1800000
        assert result["season"] == "season-tww-3"
        assert result["score"] == 150.5
        assert result["rank"] == 1
        assert isinstance(result["roster"], list)
        assert len(result["roster"]) == 1

    def test_full_response_roster_flattened(self):
        """Roster entry is a flattened dict with name, class, spec, role, realm, region."""
        run = _make_run()
        result = _run_to_row(run, "season-tww-3")

        player = result["roster"][0]
        assert player["name"] == "Thrall"
        assert player["class"] == "Warrior"
        assert player["spec"] == "Arms"
        assert player["role"] == "tank"
        # realm and region are left as-is (structs) from the raw API
        assert player["realm"] is not None
        assert player["region"] is not None


class TestRunToRowMissingOptionals:
    """Test _run_to_row with missing optional fields."""

    def test_missing_score_rank_completed_at(self):
        """score, rank, and completed_at are None when absent."""
        run = _make_run(score=None, rank=None)
        del run["completed_at"]
        result = _run_to_row(run, "season-tww-3")

        assert result["score"] is None
        assert result["rank"] is None
        assert result["completed_at"] is None

    def test_score_zero_not_confused_with_none(self):
        """A score of 0 is treated as None (since 0 is falsy and get returns None)."""
        run = _make_run(score=0)
        result = _run_to_row(run, "season-tww-3")
        # score=0 → float(0) is 0.0, but only None→None
        # The code: float(run["score"]) if run.get("score") is not None else None
        assert result["score"] == 0.0


class TestRunToRowEmptyRoster:
    """Test _run_to_row with empty roster."""

    def test_empty_roster(self):
        """Empty roster list produces an empty roster in the result."""
        run = _make_run(roster=[])
        result = _run_to_row(run, "season-tww-3")
        assert result["roster"] == []


class TestRunToRowNullCharacter:
    """Test _run_to_row when roster character is None."""

    def test_null_character_fields_are_none(self):
        """When character dict is present but fields are missing, name/class/spec/realm/region are None."""
        run = _make_run(
            roster=[
                {
                    "character": {},
                    "role": "dps",
                },
            ],
        )
        result = _run_to_row(run, "season-tww-3")
        player = result["roster"][0]

        assert player["name"] is None
        assert player["class"] is None
        assert player["spec"] is None
        assert player["role"] == "dps"
        assert player["realm"] is None
        assert player["region"] is None


class TestRunToRowWeeklyModifiers:
    """Test _run_to_row weekly_modifiers parsing."""

    def test_mixed_modifier_types(self):
        """weekly_modifiers with dicts and ints: [{id: 9}, 10] → [9, 10]."""
        run = _make_run(weekly_modifiers=[{"id": 9}, 10])
        result = _run_to_row(run, "season-tww-3")

        assert result["weekly_modifiers"] == [9, 10]

    def test_empty_modifiers(self):
        """Empty weekly_modifiers list produces empty list."""
        run = _make_run(weekly_modifiers=[])
        result = _run_to_row(run, "season-tww-3")
        assert result["weekly_modifiers"] == []

    def test_missing_modifiers_key(self):
        """Missing weekly_modifiers key defaults to empty list."""
        run = _make_run()
        del run["weekly_modifiers"]
        result = _run_to_row(run, "season-tww-3")
        assert result["weekly_modifiers"] == []


# ─── Tier 2: Spark integration tests ────────────────────────────────────────


def _make_spark_row(**overrides):
    """Factory for a run dict suitable for Spark DataFrame creation.

    Returns a dict matching bronze_raiderio_schema field types.
    Default values satisfy all non-nullable fields.
    """
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
        "roster": [
            {
                "name": "Thrall",
                "class": "Warrior",
                "spec": "Arms",
                "role": "tank",
                "realm": {"id": 1, "connectedRealmId": 11, "wowRealmId": 111, "wowConnectedRealmId": 1111, "name": "Azjol-Nerub", "slug": "azjol-nerub", "locale": "enUS"},
                "region": {"name": "US", "slug": "us", "short_name": "us"},
            }
        ],
        "score": 150.5,
        "rank": 1,
        "season": "season-tww-3",
        "ingested_at": datetime(2025, 1, 15, 21, 0, tzinfo=timezone.utc),
    }
    base.update(overrides)
    return base


@pytest.mark.spark
class TestIngestRaiderioRuns:
    """Tier 2 — Spark tests for ingest_raiderio_runs."""

    def test_ingest_returns_row_count(self, spark_session):
        """ingest_raiderio_runs returns the total number of rows written."""
        run_dicts = [_make_run(), _make_run(keystone_run_id=100002)]
        mock_client = MagicMock()
        mock_client.fetch_runs.side_effect = [run_dicts, []]

        with patch("orakel.pipeline.bronze.settings") as mock_settings, \
             patch.object(DataFrameWriter, "parquet"):
            mock_settings.MINIO_BUCKET = "test-bucket"
            result = ingest_raiderio_runs(mock_client, spark_session, "season-tww-3", limit=1)

        assert result == 2

    def test_ingest_empty_api_returns_zero(self, spark_session):
        """Empty API response returns 0 — no DataFrame created."""
        mock_client = MagicMock()
        mock_client.fetch_runs.return_value = []

        result = ingest_raiderio_runs(mock_client, spark_session, "season-tww-3")

        assert result == 0

    def test_ingest_dataframe_matches_schema(self, spark_session):
        """DataFrame created from ingest rows matches bronze_raiderio_schema."""
        rows = [_run_to_row(_make_run(), "season-tww-3")]
        df = spark_session.createDataFrame(rows, schema=bronze_raiderio_schema)

        # Verify schema fields are present and types match
        expected_fields = {f.name for f in bronze_raiderio_schema.fields}
        actual_fields = {f.name for f in df.schema.fields}
        assert expected_fields == actual_fields

        # Verify each row has source='raiderio' and correct season
        assert df.count() == 1
        row = df.collect()[0]
        assert row["source"] == "raiderio"
        assert row["season"] == "season-tww-3"

    def test_ingest_multiple_runs_schema(self, spark_session):
        """Multiple runs produce correct row count and schema."""
        rows = [
            _run_to_row(_make_run(keystone_run_id=100001), "season-tww-3"),
            _run_to_row(_make_run(keystone_run_id=100002), "season-tww-3"),
            _run_to_row(_make_run(keystone_run_id=100003), "season-tww-3"),
        ]
        df = spark_session.createDataFrame(rows, schema=bronze_raiderio_schema)

        assert df.count() == 3
        sources = [row["source"] for row in df.collect()]
        assert all(s == "raiderio" for s in sources)