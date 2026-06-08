"""Tests for orakel.pipeline.bronze._run_to_row — dict transform (Tier 1).

Only tests the _run_to_row pure function. The Spark-based ingest_raiderio_runs
will be tested in Tier 2 (WU #2).
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from orakel.pipeline.bronze import _run_to_row


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
            ]
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