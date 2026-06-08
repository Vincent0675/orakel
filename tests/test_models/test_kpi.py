"""Tests for orakel.models.kpi — pure function unit tests (Tier 1, no Spark).

Covers compute_death_clock (6 cases), compute_healer_deficit (5 cases),
and compute_synergy_score (6 cases) from Spec Requirement 3.
"""

from __future__ import annotations

import math

import pytest

from orakel.models.kpi import (
    compute_death_clock,
    compute_healer_deficit,
    compute_synergy_score,
)


# ─── KPI 1: compute_death_clock ────────────────────────────────────────────


@pytest.mark.parametrize(
    "dtps, hps, max_hp, expected_seconds, expected_category",
    [
        # Safe — healer out-heals damage
        pytest.param(100, 200, 600_000, -1.0, "safe", id="safe-healer-out-heals"),
        # Safe — death clock > 15s
        pytest.param(100, 50, 600_000, 12000.0, "safe", id="safe-clock-gt-15"),
        # Moderate — 5-15s window
        pytest.param(100_000, 20_000, 600_000, 7.5, "moderate", id="moderate-5-15s"),
        # Critical — < 5s
        pytest.param(200_000, 20_000, 600_000, 3.33, "critical", id="critical-lt-5s"),
        # Zero DTPS
        pytest.param(0, 0, 600_000, -1.0, "safe", id="zero-dtps"),
        # Negative DTPS
        pytest.param(-50, 0, 600_000, -1.0, "safe", id="negative-dtps"),
    ],
)
def test_compute_death_clock(dtps, hps, max_hp, expected_seconds, expected_category):
    result_seconds, result_category = compute_death_clock(dtps, hps, max_hp)
    assert result_category == expected_category
    if expected_seconds == -1.0:
        assert result_seconds == expected_seconds
    else:
        assert math.isclose(result_seconds, expected_seconds, rel_tol=1e-2)


# ─── KPI 2: compute_healer_deficit ────────────────────────────────────────


@pytest.mark.parametrize(
    "tank_dtps, healer_hps, expected_ratio, expected_category",
    [
        # Comfortable — ratio < 1.0
        pytest.param(100, 150, 0.6667, "comfortable", id="comfortable-ratio-lt-1"),
        # Moderate — ratio 1.0-1.2
        pytest.param(100, 90, 1.1111, "moderate", id="moderate-ratio-1-1.2"),
        # Critical — ratio > 1.2
        pytest.param(150, 100, 1.5, "critical", id="critical-ratio-gt-1.2"),
        # Zero DTPS
        pytest.param(0, 100, None, None, id="zero-dtps"),
        # Zero HPS
        pytest.param(100, 0, None, None, id="zero-hps"),
    ],
)
def test_compute_healer_deficit(tank_dtps, healer_hps, expected_ratio, expected_category):
    result_ratio, result_category = compute_healer_deficit(tank_dtps, healer_hps)
    assert result_category == expected_category
    if expected_ratio is None:
        assert result_ratio is None
    else:
        assert math.isclose(result_ratio, expected_ratio, rel_tol=1e-3)


# ─── KPI 4: compute_synergy_score ────────────────────────────────────────


@pytest.mark.parametrize(
    "comp_avg, overall_avg, expected",
    [
        # Synergy — comp clears faster (score < 1.0)
        pytest.param(180, 200, 0.9, id="synergy-comp-faster"),
        # Anti-synergy — comp slower (score > 1.0)
        pytest.param(220, 200, 1.1, id="anti-synergy-comp-slower"),
        # Neutral — same average
        pytest.param(200, 200, 1.0, id="neutral-same-average"),
        # None comp_avg
        pytest.param(None, 200, None, id="none-comp-avg"),
        # None overall_avg
        pytest.param(200, None, None, id="none-overall-avg"),
        # Zero overall_avg
        pytest.param(200, 0, None, id="zero-overall-avg"),
    ],
)
def test_compute_synergy_score(comp_avg, overall_avg, expected):
    result = compute_synergy_score(comp_avg, overall_avg)
    if expected is None:
        assert result is None
    else:
        assert math.isclose(result, expected, rel_tol=1e-4)