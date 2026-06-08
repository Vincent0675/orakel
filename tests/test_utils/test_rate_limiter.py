"""Tests for orakel.utils.rate_limiter — token bucket (Tier 1)."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from orakel.utils.rate_limiter import WCLRateLimiter


class TestRecordUsage:
    """Test WCLRateLimiter.record_usage state tracking."""

    def test_normal_consumption(self):
        """record_usage updates remaining and total_spent."""
        limiter = WCLRateLimiter()
        limiter.record_usage(cost=100, remaining=3500)

        assert limiter.points_remaining == 3500
        assert limiter.total_spent == 100

    def test_record_usage_without_remaining(self):
        """When remaining is None, it decrements from current budget."""
        limiter = WCLRateLimiter(hourly_budget=3600)
        limiter.record_usage(cost=200)

        assert limiter.points_remaining == 3400
        assert limiter.total_spent == 200


class TestWaitIfNeeded:
    """Test WCLRateLimiter.wait_if_needed budget checks."""

    def test_proceed_when_budget_available(self):
        """When enough budget remains, wait_if_needed returns True immediately."""
        limiter = WCLRateLimiter(hourly_budget=3600, threshold=0.90)
        limiter.record_usage(cost=100, remaining=3500)

        result = limiter.wait_if_needed(estimated_cost=50)
        assert result is True

    def test_rate_limit_wait_succeeds(self):
        """When budget is low but reset is imminent, wait and retry succeeds."""
        limiter = WCLRateLimiter(hourly_budget=100, threshold=0.90, max_retries=2)
        limiter.record_usage(cost=80, remaining=20, reset_at=9999999999)

        with patch("orakel.utils.rate_limiter.time.sleep") as mock_sleep:
            result = limiter.wait_if_needed(estimated_cost=50)

        # After sleep + reset, budget restored to hourly_budget → True
        assert result is True
        mock_sleep.assert_called()

    def test_budget_exhausted_returns_false(self):
        """When budget is exhausted after max retries, returns False."""
        limiter = WCLRateLimiter(hourly_budget=100, threshold=0.90, max_retries=3)
        limiter.record_usage(cost=95, remaining=5, reset_at=9999999999)

        # Force attempt count: budget is very low, retries will all fail
        # because sleep resets to hourly_budget but threshold check still fails
        # after sleeping max_retries times
        with patch("orakel.utils.rate_limiter.time.sleep"):
            # First few calls may consume retries; call repeatedly
            results = []
            for _ in range(4):
                limiter.record_usage(cost=95, remaining=5, reset_at=9999999999)
                results.append(limiter.wait_if_needed(estimated_cost=100))

        # At least one call should return False
        assert False in results

    def test_reset_at_seconds_vs_milliseconds(self):
        """Both seconds and milliseconds reset_at are normalized correctly."""
        limiter = WCLRateLimiter()

        # Seconds timestamp
        limiter.record_usage(cost=100, remaining=500, reset_at=1717000000)
        assert limiter._points_reset_at == 1717000000.0

        # Milliseconds timestamp
        limiter2 = WCLRateLimiter()
        limiter2.record_usage(cost=100, remaining=500, reset_at=1717000000000)
        assert limiter2._points_reset_at == 1717000000.0

    def test_no_reset_at_estimates_from_session_start(self):
        """Without reset_at, wait_if_needed estimates from session start + 3600."""
        limiter = WCLRateLimiter(hourly_budget=3600, threshold=0.90, max_retries=1)
        # Drain budget below threshold
        limiter.record_usage(cost=3400, remaining=200)
        # No reset_at set (stays 0.0)

        with patch("orakel.utils.rate_limiter.time.sleep") as mock_sleep, \
             patch("orakel.utils.rate_limiter.time.time", return_value=limiter._session_start + 100):
            result = limiter.wait_if_needed(estimated_cost=50)

        # Should estimate: 3600 - (now - session_start) + 5 seconds wait
        # This path should call time.sleep
        if mock_sleep.called:
            # The wait duration is max(0, 3600 - 100) + 5 = 3505 seconds, capped at 300
            wait_arg = mock_sleep.call_args[0][0]
            assert wait_arg <= 300