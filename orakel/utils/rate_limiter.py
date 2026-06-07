"""Rate limiter for WarcraftLogs API point budget management.

Implements a token-bucket style rate limiter that tracks WCL's
point-based rate limit system (~3600 points/hour).
"""

from __future__ import annotations

import logging
import time

logger = logging.getLogger(__name__)

# WCL default point budget per hour
DEFAULT_HOURLY_BUDGET = 3600
# Percentage threshold at which we pause (90% = wait until reset)
DEFAULT_THRESHOLD = 0.90


class WCLRateLimiter:
    """Rate limiter for WarcraftLogs API point budget.

    WCL uses a point system where each query costs points and the
    budget resets hourly. This tracker monitors usage via the
    `rateLimitData` field in API responses and pauses execution
    when approaching the hourly limit.

    Args:
        hourly_budget: Maximum points per hour (default: 3600).
        threshold: Fraction of budget at which to pause (default: 0.90).
        max_retries: Maximum retries on rate limit exhaustion.
    """

    def __init__(
        self,
        hourly_budget: int = DEFAULT_HOURLY_BUDGET,
        threshold: float = DEFAULT_THRESHOLD,
        max_retries: int = 3,
    ) -> None:
        self.hourly_budget = hourly_budget
        self.threshold = threshold
        self.max_retries = max_retries
        # Track from WCL's rateLimitData responses
        self._points_remaining: int = hourly_budget
        self._points_reset_at: float = 0.0  # Unix timestamp
        self._total_spent: int = 0
        self._session_start: float = time.time()

    def record_usage(
        self,
        cost: int = 0,
        remaining: int | None = None,
        reset_at: int | None = None,
    ) -> None:
        """Record point usage from a WCL API response.

        Call this after each API response with the rateLimitData
        fields to keep the internal budget tracking in sync with WCL's
        server-side counters.

        Args:
            cost: Points spent on this query.
            remaining: Points remaining from WCL response (preferred).
            reset_at: Unix timestamp when points reset (from WCL response).
        """
        self._total_spent += cost
        if remaining is not None:
            self._points_remaining = remaining
        else:
            self._points_remaining = max(0, self._points_remaining - cost)

        if reset_at is not None:
            # WCL returns epoch seconds; convert if in seconds
            if reset_at < 1e10:  # seconds
                self._points_reset_at = float(reset_at)
            else:  # milliseconds
                self._points_reset_at = reset_at / 1000.0

        logger.debug(
            "WCL rate: spent=%d, total=%d, remaining=%d",
            cost,
            self._total_spent,
            self._points_remaining,
        )

    @property
    def points_remaining(self) -> int:
        """Current estimated points remaining in the budget window."""
        return self._points_remaining

    @property
    def total_spent(self) -> int:
        """Total points spent in this session."""
        return self._total_spent

    def wait_if_needed(self, estimated_cost: int) -> bool:
        """Check if we can proceed with a query, waiting if approaching budget.

        If the remaining budget after the estimated cost would be below
        the threshold percentage, we wait until the budget resets.

        Args:
            estimated_cost: Estimated point cost for the upcoming query.

        Returns:
            True if OK to proceed, False if budget is truly exhausted
            and we should abort.
        """
        for attempt in range(self.max_retries + 1):
            budget_after = self._points_remaining - estimated_cost
            threshold_budget = self.hourly_budget * (1 - self.threshold)

            # If we have enough headroom, proceed
            if self._points_remaining > threshold_budget + estimated_cost:
                return True

            # If points reset is imminent (within 2 minutes), wait
            now = time.time()
            if self._points_reset_at > 0:
                wait_seconds = max(0, self._points_reset_at - now) + 5
            else:
                # Estimate reset: assume we started ~hourly_budget ago,
                # so reset should be about an hour from session start
                elapsed = now - self._session_start
                wait_seconds = max(0, 3600 - elapsed) + 5

            if wait_seconds > 0:
                if attempt < self.max_retries:
                    logger.warning(
                        "WCL budget low (remaining=%d, cost=%d). "
                        "Waiting %ds for reset (attempt %d/%d)...",
                        self._points_remaining,
                        estimated_cost,
                        int(wait_seconds),
                        attempt + 1,
                        self.max_retries,
                    )
                    time.sleep(min(wait_seconds, 300))  # Cap at 5 minutes
                    # After waiting, assume budget reset
                    self._points_remaining = self.hourly_budget
                    continue
                else:
                    logger.error(
                        "WCL budget exhausted after %d retries. "
                        "Remaining=%d, needed=%d",
                        self.max_retries,
                        self._points_remaining,
                        estimated_cost,
                    )
                    return False

            # Enough budget after re-check
            return True

        return False