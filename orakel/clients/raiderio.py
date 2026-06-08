"""Raider.IO REST API client with pagination and rate-limit handling."""

from __future__ import annotations

import logging
import time
from typing import Any

import requests

logger = logging.getLogger(__name__)

# Default base URL for the Raider.IO public API
DEFAULT_BASE_URL = "https://raider.io/api/v1"


class RateLimitError(Exception):
    """Raised when the Raider.IO API returns HTTP 429 after max retries."""


class RaiderIOClient:
    """Client for the Raider.IO Mythic+ runs API.

    Supports pagination, rate-limit detection (HTTP 429), and exponential
    backoff with Retry-After header respect.

    Args:
        api_key: Optional Raider.IO API key for higher rate limits.
        base_url: API base URL (default: https://raider.io/api/v1).
        max_retries: Maximum number of retries on 429 responses.
    """

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str = DEFAULT_BASE_URL,
        max_retries: int = 3,
    ) -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.max_retries = max_retries
        self._session = requests.Session()
        if api_key:
            self._session.params = {"access_key": api_key}

    def fetch_runs(
        self, season: str, page: int = 1
    ) -> list[dict[str, Any]]:
        """Fetch a single page of Mythic+ runs from Raider.IO.

        Args:
            season: Season identifier (e.g. "season-tww-3").
            page: Page number (1-indexed).

        Returns:
            List of run dicts from the API response. Returns an empty list
            if the page has no results.

        Raises:
            RateLimitError: If HTTP 429 persists after max_retries.
            requests.HTTPError: If a non-429 error response is received.
        """
        url = f"{self.base_url}/mythic-plus/runs"
        params: dict[str, Any] = {
            "season": season,
            "page": page,
        }

        for attempt in range(self.max_retries + 1):
            response = self._session.get(url, params=params, timeout=30)

            if response.status_code == 200:
                data = response.json()
                # Raider.IO returns rankings, each containing a 'run' key
                rankings = data.get("rankings", [])
                # Each ranking entry has {rank, score, run{...}}
                # Merge score/rank into the run dict before returning
                runs = []
                for r in rankings:
                    run_data = r.get("run", r)
                    run_data["score"] = r.get("score")
                    run_data["rank"] = r.get("rank")
                    runs.append(run_data)
                if not runs:
                    logger.info(
                        "No runs returned for season=%s page=%d — end of data",
                        season,
                        page,
                    )
                return runs

            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                wait = (
                    int(retry_after)
                    if retry_after and retry_after.isdigit()
                    else 2**attempt
                )
                logger.warning(
                    "HTTP 429 rate limit hit (attempt %d/%d). "
                    "Waiting %ds before retry...",
                    attempt + 1,
                    self.max_retries,
                    wait,
                )
                if attempt < self.max_retries:
                    time.sleep(wait)
                    continue
                raise RateLimitError(
                    f"Raider.IO API rate limit exceeded after "
                    f"{self.max_retries} retries"
                )

            # Other HTTP error
            response.raise_for_status()

        # Should not reach here, but just in case
        return []