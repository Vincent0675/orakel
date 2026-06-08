"""Tests for orakel.clients.raiderio — REST client with responses mock (Tier 1)."""

from __future__ import annotations

from unittest.mock import patch

import pytest
import requests
import responses

from orakel.clients.raiderio import RateLimitError, RaiderIOClient


BASE_URL = "https://raider.io/api/v1"


class TestFetchRunsSuccess:
    """Test RaiderIOClient.fetch_runs with successful responses."""

    @responses.activate
    def test_200_returns_runs_with_score_and_rank(self):
        """A 200 response returns list of runs with score and rank merged."""
        responses.add(
            responses.GET,
            f"{BASE_URL}/mythic-plus/runs",
            json={
                "rankings": [
                    {
                        "rank": 1,
                        "score": 150.5,
                        "run": {
                            "keystone_run_id": 12345,
                            "dungeon": {"id": 390, "name": "The Rookery"},
                            "mythic_level": 10,
                            "roster": [],
                        },
                    },
                    {
                        "rank": 2,
                        "score": 140.0,
                        "run": {
                            "keystone_run_id": 12346,
                            "dungeon": {"id": 390, "name": "The Rookery"},
                            "mythic_level": 9,
                            "roster": [],
                        },
                    },
                ]
            },
            status=200,
        )

        client = RaiderIOClient(api_key="test-key")
        runs = client.fetch_runs("season-tww-3", page=1)

        assert len(runs) == 2
        assert runs[0]["score"] == 150.5
        assert runs[0]["rank"] == 1
        assert runs[1]["score"] == 140.0

    @responses.activate
    def test_empty_rankings_returns_empty_list(self):
        """An empty rankings list returns []."""
        responses.add(
            responses.GET,
            f"{BASE_URL}/mythic-plus/runs",
            json={"rankings": []},
            status=200,
        )

        client = RaiderIOClient()
        runs = client.fetch_runs("season-tww-3", page=1)

        assert runs == []


class TestFetchRunsRetry:
    """Test RaiderIOClient.fetch_runs 429 retry logic."""

    @responses.activate
    def test_429_twice_then_200_succeeds(self):
        """429 twice then 200: all 3 requests made, runs returned."""
        responses.add(responses.GET, f"{BASE_URL}/mythic-plus/runs", status=429)
        responses.add(responses.GET, f"{BASE_URL}/mythic-plus/runs", status=429)
        responses.add(
            responses.GET,
            f"{BASE_URL}/mythic-plus/runs",
            json={"rankings": [{"rank": 1, "score": 100.0, "run": {"id": 1}}]},
            status=200,
        )

        with patch("orakel.clients.raiderio.time.sleep"):
            client = RaiderIOClient(max_retries=3)
            runs = client.fetch_runs("season-tww-3", page=1)

        assert len(runs) == 1
        assert runs[0]["rank"] == 1

    @responses.activate
    def test_429_exhausts_retries_raises_error(self):
        """429 four times with max_retries=3 raises RateLimitError."""
        for _ in range(4):
            responses.add(responses.GET, f"{BASE_URL}/mythic-plus/runs", status=429)

        with patch("orakel.clients.raiderio.time.sleep"):
            client = RaiderIOClient(max_retries=3)
            with pytest.raises(RateLimitError):
                client.fetch_runs("season-tww-3", page=1)

    @responses.activate
    def test_retry_after_header_respected(self):
        """Retry-After header value is passed to time.sleep."""
        responses.add(
            responses.GET,
            f"{BASE_URL}/mythic-plus/runs",
            status=429,
            headers={"Retry-After": "5"},
        )
        responses.add(
            responses.GET,
            f"{BASE_URL}/mythic-plus/runs",
            json={"rankings": []},
            status=200,
        )

        with patch("orakel.clients.raiderio.time.sleep") as mock_sleep:
            client = RaiderIOClient(max_retries=3)
            client.fetch_runs("season-tww-3", page=1)

        mock_sleep.assert_called_once_with(5)


class TestFetchRunsNetworkError:
    """Test RaiderIOClient.fetch_runs network error handling."""

    @responses.activate
    def test_connection_error_propagates(self):
        """ConnectionError from requests propagates without catching."""
        responses.add(
            responses.GET,
            f"{BASE_URL}/mythic-plus/runs",
            body=requests.ConnectionError("Network error"),
        )

        client = RaiderIOClient()
        with pytest.raises(requests.ConnectionError):
            client.fetch_runs("season-tww-3", page=1)