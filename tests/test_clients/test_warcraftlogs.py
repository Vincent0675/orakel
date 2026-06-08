"""Tests for orakel.clients.warcraftlogs — GraphQL + OAuth (Tier 1)."""

from __future__ import annotations

from unittest.mock import patch

import pytest
import responses

from orakel.clients.warcraftlogs import WCLAuthError, WarcraftLogsClient
from orakel.utils.rate_limiter import WCLRateLimiter


TOKEN_URL = "https://www.warcraftlogs.com/oauth/token"
API_URL = "https://www.warcraftlogs.com/api/v2/client"


class TestOAuthToken:
    """Test WarcraftLogsClient.authenticate() OAuth flow."""

    @responses.activate
    def test_oauth_token_acquisition_succeeds(self):
        """Mock token endpoint returns 200 with access_token; authenticate() caches it."""
        responses.add(
            responses.POST,
            TOKEN_URL,
            json={"access_token": "tok123", "expires_in": 3600},
            status=200,
        )

        client = WarcraftLogsClient(
            client_id="cid", client_secret="csec",
            rate_limiter=WCLRateLimiter(),
        )
        token = client.authenticate()

        assert token == "tok123"
        assert client._token == "tok123"
        assert client._token_expires_at > 0

    @responses.activate
    def test_oauth_401_raises_wcl_auth_error(self):
        """401 from token endpoint raises WCLAuthError."""
        responses.add(
            responses.POST,
            TOKEN_URL,
            json={"error": "unauthorized"},
            status=401,
        )

        client = WarcraftLogsClient(client_id="bad-id", client_secret="bad-sec")
        with pytest.raises(WCLAuthError, match="authentication failed"):
            client.authenticate()

    def test_missing_credentials_raises_wcl_auth_error(self, monkeypatch):
        """Empty client_id/client_secret raises WCLAuthError before HTTP call.

        The constructor falls back to settings when args are falsy,
        so we patch settings to force empty credentials.
        """
        from orakel.config import settings

        monkeypatch.setattr(settings, "WCL_CLIENT_ID", "")
        monkeypatch.setattr(settings, "WCL_CLIENT_SECRET", "")

        client = WarcraftLogsClient(client_id="", client_secret="")

        with pytest.raises(WCLAuthError, match="WCL_CLIENT_ID"):
            client.authenticate()

    @responses.activate
    def test_oauth_token_caching(self):
        """Second authenticate() call returns cached token without HTTP request."""
        responses.add(
            responses.POST,
            TOKEN_URL,
            json={"access_token": "cached-tok", "expires_in": 3600},
            status=200,
        )

        client = WarcraftLogsClient(client_id="cid", client_secret="csec")
        token1 = client.authenticate()
        token2 = client.authenticate()

        assert token1 == "cached-tok"
        assert token2 == "cached-tok"
        # Only 1 HTTP call made — token was cached
        assert len(responses.calls) == 1


class TestGraphQLQuery:
    """Test WarcraftLogsClient.query() GraphQL execution."""

    @responses.activate
    def test_graphql_query_succeeds(self):
        """Successful GraphQL query returns parsed JSON data."""
        # Token endpoint
        responses.add(
            responses.POST,
            TOKEN_URL,
            json={"access_token": "gql-tok", "expires_in": 3600},
            status=200,
        )
        # GraphQL endpoint
        responses.add(
            responses.POST,
            API_URL,
            json={"data": {"characterData": {"character": {"name": "Test"}}}},
            status=200,
        )

        client = WarcraftLogsClient(client_id="cid", client_secret="csec")
        result = client.query("{ characterData { character { name } } }")

        assert "data" in result
        assert result["data"]["characterData"]["character"]["name"] == "Test"

    @responses.activate
    def test_401_triggers_token_refresh_then_succeeds(self):
        """First GraphQL request returns 401; client refreshes token and retries successfully."""
        # First token
        responses.add(
            responses.POST,
            TOKEN_URL,
            json={"access_token": "first-tok", "expires_in": 3600},
            status=200,
        )
        # GraphQL 401
        responses.add(
            responses.POST,
            API_URL,
            json={"error": "unauthorized"},
            status=401,
        )
        # Second token (refresh)
        responses.add(
            responses.POST,
            TOKEN_URL,
            json={"access_token": "second-tok", "expires_in": 3600},
            status=200,
        )
        # GraphQL success on retry
        responses.add(
            responses.POST,
            API_URL,
            json={"data": {"reportData": {"report": {"fights": []}}}},
            status=200,
        )

        client = WarcraftLogsClient(client_id="cid", client_secret="csec")
        result = client.query("{ reportData { report { fights } } }")

        assert "data" in result
        # Token was refreshed (2 token calls total: initial + refresh)
        token_calls = [c for c in responses.calls if TOKEN_URL in c.request.url]
        assert len(token_calls) == 2

    @responses.activate
    def test_429_triggers_wait_and_retry(self):
        """429 triggers sleep(Retry-After) then retry succeeds."""
        # Token endpoint
        responses.add(
            responses.POST,
            TOKEN_URL,
            json={"access_token": "rl-tok", "expires_in": 3600},
            status=200,
        )
        # GraphQL 429 with Retry-After header
        responses.add(
            responses.POST,
            API_URL,
            json={"error": "rate_limited"},
            status=429,
            headers={"Retry-After": "10"},
        )
        # GraphQL success after retry
        responses.add(
            responses.POST,
            API_URL,
            json={"data": {"rateLimitData": {"cost": 5, "pointsRemaining": 3595, "pointsReset": 1717000000}}},
            status=200,
        )

        with patch("orakel.clients.warcraftlogs.time.sleep") as mock_sleep:
            client = WarcraftLogsClient(client_id="cid", client_secret="csec")
            result = client.query("{ test }")

        # Verify time.sleep was called with Retry-After value
        mock_sleep.assert_called_once_with(10)
        assert "data" in result


class TestRateLimitTracking:
    """Test rate limit data tracking in GraphQL responses."""

    @responses.activate
    def test_rate_limit_data_updates_limiter(self):
        """GraphQL response with rateLimitData updates WCLRateLimiter."""
        responses.add(
            responses.POST,
            TOKEN_URL,
            json={"access_token": "rl2-tok", "expires_in": 3600},
            status=200,
        )
        responses.add(
            responses.POST,
            API_URL,
            json={
                "data": {"test": "value"},
                "rateLimitData": {
                    "cost": 10,
                    "pointsRemaining": 3500,
                    "pointsReset": 1717000000,
                },
            },
            status=200,
        )

        limiter = WCLRateLimiter()
        client = WarcraftLogsClient(
            client_id="cid", client_secret="csec", rate_limiter=limiter,
        )
        client.query("{ test }")

        assert limiter.total_spent >= 10
        assert limiter.points_remaining == 3500