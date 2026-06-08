"""WarcraftLogs GraphQL API client with OAuth 2.0 authentication and point tracking."""

from __future__ import annotations

import logging
import re
import time
from typing import Any

import requests

from orakel.config import settings
from orakel.utils.rate_limiter import WCLRateLimiter

logger = logging.getLogger(__name__)

# WCL GraphQL endpoint and OAuth token endpoint
DEFAULT_API_URL = "https://www.warcraftlogs.com/api/v2/client"
DEFAULT_TOKEN_URL = "https://www.warcraftlogs.com/oauth/token"


class WCLAuthError(Exception):
    """Raised when WarcraftLogs OAuth authentication fails."""


class WCLRateLimitError(Exception):
    """Raised when WCL point budget is exhausted after retries."""


class WarcraftLogsClient:
    """Client for the WarcraftLogs GraphQL API.

    Supports OAuth 2.0 Client Credentials authentication, point budget
    tracking, and automatic rate-limit handling.

    Args:
        client_id: WCL OAuth client ID.
        client_secret: WCL OAuth client secret.
        api_url: WCL GraphQL API URL.
        token_url: WCL OAuth token URL.
        rate_limiter: Optional rate limiter instance. Auto-created if None.
    """

    def __init__(
        self,
        client_id: str | None = None,
        client_secret: str | None = None,
        api_url: str = DEFAULT_API_URL,
        token_url: str = DEFAULT_TOKEN_URL,
        rate_limiter: WCLRateLimiter | None = None,
    ) -> None:
        self.client_id = client_id or settings.WCL_CLIENT_ID
        self.client_secret = client_secret or settings.WCL_CLIENT_SECRET
        self.api_url = api_url
        self.token_url = token_url
        self.rate_limiter = rate_limiter or WCLRateLimiter()
        self._token: str | None = None
        self._token_expires_at: float = 0.0
        self._session = requests.Session()

    def authenticate(self) -> str:
        """Obtain or refresh OAuth 2.0 Bearer token via Client Credentials flow.

        Returns:
            Access token string.

        Raises:
            WCLAuthError: If authentication fails or credentials are missing.
        """
        if not self.client_id or not self.client_secret:
            raise WCLAuthError(
                "WCL_CLIENT_ID and WCL_CLIENT_SECRET must be set in .env. "
                "Get them from https://www.warcraftlogs.com/profile"
            )

        # Reuse token if still valid (with 60s buffer)
        if self._token and time.time() < (self._token_expires_at - 60):
            return self._token

        logger.info("Authenticating with WarcraftLogs OAuth 2.0...")
        response = requests.post(
            self.token_url,
            data={"grant_type": "client_credentials"},
            auth=(self.client_id, self.client_secret),
            timeout=30,
        )

        if response.status_code != 200:
            raise WCLAuthError(
                f"WCL authentication failed (HTTP {response.status_code}): "
                f"{response.text[:200]}"
            )

        token_data = response.json()
        self._token = token_data["access_token"]
        self._token_expires_at = time.time() + token_data.get("expires_in", 3600)
        logger.info("WCL authentication successful, token expires in %ds", token_data.get("expires_in", 3600))
        return self._token

    def query(self, query: str, variables: dict | None = None) -> dict[str, Any]:
        """Execute a GraphQL query against the WCL API.

        Automatically authenticates if needed and handles 401 errors by
        refreshing the token and retrying once.

        Args:
            query: GraphQL query string.
            variables: Optional dict of GraphQL variables.

        Returns:
            Parsed JSON response as a dict.

        Raises:
            WCLAuthError: If authentication fails.
            WCLRateLimitError: If rate limit budget is exhausted.
            requests.HTTPError: If a non-retryable HTTP error occurs.
        """
        token = self.authenticate()
        headers = {"Authorization": f"Bearer {token}"}
        payload: dict[str, Any] = {"query": query}
        if variables:
            payload["variables"] = variables

        response = self._session.post(
            self.api_url,
            json=payload,
            headers=headers,
            timeout=60,
        )

        # Handle 401 by refreshing token and retrying once
        if response.status_code == 401:
            logger.warning("WCL 401 Unauthorized — refreshing token and retrying")
            self._token = None
            token = self.authenticate()
            headers = {"Authorization": f"Bearer {token}"}
            response = self._session.post(
                self.api_url,
                json=payload,
                headers=headers,
                timeout=60,
            )

        # Handle rate limiting (HTTP 429 or point budget exhaustion)
        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After")
            wait = int(retry_after) if retry_after and retry_after.isdigit() else 60
            logger.warning("WCL HTTP 429 rate limited. Waiting %ds...", wait)
            time.sleep(wait)
            # Retry once after waiting
            response = self._session.post(
                self.api_url,
                json=payload,
                headers=headers,
                timeout=60,
            )

        if response.status_code != 200:
            raise requests.HTTPError(
                f"WCL API error (HTTP {response.status_code}): {response.text[:500]}"
            )

        result = response.json()

        # Track point usage from rateLimitData
        rate_limit_data = result.get("rateLimitData", {})
        if rate_limit_data:
            points_spent = rate_limit_data.get("cost", 0)
            points_remaining = rate_limit_data.get("pointsRemaining", 0)
            points_reset = rate_limit_data.get("pointsReset", 0)
            self.rate_limiter.record_usage(
                cost=points_spent,
                remaining=points_remaining,
                reset_at=points_reset,
            )
            logger.debug(
                "WCL points: spent=%d, remaining=%d, reset_at=%d",
                points_spent,
                points_remaining,
                points_reset,
            )

        # Check for GraphQL errors
        if "errors" in result:
            errors = result["errors"]
            logger.warning("WCL GraphQL errors: %s", errors[0].get("message", str(errors)))
            # Return the full result so callers can inspect errors
            # but also data if present

        return result

    def _fetch_reports_page(
        self,
        character_name: str,
        server_slug: str,
        region: str,
        page: int,
        limit: int = 20,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Fetch a single page of recent reports for a character.

        Args:
            character_name: Cleaned character name.
            server_slug: Server slug.
            region: Region code.
            page: Page number (1-indexed).
            limit: Reports per page (max 20 for complexity).

        Returns:
            Tuple of (reports_list, guilds_list).
        """
        query = """
        query GetRecentReportsPage($name: String!, $serverSlug: String!, $serverRegion: String!, $limit: Int!, $page: Int!) {
            characterData {
                character(name: $name, serverSlug: $serverSlug, serverRegion: $serverRegion) {
                    guilds { id name }
                    recentReports(limit: $limit, page: $page) {
                        data {
                            code
                            startTime
                            endTime
                            zone { id }
                            fights {
                                id
                                encounterID
                                keystoneLevel
                                keystoneAffixes
                                keystoneTime
                                kill
                                startTime
                                endTime
                            }
                        }
                    }
                }
            }
            rateLimitData {
                limitPerHour
                pointsSpentThisHour
                pointsResetIn
            }
        }
        """
        result = self.query(query, {
            "name": character_name,
            "serverSlug": server_slug,
            "serverRegion": region,
            "limit": limit,
            "page": page,
        })
        char_data = result.get("data", {}).get("characterData", {}).get("character")
        if not char_data:
            return [], []

        pagination = char_data.get("recentReports", {})
        reports = pagination.get("data", []) if isinstance(pagination, dict) else []
        guilds = char_data.get("guilds", [])
        return reports, guilds

    def get_recent_reports(
        self,
        character_name: str,
        server_slug: str,
        region: str = "us",
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Fetch recent WCL reports for a character, including fights data.

        Automatically paginates across multiple pages to find M+ reports
        (zones 45, 43, 47 = TWW S3, S2, S1 Mythic+).

        Args:
            character_name: Character name (e.g. "Jinskii").
            server_slug: Server slug (e.g. "azjol-nerub").
            region: Region code (e.g. "us", "eu").
            limit: Max reports to fetch (default 100).

        Returns:
            List of report dicts with nested fights data.
        """
        estimated_cost = min(limit * 15, 3600)
        if not self.rate_limiter.wait_if_needed(estimated_cost):
            raise WCLRateLimitError("WCL point budget exhausted, cannot fetch recent reports")

        clean_name = re.sub(r'-\d+$', '', character_name)

        all_reports: list[dict[str, Any]] = []
        all_guilds: list[dict[str, Any]] = []
        page = 1
        max_pages = max(1, min(5, (limit + 19) // 20))  # 5 pages max = 100 reports

        while page <= max_pages:
            reports, guilds = self._fetch_reports_page(
                clean_name, server_slug, region, page
            )
            if not reports:
                break

            # Store guild info from first page
            if page == 1 and guilds:
                all_guilds = guilds

            # Filter to M+ zones (45 = TWW S3, 43 = TWW S2, 47 = TWW S1)
            for r in reports:
                zone = r.get("zone", {})
                zone_id = zone.get("id") if isinstance(zone, dict) else zone
                r["_zone_id"] = zone_id
                if zone_id in (45, 43, 47):
                    all_reports.append(r)

            # Check if we already have enough M+ reports (stop fetching more pages)
            if len(all_reports) >= 10:
                break

            page += 1

        # If no M+ reports found, return ALL reports (for broader search)
        if not all_reports:
            logger.debug("No M+ zone reports found for %s in pages 1-%d", clean_name, page)

        logger.info(
            "Found %d M+ reports for %s on %s-%s (checked %d pages)",
            len(all_reports), clean_name, server_slug, region, page,
        )
        return all_reports

    def get_fights(self, report_code: str) -> list[dict[str, Any]]:
        """Fetch the fight list for a WCL report.

        Args:
            report_code: WCL report code (e.g. "ABC123").

        Returns:
            List of fight dicts with M+ metadata (keystoneLevel, affixes, etc.).
        """
        if not self.rate_limiter.wait_if_needed(10):
            raise WCLRateLimitError("WCL point budget exhausted, cannot fetch fights")

        query = """
        query GetFights($code: String!) {
            reportData {
                report(code: $code) {
                    fights {
                        id
                        encounterID
                        keystoneLevel
                        keystoneAffixes { id }
                        keystoneTime
                        kill
                        startTime
                        endTime
                    }
                }
            }
        }
        """

        result = self.query(query, {"code": report_code})
        report_data = result.get("data", {}).get("reportData", {}).get("report", {})
        return report_data.get("fights", [])

    def get_master_data(self, report_code: str) -> dict[str, Any]:
        """Fetch master data (actor/player resolution) for a WCL report.

        Resolves WCL actor IDs to character names and server info,
        needed for roster verification (Layer 3 of fuzzy join).

        Args:
            report_code: WCL report code.

        Returns:
            Dict with 'actors' list containing {id, name, server, type} entries.
        """
        if not self.rate_limiter.wait_if_needed(10):
            raise WCLRateLimitError("WCL point budget exhausted, cannot fetch master data")

        query = """
        query GetMasterData($code: String!) {
            reportData {
                report(code: $code) {
                    masterData {
                        actors {
                            id
                            name
                            server
                            type
                        }
                    }
                }
            }
        }
        """

        result = self.query(query, {"code": report_code})
        report_data = result.get("data", {}).get("reportData", {}).get("report", {})
        master = report_data.get("masterData", {})
        return master

    def get_table(
        self,
        report_code: str,
        fight_ids: list[int],
        data_type: str,
    ) -> dict[str, Any]:
        """Fetch a summary table from WCL for specific fights.

        Used for DamageTaken and HealingDone aggregated data.

        Args:
            report_code: WCL report code.
            fight_ids: List of fight IDs to include.
            data_type: Table type, e.g. "DamageTaken" or "Healing".

        Returns:
            Dict with table data including entries with per-player summaries.
        """
        if not self.rate_limiter.wait_if_needed(50):
            raise WCLRateLimitError(f"WCL point budget exhausted, cannot fetch {data_type} table")

        fight_ids_str = ", ".join(str(fid) for fid in fight_ids)

        query = """
        query GetTable($code: String!, $fightIDs: [Int]!, $dataType: TableDataType!) {
            reportData {
                report(code: $code) {
                    table(dataType: $dataType, fightIDs: $fightIDs)
                }
            }
        }
        """

        variables = {
            "code": report_code,
            "fightIDs": fight_ids,
            "dataType": data_type,
        }

        result = self.query(query, variables)
        report_data = result.get("data", {}).get("reportData", {}).get("report", {})
        return report_data.get("table", {})

    def get_events(
        self,
        report_code: str,
        fight_ids: list[int],
        data_type: str,
    ) -> list[dict[str, Any]]:
        """Fetch raw events from WCL for specific fights.

        Used for Interrupt events (KPI 3). WCL events query returns
        a ReportEventPaginator with ``data`` as a JSON list and
        ``nextPageTimestamp`` for pagination.

        Args:
            report_code: WCL report code.
            fight_ids: List of fight IDs to include.
            data_type: Event type, e.g. "Interrupts".

        Returns:
            List of event dicts.
        """
        all_events: list[dict[str, Any]] = []
        if not self.rate_limiter.wait_if_needed(150):
            raise WCLRateLimitError(f"WCL point budget exhausted, cannot fetch {data_type} events")

        query = """
        query GetEvents($code: String!, $fightIDs: [Int]!, $dataType: EventDataType!) {
            reportData {
                report(code: $code) {
                    events(dataType: $dataType, fightIDs: $fightIDs) {
                        data
                        nextPageTimestamp
                    }
                }
            }
        }
        """

        variables = {
            "code": report_code,
            "fightIDs": fight_ids,
            "dataType": data_type,
        }

        result = self.query(query, variables)
        report_data = result.get("data", {}).get("reportData", {}).get("report", {})
        events_data = report_data.get("events", {})

        if isinstance(events_data, dict):
            # data field is a raw JSON list (no sub-fields allowed)
            raw_data = events_data.get("data", [])
            if isinstance(raw_data, list):
                all_events.extend(raw_data)
            elif isinstance(raw_data, dict):
                all_events.extend(raw_data.get("entries", raw_data.get("data", [])))

        return all_events