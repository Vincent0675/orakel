"""Shared test fixtures for Orakel — Tier 1 only (no SparkSession).

Tier 2 fixtures (spark_session) will be added in WU #2.
"""

from __future__ import annotations

import pytest


@pytest.fixture()
def mock_settings(monkeypatch):
    """Override environment variables with test values.

    Provides deterministic Settings without needing a .env file.
    Restores original env vars after the test via monkeypatch.
    """
    monkeypatch.setenv("RAIDERIO_API_KEY", "test-rio-key")
    monkeypatch.setenv("WCL_CLIENT_ID", "test-wcl-id")
    monkeypatch.setenv("WCL_CLIENT_SECRET", "test-wcl-secret")
    monkeypatch.setenv("SEASON", "season-test-1")
    monkeypatch.setenv("MINIO_ENDPOINT", "localhost:9000")
    monkeypatch.setenv("MINIO_BUCKET", "test-orakel")


@pytest.fixture()
def mock_raiderio_session():
    """Pre-configured responses mock for Raider.IO API.

    Returns the RequestMock object. Tests should add specific
    responses and use ``@responses.activate`` or the context manager.
    """
    import responses

    return responses


@pytest.fixture()
def mock_wcl_session():
    """Pre-configured responses mock for WCL OAuth + GraphQL endpoints.

    Returns the RequestMock object. Tests should add specific
    responses and use ``@responses.activate`` or the context manager.
    """
    import responses

    return responses