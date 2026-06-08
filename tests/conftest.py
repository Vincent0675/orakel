"""Shared test fixtures for Orakel.

Tier 1 fixtures: pure pytest (no Spark).
Tier 2 fixtures: SparkSession for pipeline integration tests.
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


# ─── Tier 2: Spark fixtures ────────────────────────────────────────────────


@pytest.fixture(scope="session")
def spark_session():
    """Session-scoped SparkSession for Tier 2 tests (local mode, minimal config).

    Created once per test session and reused across all @pytest.mark.spark
    tests. Automatically stopped when the session ends.
    """
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
        .getOrCreate()
    )
    yield spark
    spark.stop()