"""Tests for orakel.config — env var loading and defaults (Tier 1)."""

from __future__ import annotations

import os

import pytest


@pytest.fixture()
def fresh_settings():
    """Return a new Settings instance for each test.

    This avoids shared state from the module-level ``settings`` singleton.
    """
    from orakel.config import Settings

    return Settings()


class TestSettingsEnvVars:
    """Test Settings dataclass with env var overrides."""

    def test_all_vars_set(self, monkeypatch):
        """When all env vars are set, Settings uses those values."""
        monkeypatch.setenv("RAIDERIO_API_KEY", "test-key")
        monkeypatch.setenv("SEASON", "season-test-1")
        monkeypatch.setenv("MINIO_ENDPOINT", "minio:9000")

        from orakel.config import Settings

        s = Settings()
        assert s.RAIDERIO_API_KEY == "test-key"
        assert s.SEASON == "season-test-1"
        assert s.MINIO_ENDPOINT == "minio:9000"

    def test_missing_vars_get_defaults(self, monkeypatch):
        """When env vars are unset, Settings uses default values."""
        for var in [
            "RAIDERIO_API_KEY",
            "RAIDERIO_BASE_URL",
            "MINIO_BUCKET",
            "SEASON",
        ]:
            monkeypatch.delenv(var, raising=False)

        from orakel.config import Settings

        s = Settings()
        assert s.RAIDERIO_API_KEY == ""
        assert s.RAIDERIO_BASE_URL == "https://raider.io/api/v1"
        assert s.MINIO_BUCKET == "orakel"
        assert s.SEASON == "season-tww-3"

    def test_minio_secure_true(self, monkeypatch):
        """MINIO_SECURE=true should parse to True."""
        monkeypatch.setenv("MINIO_SECURE", "true")

        from orakel.config import Settings

        s = Settings()
        assert s.MINIO_SECURE is True

    def test_minio_secure_false_unset(self, monkeypatch):
        """MINIO_SECURE=false (or unset) should parse to False."""
        monkeypatch.delenv("MINIO_SECURE", raising=False)

        from orakel.config import Settings

        s = Settings()
        assert s.MINIO_SECURE is False