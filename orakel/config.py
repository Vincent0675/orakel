"""Orakel configuration — loads environment variables via python-dotenv."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

# Load .env from project root (where pyproject.toml lives)
_project_root = Path(__file__).resolve().parent.parent
load_dotenv(_project_root / ".env")


@dataclass
class Settings:
    """Application settings loaded from environment variables."""

    # Raider.IO
    RAIDERIO_API_KEY: str = field(default_factory=lambda: os.getenv("RAIDERIO_API_KEY", ""))
    RAIDERIO_BASE_URL: str = field(
        default_factory=lambda: os.getenv("RAIDERIO_BASE_URL", "https://raider.io/api/v1")
    )

    # WarcraftLogs
    WCL_CLIENT_ID: str = field(default_factory=lambda: os.getenv("WCL_CLIENT_ID", ""))
    WCL_CLIENT_SECRET: str = field(default_factory=lambda: os.getenv("WCL_CLIENT_SECRET", ""))
    WCL_TOKEN_URL: str = field(
        default_factory=lambda: os.getenv(
            "WCL_TOKEN_URL", "https://www.warcraftlogs.com/oauth/token"
        )
    )
    WCL_API_URL: str = field(
        default_factory=lambda: os.getenv(
            "WCL_API_URL", "https://www.warcraftlogs.com/api/v2/client"
        )
    )

    # MinIO
    MINIO_ENDPOINT: str = field(default_factory=lambda: os.getenv("MINIO_ENDPOINT", "localhost:9000"))
    MINIO_ACCESS_KEY: str = field(default_factory=lambda: os.getenv("MINIO_ACCESS_KEY", "orakel"))
    MINIO_SECRET_KEY: str = field(default_factory=lambda: os.getenv("MINIO_SECRET_KEY", "orakel123"))
    MINIO_BUCKET: str = field(default_factory=lambda: os.getenv("MINIO_BUCKET", "orakel"))
    MINIO_SECURE: bool = field(default_factory=lambda: os.getenv("MINIO_SECURE", "false").lower() == "true")

    # Season
    SEASON: str = field(default_factory=lambda: os.getenv("SEASON", "season-tww-3"))

    # MLflow
    MLFLOW_TRACKING_URI: str = field(
        default_factory=lambda: os.getenv(
            "MLFLOW_TRACKING_URI",
            f"file:///{str(Path(__file__).resolve().parent.parent / 'mlruns')}",
        )
    )

    # ─── AssetCheck tuning (SDD: Verification-Dagster-Orchestation) ─────────
    # All checks are warning-only (non-blocking). These knobs tune the
    # thresholds and let operators disable heavy checks during incidents.

    # Completeness: downstream_count / upstream_count must be >= this.
    # 0.5 = 50% — generous default, tune up after observing production.
    CHECK_COMPLETENESS_THRESHOLD: float = field(
        default_factory=lambda: float(os.getenv("CHECK_COMPLETENESS_THRESHOLD", "0.5"))
    )

    # Referential integrity sample size. 0 = full scan (default — Bronze/Silver
    # are small). Increase to e.g. 10000 when Gold tables grow past 100k rows.
    CHECK_RI_SAMPLE_SIZE: int = field(
        default_factory=lambda: int(os.getenv("CHECK_RI_SAMPLE_SIZE", "0"))
    )

    # Master toggles — set to "false" in .env to disable an entire check class.
    CHECK_RI_ENABLED: bool = field(
        default_factory=lambda: os.getenv("CHECK_RI_ENABLED", "true").lower() == "true"
    )
    CHECK_COMPLETENESS_ENABLED: bool = field(
        default_factory=lambda: os.getenv("CHECK_COMPLETENESS_ENABLED", "true").lower() == "true"
    )
    CHECK_SCHEMA_DRIFT_ENABLED: bool = field(
        default_factory=lambda: os.getenv("CHECK_SCHEMA_DRIFT_ENABLED", "true").lower() == "true"
    )


settings = Settings()