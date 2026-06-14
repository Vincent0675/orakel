"""Cross-layer schema drift @asset_check wrappers.

Each wrapper delegates to the reusable ``check_schema_drift`` core
function (in ``orakel.pipeline.assets.checks``).  Wrappers exist to:

  1. Bind a Parquet path to its expected ``StructType`` from
     ``orakel.models.schemas`` at import time so the Dagster UI can
     surface a check on every asset.
  2. Apply consistent mode (``"superset"``) and season filtering
     across all checks.
  3. Keep the path -> schema mapping centralized here; schema
     fingerprints come from the import, not from runtime reflection.

All wrappers are warning-only — drift is reported but does not block.

Targets (SDD: Verification-Dagster-Orchestation, Phase 8 / SD table):

    SD-1 bronze/raiderio/runs           -> bronze_raiderio_schema
    SD-2 bronze/wcl                     -> bronze_wcl_reports_schema
    SD-3 silver/raiderio_runs           -> silver_raiderio_schema
    SD-4 silver/dungeon_runs            -> silver_dungeon_runs_schema
    SD-5 silver/player_performance      -> silver_player_performance_schema
    SD-6 gold KPIs (composite)          -> per-KPI schemas
    SD-7 gold/features                  -> gold_features_schema
"""

from __future__ import annotations

import logging

from dagster import AssetCheckResult, AssetKey, asset_check

from orakel.config import settings
from orakel.models.schemas import (
    bronze_raiderio_schema,
    bronze_wcl_reports_schema,
    gold_features_schema,
    gold_kpi_composition_synergy_schema,
    gold_kpi_healer_deficit_schema,
    gold_kpi_interrupt_rate_schema,
    gold_kpi_tank_death_clock_schema,
    silver_dungeon_runs_schema,
    silver_player_performance_schema,
    silver_raiderio_schema,
)
from orakel.pipeline.assets.checks import check_schema_drift
from orakel.utils.minio import get_spark_session

logger = logging.getLogger(__name__)


def _run_sd(
    path: str,
    expected_schema,
    mode: str = "superset",
    season: str | None = None,
) -> AssetCheckResult:
    """Acquire a SparkSession, run the schema-drift check, tear down."""
    spark = get_spark_session("sd_check")
    try:
        return check_schema_drift(
            spark,
            path=path,
            expected_schema=expected_schema,
            season=season or settings.SEASON,
            mode=mode,
        )
    finally:
        spark.stop()


# ─── SD-1: bronze/raiderio/runs ──────────────────────────────────────────────


@asset_check(
    asset=AssetKey(["orakel", "bronze_rio"]),
    description="bronze/raiderio/runs schema must contain bronze_raiderio_schema",
)
def sd_bronze_rio_check() -> dict:
    """SD-1: bronze_rio schema drift."""
    if not settings.CHECK_SCHEMA_DRIFT_ENABLED:
        return AssetCheckResult(
            passed=True, metadata={"disabled": True, "mode": "superset"}
        )
    return _run_sd(
        path=f"s3a://{settings.MINIO_BUCKET}/bronze/raiderio/runs",
        expected_schema=bronze_raiderio_schema,
    )


# ─── SD-2: bronze/wcl ────────────────────────────────────────────────────────


@asset_check(
    asset=AssetKey(["orakel", "bronze_wcl"]),
    description="bronze/wcl schema must contain bronze_wcl_reports_schema",
)
def sd_bronze_wcl_check() -> dict:
    """SD-2: bronze_wcl schema drift.

    Uses ``bronze_wcl_reports_schema`` (the primary WCL ingest target)
    as the reference.  ``bronze_wcl_events_schema`` is the sub-table
    for events; its drift is not checked separately because the
    reports table is the entry point and its schema is the public
    contract.
    """
    if not settings.CHECK_SCHEMA_DRIFT_ENABLED:
        return AssetCheckResult(
            passed=True, metadata={"disabled": True, "mode": "superset"}
        )
    return _run_sd(
        path=f"s3a://{settings.MINIO_BUCKET}/bronze/wcl",
        expected_schema=bronze_wcl_reports_schema,
    )


# ─── SD-3: silver/raiderio_runs ──────────────────────────────────────────────


@asset_check(
    asset=AssetKey(["orakel", "silver_raiderio"]),
    description="silver/raiderio_runs schema must contain silver_raiderio_schema",
)
def sd_silver_raiderio_check() -> dict:
    """SD-3: silver_raiderio schema drift."""
    if not settings.CHECK_SCHEMA_DRIFT_ENABLED:
        return AssetCheckResult(
            passed=True, metadata={"disabled": True, "mode": "superset"}
        )
    return _run_sd(
        path=f"s3a://{settings.MINIO_BUCKET}/silver/raiderio_runs",
        expected_schema=silver_raiderio_schema,
    )


# ─── SD-4: silver/dungeon_runs ───────────────────────────────────────────────


@asset_check(
    asset=AssetKey(["orakel", "silver_dungeon_runs"]),
    description="silver/dungeon_runs schema must contain silver_dungeon_runs_schema",
)
def sd_silver_dungeon_runs_check() -> dict:
    """SD-4: silver_dungeon_runs schema drift."""
    if not settings.CHECK_SCHEMA_DRIFT_ENABLED:
        return AssetCheckResult(
            passed=True, metadata={"disabled": True, "mode": "superset"}
        )
    return _run_sd(
        path=f"s3a://{settings.MINIO_BUCKET}/silver/dungeon_runs",
        expected_schema=silver_dungeon_runs_schema,
    )


# ─── SD-5: silver/player_performance ─────────────────────────────────────────


@asset_check(
    asset=AssetKey(["orakel", "silver_player_performance"]),
    description="silver/player_performance schema must contain silver_player_performance_schema",
)
def sd_silver_player_performance_check() -> dict:
    """SD-5: silver_player_performance schema drift."""
    if not settings.CHECK_SCHEMA_DRIFT_ENABLED:
        return AssetCheckResult(
            passed=True, metadata={"disabled": True, "mode": "superset"}
        )
    return _run_sd(
        path=f"s3a://{settings.MINIO_BUCKET}/silver/player_performance",
        expected_schema=silver_player_performance_schema,
    )


# ─── SD-6: gold KPIs (composite) ─────────────────────────────────────────────
# Each KPI has its own schema.  We perform a per-KPI drift check
# and aggregate into a single composite result.  In superset mode,
# extra columns are allowed; missing or type-mismatched columns
# fail the whole composite.

_GOLD_KPI_SCHEMA_MAP = {
    "kpi_tank_death_clock": gold_kpi_tank_death_clock_schema,
    "kpi_healer_deficit": gold_kpi_healer_deficit_schema,
    "kpi_interrupt_rate": gold_kpi_interrupt_rate_schema,
    "kpi_composition_synergy": gold_kpi_composition_synergy_schema,
}


@asset_check(
    asset=AssetKey(["orakel", "gold_kpi_death_clock"]),
    description=(
        "All gold KPI schemas (death_clock, healer_deficit, "
        "interrupt_rate, composition_synergy) must conform to "
        "their respective StructTypes (composite check)"
    ),
)
def sd_gold_kpis_composite_check() -> dict:
    """SD-6: composite schema-drift check across all 4 gold KPIs.

    Attached to ``gold_kpi_death_clock`` as a representative target —
    the check internally covers all 4 KPIs.  The composite returns
    ``passed=True`` only if every individual KPI passes.
    """
    if not settings.CHECK_SCHEMA_DRIFT_ENABLED:
        return AssetCheckResult(
            passed=True, metadata={"disabled": True, "mode": "superset"}
        )

    spark = get_spark_session("sd_kpis_composite")
    try:
        per_kpi_results: dict[str, bool] = {}
        per_kpi_diffs: dict[str, str] = {}
        any_path_missing = False

        for short, schema in _GOLD_KPI_SCHEMA_MAP.items():
            path = f"s3a://{settings.MINIO_BUCKET}/gold/{short}"
            res = check_schema_drift(
                spark,
                path=path,
                expected_schema=schema,
                season=settings.SEASON,
                mode="superset",
            )
            per_kpi_results[short] = res.passed
            if res.metadata.get("path_not_found") is True:
                any_path_missing = True
            diffs = []
            if res.metadata.get("missing_columns"):
                diffs.append(f"missing={res.metadata['missing_columns']}")
            if res.metadata.get("type_mismatches"):
                diffs.append(
                    f"type_mismatch={res.metadata['type_mismatches']}"
                )
            per_kpi_diffs[short] = ",".join(diffs) if diffs else ""

        composite_passed = all(per_kpi_results.values())
        return AssetCheckResult(
            passed=composite_passed,
            metadata={
                "kpis": ",".join(per_kpi_results.keys()),
                "per_kpi_passed": ",".join(
                    f"{k}={v}" for k, v in per_kpi_results.items()
                ),
                "per_kpi_diffs": ",".join(
                    f"{k}={v}" for k, v in per_kpi_diffs.items() if v
                ),
                "path_not_found": any_path_missing,
            },
        )
    finally:
        spark.stop()


# ─── SD-7: gold/features ─────────────────────────────────────────────────────


@asset_check(
    asset=AssetKey(["orakel", "gold_features"]),
    description="gold/features schema must contain gold_features_schema",
)
def sd_gold_features_check() -> dict:
    """SD-7: gold_features schema drift.

    Uses ``gold_features_schema`` defined in ``orakel.models.schemas``.
    The schema is intentionally a stable subset — the actual feature
    view can grow (one-hot, embeddings) but the public contract columns
    must remain present with stable types.
    """
    if not settings.CHECK_SCHEMA_DRIFT_ENABLED:
        return AssetCheckResult(
            passed=True, metadata={"disabled": True, "mode": "superset"}
        )
    return _run_sd(
        path=f"s3a://{settings.MINIO_BUCKET}/gold/features",
        expected_schema=gold_features_schema,
    )
