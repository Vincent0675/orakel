"""Cross-layer completeness ratio @asset_check wrappers.

Each wrapper is a thin Dagster entry-point that delegates to the
reusable ``check_completeness_ratio`` core function (in
``orakel.pipeline.assets.checks``).  See design.md, "Completeness Ratio
Pairs" table for thresholds.

Pairs (SDD: Verification-Dagster-Orchestation, Phase 7 / CR table):

    CR-1 bronze/raiderio/runs       -> silver/raiderio_runs       (>= 0.5)
    CR-2 bronze/raiderio/runs       -> silver/dungeon_runs        (>= 0.3)
    CR-3 bronze/wcl                 -> silver/player_performance  (>= 0.5)
    CR-4 silver/raiderio_runs       -> gold KPIs composite        (>= 0.8)
    CR-5 silver/dungeon_runs        -> gold/features              (>= 0.8)

All wrappers are warning-only.  Empty upstream is treated as a
non-failure (operator signal), not a ratio violation.
"""

from __future__ import annotations

import logging

from dagster import AssetCheckResult, AssetKey, asset_check

from orakel.config import settings
from orakel.pipeline.assets.checks import check_completeness_ratio
from orakel.utils.minio import get_spark_session

logger = logging.getLogger(__name__)


def _run_cr(
    upstream_path: str,
    downstream_path: str,
    min_ratio: float,
    season: str | None = None,
) -> AssetCheckResult:
    """Acquire a SparkSession, run the completeness check, and tear down."""
    spark = get_spark_session("cr_check")
    try:
        return check_completeness_ratio(
            spark,
            upstream_path=upstream_path,
            downstream_path=downstream_path,
            season=season or settings.SEASON,
            min_ratio=min_ratio,
        )
    finally:
        spark.stop()


# ─── CR-1: bronze_rio -> silver_raiderio ─────────────────────────────────────


@asset_check(
    asset=AssetKey(["orakel", "silver_raiderio"]),
    description="silver_raiderio row count >= 50% of bronze_rio",
)
def cr_bronze_to_silver_rio_check() -> dict:
    """CR-1: bronze_rio -> silver_raiderio, min_ratio=0.5.

    Threshold is 50% because Silver applies dedup + null filtering;
    we expect some loss.  Tune up after observing production.
    """
    if not settings.CHECK_COMPLETENESS_ENABLED:
        return AssetCheckResult(
            passed=True, metadata={"disabled": True, "min_ratio": 0.5}
        )
    return _run_cr(
        upstream_path=f"s3a://{settings.MINIO_BUCKET}/bronze/raiderio/runs",
        downstream_path=f"s3a://{settings.MINIO_BUCKET}/silver/raiderio_runs",
        min_ratio=0.5,
    )


# ─── CR-2: bronze_rio -> silver_dungeon_runs ────────────────────────────────


@asset_check(
    asset=AssetKey(["orakel", "silver_dungeon_runs"]),
    description="silver_dungeon_runs row count >= 30% of bronze_rio",
)
def cr_bronze_to_silver_dungeon_runs_check() -> dict:
    """CR-2: bronze_rio -> silver_dungeon_runs, min_ratio=0.3.

    Threshold is 30% because the fuzzy join between Raider.IO and WCL
    is lossy; many bronze runs will not have a matching WCL report.
    """
    if not settings.CHECK_COMPLETENESS_ENABLED:
        return AssetCheckResult(
            passed=True, metadata={"disabled": True, "min_ratio": 0.3}
        )
    return _run_cr(
        upstream_path=f"s3a://{settings.MINIO_BUCKET}/bronze/raiderio/runs",
        downstream_path=f"s3a://{settings.MINIO_BUCKET}/silver/dungeon_runs",
        min_ratio=0.3,
    )


# ─── CR-3: bronze_wcl -> silver_player_performance ───────────────────────────


@asset_check(
    asset=AssetKey(["orakel", "silver_player_performance"]),
    description="silver_player_performance row count >= 50% of bronze_wcl",
)
def cr_bronze_wcl_to_silver_player_perf_check() -> dict:
    """CR-3: bronze_wcl -> silver_player_performance, min_ratio=0.5.

    Player performance is fan-out from WCL fights (5 players per fight),
    so 50% is achievable.  WCL itself may have 0 rows (graceful), in
    which case the check passes with ``upstream_empty=True``.
    """
    if not settings.CHECK_COMPLETENESS_ENABLED:
        return AssetCheckResult(
            passed=True, metadata={"disabled": True, "min_ratio": 0.5}
        )
    return _run_cr(
        upstream_path=f"s3a://{settings.MINIO_BUCKET}/bronze/wcl",
        downstream_path=f"s3a://{settings.MINIO_BUCKET}/silver/player_performance",
        min_ratio=0.5,
    )


# ─── CR-4: silver_raiderio -> gold KPIs (composite) ──────────────────────────
# Composite check: averages the ratio of each individual KPI against
# silver_raiderio.  Individual KPIs (e.g. one per tank role) may be
# sparse, so the average gives a more stable signal than any single KPI.

_GOLD_KPI_RIOS = {
    "death_clock": "kpi_tank_death_clock",
    "healer_deficit": "kpi_healer_deficit",
    "interrupt_rate": "kpi_interrupt_rate",
    "composition_synergy": "kpi_composition_synergy",
    "role_flexibility": "kpi_role_flexibility",
}


@asset_check(
    asset=AssetKey(["orakel", "gold_kpi_death_clock"]),
    description="Average gold KPI ratio against silver_raiderio >= 80% (composite)",
)
def cr_silver_rio_to_gold_kpis_composite_check() -> dict:
    """CR-4: composite of silver_raiderio -> all 5 gold KPIs.

    Averages downstream/upstream ratio across all 5 gold KPIs to give
    a single, more stable signal.  If individual KPIs (tank-only,
    healer-only) are sparse, the average smooths that out.

    Note: the user-facing check is attached to ``gold_kpi_death_clock``
    as a representative gold KPI.  The composite covers all 5 KPIs
    internally.
    """
    if not settings.CHECK_COMPLETENESS_ENABLED:
        return AssetCheckResult(
            passed=True, metadata={"disabled": True, "min_ratio": 0.8}
        )

    spark = get_spark_session("cr_composite")
    try:
        upstream_path = f"s3a://{settings.MINIO_BUCKET}/silver/raiderio_runs"
        season = settings.SEASON
        ratios: list[float] = []
        per_kpi: dict[str, float] = {}
        upstream_count: int | None = None

        for short, full_path in _GOLD_KPI_RIOS.items():
            downstream_path = (
                f"s3a://{settings.MINIO_BUCKET}/gold/{full_path}"
            )
            res = check_completeness_ratio(
                spark,
                upstream_path=upstream_path,
                downstream_path=downstream_path,
                season=season,
                min_ratio=0.8,
            )
            # Each call returns AssetCheckResult — surface metadata
            per_kpi[short] = float(res.metadata.get("ratio", 0.0))
            if res.metadata.get("upstream_empty") is True:
                # If upstream is empty, all KPIs report 0.0; bail early
                return AssetCheckResult(
                    passed=True,
                    metadata={
                        "composite_ratio": 0.0,
                        "upstream_count": 0,
                        "kpis": ",".join(_GOLD_KPI_RIOS.keys()),
                        "upstream_empty": True,
                        "min_ratio": 0.8,
                    },
                )
            ratios.append(float(res.metadata.get("ratio", 0.0)))
            if upstream_count is None:
                upstream_count = int(res.metadata.get("upstream_count", 0))

        composite = sum(ratios) / len(ratios) if ratios else 0.0
        return AssetCheckResult(
            passed=composite >= 0.8,
            metadata={
                "composite_ratio": round(composite, 4),
                "min_ratio": 0.8,
                "kpis": ",".join(per_kpi.keys()),
                "per_kpi_ratio": ",".join(
                    f"{k}={v:.2f}" for k, v in per_kpi.items()
                ),
                "upstream_count": upstream_count or 0,
            },
        )
    finally:
        spark.stop()


# ─── CR-5: silver_dungeon_runs -> gold_features ─────────────────────────────


@asset_check(
    asset=AssetKey(["orakel", "gold_features"]),
    description="gold_features row count >= 80% of silver_dungeon_runs",
)
def cr_silver_dungeon_runs_to_gold_features_check() -> dict:
    """CR-5: silver_dungeon_runs -> gold_features, min_ratio=0.8.

    Feature view joins all KPIs + silver on run_id; expect to keep
    most rows.  80% is conservative.
    """
    if not settings.CHECK_COMPLETENESS_ENABLED:
        return AssetCheckResult(
            passed=True, metadata={"disabled": True, "min_ratio": 0.8}
        )
    return _run_cr(
        upstream_path=f"s3a://{settings.MINIO_BUCKET}/silver/dungeon_runs",
        downstream_path=f"s3a://{settings.MINIO_BUCKET}/gold/features",
        min_ratio=0.8,
    )
