"""Cross-layer referential integrity @asset_check wrappers.

Each wrapper is a thin Dagster entry-point that delegates to the reusable
``check_referential_integrity`` core function (defined in
``orakel.pipeline.assets.checks``).  Wrappers exist to:

  1. Provide a stable @asset_check target for Dagster's UI.
  2. Capture the (upstream, downstream, join_key) tuple in code so
     re-wiring a check is a one-line change here, not a refactor of the
     reusable function.
  3. Keep path and key strings consistent with the asset layout in
     ``orakel/pipeline/assets/{bronze,silver,gold}.py``.

Warning-only: failures do not block downstream assets (see design.md,
"Warning-only on failure" decision).  Promote to ``severity=BLOCKING``
after the PR 2 validation period stabilizes thresholds.

Pairs (SDD: Verification-Dagster-Orchestation, Phase 6 / RI table):

    RI-1 silver_raiderio.run_id     -> bronze_rio.run_id (keystone_run_id)
    RI-2 silver_dungeon_runs.run_id -> bronze_rio.run_id (keystone_run_id)
    RI-3 gold KPIs (5)              -> silver run_id    (composite)
    RI-4 gold_features.run_id       -> silver run_id
    RI-5 gold_dim_dungeon.dungeon_id-> silver run_id
"""

from __future__ import annotations

import logging

from dagster import AssetCheckResult, AssetKey, asset_check

from orakel.config import settings
from orakel.pipeline.assets.checks import check_referential_integrity

logger = logging.getLogger(__name__)


def _run_ri(
    spark,
    upstream_path: str,
    downstream_path: str,
    join_key: str,
    sample_size: int | None = None,
    season: str | None = None,
) -> AssetCheckResult:
    """Run the referential-integrity check using the caller-supplied SparkSession.

    ``spark`` is provided by the Dagster ``spark_resource`` so this helper
    no longer creates or tears down a session of its own.
    """
    return check_referential_integrity(
        spark,
        upstream_path=upstream_path,
        downstream_path=downstream_path,
        join_key=join_key,
        season=season or settings.SEASON,
        sample_size=sample_size or settings.CHECK_RI_SAMPLE_SIZE,
    )


# ─── RI-1: silver_raiderio -> bronze_rio (keystone_run_id) ───────────────────


@asset_check(
    asset=AssetKey(["orakel", "silver_raiderio"]),
    description="silver_raiderio.run_id must exist in bronze_rio.keystone_run_id",
    required_resource_keys={"spark"},
)
def ri_silver_raiderio_to_bronze_check(context) -> dict:
    """RI-1: silver_raiderio -> bronze_rio on keystone_run_id.

    Bronze is small — full scan (no sampling).
    """
    if not settings.CHECK_RI_ENABLED:
        return AssetCheckResult(
            passed=True,
            metadata={"disabled": True, "join_key": "keystone_run_id"},
        )
    return _run_ri(
        context.resources.spark,
        upstream_path=f"s3a://{settings.MINIO_BUCKET}/bronze/raiderio/runs",
        downstream_path=f"s3a://{settings.MINIO_BUCKET}/silver/raiderio_runs",
        join_key="keystone_run_id",
    )


# ─── RI-2: silver_dungeon_runs -> bronze_rio (keystone_run_id) ──────────────


@asset_check(
    asset=AssetKey(["orakel", "silver_dungeon_runs"]),
    description="silver_dungeon_runs.rio_run_id must exist in bronze_rio.keystone_run_id",
    required_resource_keys={"spark"},
)
def ri_silver_dungeon_runs_to_bronze_check(context) -> dict:
    """RI-2: silver_dungeon_runs -> bronze_rio on keystone_run_id.

    Joins on ``rio_run_id`` (silver) which is the cast/long-form of
    bronze's ``keystone_run_id``.  The reusable function performs the
    left-anti join on the named column.
    """
    if not settings.CHECK_RI_ENABLED:
        return AssetCheckResult(
            passed=True,
            metadata={"disabled": True, "join_key": "keystone_run_id"},
        )
    return _run_ri(
        context.resources.spark,
        upstream_path=f"s3a://{settings.MINIO_BUCKET}/bronze/raiderio/runs",
        downstream_path=f"s3a://{settings.MINIO_BUCKET}/silver/dungeon_runs",
        join_key="keystone_run_id",
    )


# ─── RI-3: gold KPIs -> silver (run_id) ─────────────────────────────────────
# All five gold KPIs (death_clock, healer_deficit, interrupt_rate,
# composition_synergy, role_flexibility) trace back to silver on run_id.
# Each KPI has its own @asset_check so Dagster can surface per-asset
# orphan counts.  Synergy's upstream is silver_raiderio (grouped agg);
# the other four are silver_dungeon_runs / silver_player_performance.

_GOLD_KPI_PATHS = {
    "death_clock": "kpi_tank_death_clock",
    "healer_deficit": "kpi_healer_deficit",
    "interrupt_rate": "kpi_interrupt_rate",
    "composition_synergy": "kpi_composition_synergy",
    "role_flexibility": "kpi_role_flexibility",
}


@asset_check(
    asset=AssetKey(["orakel", "gold_kpi_death_clock"]),
    description="gold_kpi_death_clock.run_id must exist in silver_dungeon_runs.run_id",
    required_resource_keys={"spark"},
)
def ri_gold_kpi_death_clock_check(context) -> dict:
    """RI-3a: gold_kpi_death_clock -> silver_dungeon_runs on run_id."""
    if not settings.CHECK_RI_ENABLED:
        return AssetCheckResult(
            passed=True, metadata={"disabled": True, "join_key": "run_id"}
        )
    return _run_ri(
        context.resources.spark,
        upstream_path=f"s3a://{settings.MINIO_BUCKET}/silver/dungeon_runs",
        downstream_path=f"s3a://{settings.MINIO_BUCKET}/gold/{_GOLD_KPI_PATHS['death_clock']}",
        join_key="run_id",
    )


@asset_check(
    asset=AssetKey(["orakel", "gold_kpi_healer_deficit"]),
    description="gold_kpi_healer_deficit.run_id must exist in silver_player_performance.run_id",
    required_resource_keys={"spark"},
)
def ri_gold_kpi_healer_deficit_check(context) -> dict:
    """RI-3b: gold_kpi_healer_deficit -> silver_player_performance on run_id."""
    if not settings.CHECK_RI_ENABLED:
        return AssetCheckResult(
            passed=True, metadata={"disabled": True, "join_key": "run_id"}
        )
    return _run_ri(
        context.resources.spark,
        upstream_path=f"s3a://{settings.MINIO_BUCKET}/silver/player_performance",
        downstream_path=f"s3a://{settings.MINIO_BUCKET}/gold/{_GOLD_KPI_PATHS['healer_deficit']}",
        join_key="run_id",
    )


@asset_check(
    asset=AssetKey(["orakel", "gold_kpi_interrupt_rate"]),
    description="gold_kpi_interrupt_rate.run_id must exist in silver_player_performance.run_id",
    required_resource_keys={"spark"},
)
def ri_gold_kpi_interrupt_rate_check(context) -> dict:
    """RI-3c: gold_kpi_interrupt_rate -> silver_player_performance on run_id.

    Included per spec table line 13 (SDD: Verification-Dagster-Orchestation).
    """
    if not settings.CHECK_RI_ENABLED:
        return AssetCheckResult(
            passed=True, metadata={"disabled": True, "join_key": "run_id"}
        )
    return _run_ri(
        context.resources.spark,
        upstream_path=f"s3a://{settings.MINIO_BUCKET}/silver/player_performance",
        downstream_path=f"s3a://{settings.MINIO_BUCKET}/gold/{_GOLD_KPI_PATHS['interrupt_rate']}",
        join_key="run_id",
    )


# ─── RI-4: gold_features -> silver_dungeon_runs (run_id) ─────────────────────


@asset_check(
    asset=AssetKey(["orakel", "gold_features"]),
    description="gold_features.run_id must exist in silver_dungeon_runs.run_id",
    required_resource_keys={"spark"},
)
def ri_gold_features_to_silver_check(context) -> dict:
    """RI-4: gold_features -> silver_dungeon_runs on run_id.

    Note: gold_features is an ML artifact, but its run_id propagation is
    a pipeline-integrity concern, not a model-validation concern.  See
    design.md "Open Questions" resolved decision.
    """
    if not settings.CHECK_RI_ENABLED:
        return AssetCheckResult(
            passed=True, metadata={"disabled": True, "join_key": "run_id"}
        )
    return _run_ri(
        context.resources.spark,
        upstream_path=f"s3a://{settings.MINIO_BUCKET}/silver/dungeon_runs",
        downstream_path=f"s3a://{settings.MINIO_BUCKET}/gold/features",
        join_key="run_id",
    )


# ─── RI-5: gold_dim_dungeon.dungeon_id -> silver_dungeon_runs (dungeon_id) ──


@asset_check(
    asset=AssetKey(["orakel", "gold_dim_dungeon"]),
    description="gold_dim_dungeon.dungeon_id must exist in silver_dungeon_runs.dungeon_id",
    required_resource_keys={"spark"},
)
def ri_gold_dim_dungeon_to_silver_check(context) -> dict:
    """RI-5: gold_dim_dungeon -> silver_dungeon_runs on dungeon_id.

    Dim table is small — full scan.  Spark auto-broadcasts the join
    since dim_dungeon << silver_dungeon_runs.
    """
    if not settings.CHECK_RI_ENABLED:
        return AssetCheckResult(
            passed=True, metadata={"disabled": True, "join_key": "dungeon_id"}
        )
    return _run_ri(
        context.resources.spark,
        upstream_path=f"s3a://{settings.MINIO_BUCKET}/silver/dungeon_runs",
        downstream_path=f"s3a://{settings.MINIO_BUCKET}/gold/dim_dungeon",
        join_key="dungeon_id",
    )
