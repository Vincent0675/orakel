"""Dagster Definitions — asset graph, asset checks, and schedule.

This module wires together all Dagster assets, asset checks, and the
daily pipeline schedule.  Run with::

    dagster dev -m orakel.pipeline.definitions
"""

from __future__ import annotations

from dagster import (
    Definitions,
    ScheduleDefinition,
    define_asset_job,
)

from orakel.config import settings
from orakel.pipeline.assets.bronze import (
    bronze_rio,
    bronze_wcl,
    check_minio_state,
    match_manifest,
)
from orakel.pipeline.assets.checks import (
    bronze_rio_checks,
    bronze_wcl_checks,
    gold_dim_affix_checks,
    gold_dim_dungeon_checks,
    gold_dim_player_checks,
    gold_dim_spec_checks,
    gold_features_check,
    gold_kpi_death_clock_check,
    gold_kpi_healer_deficit_check,
    gold_kpi_interrupt_rate_check,
    gold_kpi_synergy_check,
    match_manifest_checks,
    silver_dungeon_runs_checks,
    silver_player_performance_checks,
    silver_raiderio_checks,
)
from orakel.pipeline.assets.checks_completeness import (
    cr_bronze_to_silver_dungeon_runs_check,
    cr_bronze_to_silver_rio_check,
    cr_bronze_wcl_to_silver_player_perf_check,
    cr_silver_dungeon_runs_to_gold_features_check,
    cr_silver_rio_to_gold_kpis_composite_check,
)
from orakel.pipeline.assets.checks_referential import (
    ri_gold_dim_dungeon_to_silver_check,
    ri_gold_features_to_silver_check,
    ri_gold_kpi_death_clock_check,
    ri_gold_kpi_healer_deficit_check,
    ri_gold_kpi_interrupt_rate_check,
    ri_silver_dungeon_runs_to_bronze_check,
    ri_silver_raiderio_to_bronze_check,
)
from orakel.pipeline.assets.checks_schema import (
    sd_bronze_rio_check,
    sd_bronze_wcl_check,
    sd_gold_features_check,
    sd_gold_kpis_composite_check,
    sd_silver_dungeon_runs_check,
    sd_silver_player_performance_check,
    sd_silver_raiderio_check,
)
from orakel.pipeline.assets.gold import (
    gold_dim_affix,
    gold_dim_dungeon,
    gold_dim_player,
    gold_dim_spec,
    gold_features,
    gold_kpi_death_clock,
    gold_kpi_healer_deficit,
    gold_kpi_interrupt_rate,
    gold_kpi_synergy,
    ml_model,
)
from orakel.pipeline.assets.silver import (
    silver_dungeon_runs,
    silver_player_performance,
    silver_raiderio,
)

# ─── Jobs & Schedules ──────────────────────────────────────────────────────────

daily_pipeline_job = define_asset_job(
    name="daily_pipeline",
    selection="*",
    description="Full Orakel pipeline: Bronze → Silver → Gold",
)

daily_pipeline_schedule = ScheduleDefinition(
    job=daily_pipeline_job,
    cron_schedule="0 2 * * *",  # Daily at 2 AM
    description=f"Run the full Orakel pipeline daily at 2 AM (season={settings.SEASON})",
)

# ─── Definitions ────────────────────────────────────────────────────────────────

defs = Definitions(
    assets=[
        check_minio_state,
        bronze_rio,
        bronze_wcl,
        match_manifest,
        silver_raiderio,
        silver_dungeon_runs,
        silver_player_performance,
        gold_dim_dungeon,
        gold_dim_player,
        gold_dim_affix,
        gold_dim_spec,
        gold_kpi_death_clock,
        gold_kpi_healer_deficit,
        gold_kpi_interrupt_rate,
        gold_kpi_synergy,
        gold_features,
        ml_model,
    ],
    asset_checks=[
        # Per-asset row-count checks (PR 1)
        bronze_rio_checks,
        bronze_wcl_checks,
        match_manifest_checks,
        silver_raiderio_checks,
        silver_dungeon_runs_checks,
        silver_player_performance_checks,
        gold_dim_dungeon_checks,
        gold_dim_player_checks,
        gold_dim_affix_checks,
        gold_dim_spec_checks,
        gold_kpi_death_clock_check,
        gold_kpi_healer_deficit_check,
        gold_kpi_interrupt_rate_check,
        gold_kpi_synergy_check,
        gold_features_check,
        # Cross-layer referential integrity (PR 2)
        ri_silver_raiderio_to_bronze_check,
        ri_silver_dungeon_runs_to_bronze_check,
        ri_gold_kpi_death_clock_check,
        ri_gold_kpi_healer_deficit_check,
        ri_gold_kpi_interrupt_rate_check,
        ri_gold_features_to_silver_check,
        ri_gold_dim_dungeon_to_silver_check,
        # Cross-layer completeness ratios (PR 2)
        cr_bronze_to_silver_rio_check,
        cr_bronze_to_silver_dungeon_runs_check,
        cr_bronze_wcl_to_silver_player_perf_check,
        cr_silver_rio_to_gold_kpis_composite_check,
        cr_silver_dungeon_runs_to_gold_features_check,
        # Cross-layer schema drift (PR 2)
        sd_bronze_rio_check,
        sd_bronze_wcl_check,
        sd_silver_raiderio_check,
        sd_silver_dungeon_runs_check,
        sd_silver_player_performance_check,
        sd_gold_kpis_composite_check,
        sd_gold_features_check,
    ],
    schedules=[daily_pipeline_schedule],
)