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
    ],
    schedules=[daily_pipeline_schedule],
)