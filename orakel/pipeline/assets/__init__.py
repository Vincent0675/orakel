"""Dagster asset definitions — re-exported for Definitions."""

from orakel.pipeline.assets.bronze import (
    bronze_rio,
    bronze_wcl,
    check_minio_state,
    match_manifest,
)
from orakel.pipeline.assets.checks import (
    bronze_rio_checks,
    gold_features_check,
    gold_kpi_death_clock_check,
    gold_kpi_healer_deficit_check,
    gold_kpi_interrupt_rate_check,
    gold_kpi_synergy_check,
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

__all__ = [
    "check_minio_state",
    "bronze_rio",
    "bronze_wcl",
    "match_manifest",
    "silver_raiderio",
    "silver_dungeon_runs",
    "silver_player_performance",
    "gold_dim_dungeon",
    "gold_dim_player",
    "gold_dim_affix",
    "gold_dim_spec",
    "gold_kpi_death_clock",
    "gold_kpi_healer_deficit",
    "gold_kpi_interrupt_rate",
    "gold_kpi_synergy",
    "gold_features",
    "ml_model",
    "bronze_rio_checks",
    "silver_raiderio_checks",
    "silver_dungeon_runs_checks",
    "silver_player_performance_checks",
    "gold_kpi_death_clock_check",
    "gold_kpi_healer_deficit_check",
    "gold_kpi_interrupt_rate_check",
    "gold_kpi_synergy_check",
    "gold_features_check",
]