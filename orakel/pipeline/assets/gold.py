"""Gold-layer Dagster assets: KPI aggregations, dimension tables, and ML."""

import logging

from dagster import AssetExecutionContext, AssetKey, MetadataValue, Output, asset

from orakel.config import settings
from orakel.pipeline.io_managers import merge_write
from orakel.utils.minio import get_spark_session

logger = logging.getLogger(__name__)


# ─── Dimension Tables ─────────────────────────────────────────────────────────


@asset(
    key_prefix=["orakel"],
    deps=[AssetKey(["orakel", "silver_raiderio"])],
)
def gold_dim_dungeon(context: AssetExecutionContext) -> Output:
    """Gold dimension: dungeon timers and metadata.

    Wraps ``GoldPipeline.build_dim_dungeon()``.  Lookup fallback is
    deferred to PR3 (Operational Hardening).
    """
    from orakel.pipeline.gold import GoldPipeline

    spark = get_spark_session("gold_dim_dungeon")
    try:
        # write=False: asset handles write via overwrite (dim tables are small)
        dim_df = GoldPipeline.build_dim_dungeon(spark, settings.SEASON, write=False)

        path = f"s3a://{settings.MINIO_BUCKET}/gold/dim_dungeon"
        row_count = dim_df.count()
        # Dimension tables use overwrite (small, regenerated each run)
        dim_df.write.mode("overwrite").parquet(path)
        context.log.info("gold_dim_dungeon: %d rows", row_count)
        return Output(
            value=row_count,
            metadata={
                "row_count": MetadataValue.int(row_count),
                "season": MetadataValue.text(settings.SEASON),
            },
        )
    finally:
        spark.stop()


@asset(
    key_prefix=["orakel"],
    deps=[AssetKey(["orakel", "silver_raiderio"])],
)
def gold_dim_player(context: AssetExecutionContext) -> Output:
    """Gold dimension: player dimension from Silver Raider.IO data.

    Wraps ``GoldPipeline.build_dim_player()``.
    """
    from orakel.pipeline.gold import GoldPipeline

    spark = get_spark_session("gold_dim_player")
    try:
        dim_df = GoldPipeline.build_dim_player(spark, settings.SEASON, write=False)

        path = f"s3a://{settings.MINIO_BUCKET}/gold/dim_player"
        row_count = dim_df.count()
        dim_df.write.mode("overwrite").parquet(path)
        context.log.info("gold_dim_player: %d rows", row_count)
        return Output(
            value=row_count,
            metadata={
                "row_count": MetadataValue.int(row_count),
                "season": MetadataValue.text(settings.SEASON),
            },
        )
    finally:
        spark.stop()


@asset(
    key_prefix=["orakel"],
    deps=[AssetKey(["orakel", "silver_raiderio"])],
)
def gold_dim_affix(context: AssetExecutionContext) -> Output:
    """Gold dimension: affix metadata (hardcoded for MVP).

    Wraps ``GoldPipeline.build_dim_affix()``.  Lookup fallback is
    deferred to PR3 (Operational Hardening).
    """
    from orakel.pipeline.gold import GoldPipeline

    spark = get_spark_session("gold_dim_affix")
    try:
        dim_df = GoldPipeline.build_dim_affix(spark, settings.SEASON, write=False)

        path = f"s3a://{settings.MINIO_BUCKET}/gold/dim_affix"
        row_count = dim_df.count()
        dim_df.write.mode("overwrite").parquet(path)
        context.log.info("gold_dim_affix: %d rows", row_count)
        return Output(
            value=row_count,
            metadata={
                "row_count": MetadataValue.int(row_count),
                "season": MetadataValue.text(settings.SEASON),
            },
        )
    finally:
        spark.stop()


@asset(
    key_prefix=["orakel"],
    deps=[AssetKey(["orakel", "silver_raiderio"])],
)
def gold_dim_spec(context: AssetExecutionContext) -> Output:
    """Gold dimension: spec-role mapping (hardcoded for MVP).

    Wraps ``GoldPipeline.build_dim_spec()``.  Lookup fallback is
    deferred to PR3 (Operational Hardening).
    """
    from orakel.pipeline.gold import GoldPipeline

    spark = get_spark_session("gold_dim_spec")
    try:
        dim_df = GoldPipeline.build_dim_spec(spark, write=False)

        path = f"s3a://{settings.MINIO_BUCKET}/gold/dim_spec"
        row_count = dim_df.count()
        dim_df.write.mode("overwrite").parquet(path)
        context.log.info("gold_dim_spec: %d rows", row_count)
        return Output(
            value=row_count,
            metadata={
                "row_count": MetadataValue.int(row_count),
                "season": MetadataValue.text(settings.SEASON),
            },
        )
    finally:
        spark.stop()


# ─── KPI Assets ────────────────────────────────────────────────────────────────


@asset(
    key_prefix=["orakel"],
    deps=[AssetKey(["orakel", "silver_player_performance"])],
)
def gold_kpi_death_clock(
    context: AssetExecutionContext,
) -> Output:
    """Gold KPI 1: Tank Death Clock — seconds until tank death.

    Wraps ``GoldPipeline.compute_kpi_death_clock()``.
    Uses merge_write for incremental upsert by ``run_id`` + ``tank_name``.
    """
    from orakel.pipeline.gold import GoldPipeline

    spark = get_spark_session("gold_kpi_death_clock")
    try:
        kpi_df = GoldPipeline.compute_kpi_death_clock(spark, settings.SEASON, write=False)

        path = f"s3a://{settings.MINIO_BUCKET}/gold/kpi_tank_death_clock"
        row_count = merge_write(spark, kpi_df, path, merge_key=["run_id", "tank_name"])
        context.log.info("gold_kpi_death_clock: %d rows (merged)", row_count)
        return Output(
            value=row_count,
            metadata={
                "row_count": MetadataValue.int(row_count),
                "season": MetadataValue.text(settings.SEASON),
            },
        )
    finally:
        spark.stop()


@asset(
    key_prefix=["orakel"],
    deps=[AssetKey(["orakel", "silver_player_performance"])],
)
def gold_kpi_healer_deficit(
    context: AssetExecutionContext,
) -> Output:
    """Gold KPI 2: Healer Deficit — tank DTPS / healer HPS ratio.

    Wraps ``GoldPipeline.compute_kpi_healer_deficit()``.
    Uses merge_write for incremental upsert by ``run_id`` + ``healer_name``.
    """
    from orakel.pipeline.gold import GoldPipeline

    spark = get_spark_session("gold_kpi_healer_deficit")
    try:
        kpi_df = GoldPipeline.compute_kpi_healer_deficit(spark, settings.SEASON, write=False)

        path = f"s3a://{settings.MINIO_BUCKET}/gold/kpi_healer_deficit"
        row_count = merge_write(spark, kpi_df, path, merge_key=["run_id", "healer_name"])
        context.log.info("gold_kpi_healer_deficit: %d rows (merged)", row_count)
        return Output(
            value=row_count,
            metadata={
                "row_count": MetadataValue.int(row_count),
                "season": MetadataValue.text(settings.SEASON),
            },
        )
    finally:
        spark.stop()


@asset(
    key_prefix=["orakel"],
    deps=[AssetKey(["orakel", "silver_player_performance"])],
)
def gold_kpi_interrupt_rate(
    context: AssetExecutionContext,
) -> Output:
    """Gold KPI 3: Interrupt Rate — interrupts per minute per player.

    Wraps ``GoldPipeline.compute_kpi_interrupt_rate()``.
    Uses merge_write for incremental upsert by ``run_id`` + ``player_name``.
    """
    from orakel.pipeline.gold import GoldPipeline

    spark = get_spark_session("gold_kpi_interrupt_rate")
    try:
        kpi_df = GoldPipeline.compute_kpi_interrupt_rate(spark, settings.SEASON, write=False)

        path = f"s3a://{settings.MINIO_BUCKET}/gold/kpi_interrupt_rate"
        row_count = merge_write(
            spark, kpi_df, path, merge_key=["run_id", "player_name"]
        )
        context.log.info("gold_kpi_interrupt_rate: %d rows (merged)", row_count)
        return Output(
            value=row_count,
            metadata={
                "row_count": MetadataValue.int(row_count),
                "season": MetadataValue.text(settings.SEASON),
            },
        )
    finally:
        spark.stop()


@asset(
    key_prefix=["orakel"],
    deps=[AssetKey(["orakel", "silver_raiderio"])],
)
def gold_kpi_synergy(
    context: AssetExecutionContext,
) -> Output:
    """Gold KPI 4: Composition Synergy Score.

    Wraps ``GoldPipeline.compute_kpi_synergy()``.
    Uses overwrite (grouped aggregation — regenerated from scratch each run).
    """
    from orakel.pipeline.gold import GoldPipeline

    spark = get_spark_session("gold_kpi_synergy")
    try:
        kpi_df = GoldPipeline.compute_kpi_synergy(spark, settings.SEASON, write=False)

        # Synergy uses grouped aggregation — overwrite is appropriate
        path = f"s3a://{settings.MINIO_BUCKET}/gold/kpi_composition_synergy"
        row_count = kpi_df.count()
        kpi_df.write.mode("overwrite").parquet(path)
        context.log.info("gold_kpi_synergy: %d rows", row_count)
        return Output(
            value=row_count,
            metadata={
                "row_count": MetadataValue.int(row_count),
                "season": MetadataValue.text(settings.SEASON),
            },
        )
    finally:
        spark.stop()


# ─── ML Feature Engineering ────────────────────────────────────────────────────


@asset(
    key_prefix=["orakel"],
    deps=[
        AssetKey(["orakel", "gold_kpi_death_clock"]),
        AssetKey(["orakel", "gold_kpi_healer_deficit"]),
        AssetKey(["orakel", "gold_kpi_interrupt_rate"]),
        AssetKey(["orakel", "gold_kpi_synergy"]),
    ],
)
def gold_features(
    context: AssetExecutionContext,
) -> Output:
    """Gold feature view for ML model training.

    Joins all Gold KPIs + silver_dungeon_runs on ``run_id``, computes
    derived features (log1p, role counts, affix flags, dungeon one-hot),
    and writes to ``gold/features/`` partitioned by season and key_level.

    Wraps ``orakel.ml.features.build_feature_view()``.
    """
    from orakel.ml.features import build_feature_view

    spark = get_spark_session("gold_features")
    try:
        result_df = build_feature_view(spark, settings.SEASON)
        row_count = result_df.count()
        context.log.info("gold_features: %d rows written", row_count)

        return Output(
            value=row_count,
            metadata={
                "row_count": MetadataValue.int(row_count),
                "season": MetadataValue.text(settings.SEASON),
            },
        )
    finally:
        spark.stop()


# ─── ML Model Training ──────────────────────────────────────────────────────────


@asset(
    key_prefix=["orakel"],
    deps=[AssetKey(["orakel", "gold_features"])],
)
def ml_model(context: AssetExecutionContext) -> Output:
    """Train Ridge regression model on Gold feature view.

    Reads feature view Parquet, converts to pandas, performs temporal
    80/20 split, and trains a Ridge model with MLflow tracking.

    If < 10 runs available, the asset is **skipped** with a message
    ``"Datos insuficientes: N < 10 runs"``.
    """
    spark = get_spark_session("ml_model")
    try:
        # Read feature view from Parquet
        features_path = f"s3a://{settings.MINIO_BUCKET}/gold/features"
        from pyspark.sql import functions as F

        feature_df = spark.read.parquet(features_path).filter(
            F.col("season") == settings.SEASON
        )

        row_count = feature_df.count()
        if row_count < 10:
            context.log.warning("Datos insuficientes: %d < 10 runs. Skipping model training.", row_count)
            return Output(
                value=0,
                metadata={
                    "skipped": MetadataValue.bool(True),
                    "reason": MetadataValue.text(f"Datos insuficientes: {row_count} < 10 runs"),
                    "row_count": MetadataValue.int(row_count),
                },
            )

        # Convert to pandas for sklearn
        pdf = feature_df.toPandas()

        from orakel.ml.trainer import train_model

        model, metrics = train_model(pdf)

        if metrics.get("skipped", False):
            return Output(
                value=0,
                metadata={
                    "skipped": MetadataValue.bool(True),
                    "reason": MetadataValue.text(metrics.get("reason", "Unknown")),
                    "row_count": MetadataValue.int(metrics.get("total_rows", 0)),
                },
            )

        context.log.info(
            "ML model trained: MAE=%.2f, RMSE=%.2f, R²=%.4f",
            metrics["mae"],
            metrics["rmse"],
            metrics["r2"],
        )

        metadata = {
            "mae": MetadataValue.float(metrics["mae"]),
            "rmse": MetadataValue.float(metrics["rmse"]),
            "r2": MetadataValue.float(metrics["r2"]),
            "baseline_mae": MetadataValue.float(metrics["baseline_mae"]),
            "baseline_r2": MetadataValue.float(metrics["baseline_r2"]),
            "train_size": MetadataValue.int(metrics["train_size"]),
            "test_size": MetadataValue.int(metrics["test_size"]),
            "total_rows": MetadataValue.int(metrics["total_rows"]),
            "mlflow_run_id": MetadataValue.text(metrics.get("mlflow_run_id", "unknown")),
            "skipped": MetadataValue.bool(False),
        }
        if metrics.get("minio_model_path"):
            metadata["minio_model_path"] = MetadataValue.text(metrics["minio_model_path"])

        return Output(
            value=1,
            metadata=metadata,
        )
    finally:
        spark.stop()