"""AssetCheck definitions — data quality gates per layer.

Each check validates row counts and null percentages on critical columns.
 """

import logging

from dagster import AssetKey, asset_check

from orakel.config import settings
from orakel.utils.minio import get_spark_session

logger = logging.getLogger(__name__)


def _read_parquet(path: str):
    """Read a Parquet path from MinIO and return (spark, df) or (spark, None)."""
    from pyspark.errors import AnalysisException

    spark = get_spark_session("asset_check")
    try:
        df = spark.read.parquet(path)
        return spark, df
    except AnalysisException:
        logger.warning("Path %s does not exist — check will return warning.", path)
        return spark, None
    except Exception:
        # Path may not exist yet or S3A connection failed
        return spark, None


# ─── Bronze Checks ──────────────────────────────────────────────────────────────


@asset_check(
    asset=AssetKey(["orakel", "bronze_rio"]),
    description="Bronze Raider.IO: row_count >= 1, null % on keystone_run_id < 1%",
)
def bronze_rio_checks() -> dict:
    """Check bronze_rio data quality."""
    spark = get_spark_session("check_bronze_rio")
    try:
        path = f"s3a://{settings.MINIO_BUCKET}/bronze/raiderio/runs"
        try:
            df = spark.read.parquet(path).filter(
                __import__("pyspark.sql.functions", fromlist=["col"]).col("season") == settings.SEASON
            )
        except Exception:
            from dagster import AssetCheckResult

            return AssetCheckResult(
                passed=False,
                metadata={"error": "Bronze Raider.IO data not found"},
            )

        row_count = df.count()
        if row_count < 1:
            from dagster import AssetCheckResult

            return AssetCheckResult(
                passed=False,
                metadata={"row_count": row_count, "error": "Expected >= 1 row"},
            )

        # Check null % on keystone_run_id
        null_count = df.filter(df["keystone_run_id"].isNull()).count()
        null_pct = null_count / row_count if row_count > 0 else 1.0

        from dagster import AssetCheckResult

        return AssetCheckResult(
            passed=null_pct < 0.01,
            metadata={
                "row_count": row_count,
                "null_pct_keystone_run_id": f"{null_pct:.4f}",
            },
        )
    finally:
        spark.stop()


# ─── Silver Checks ──────────────────────────────────────────────────────────────


@asset_check(
    asset=AssetKey(["orakel", "silver_raiderio"]),
    description="Silver Raider.IO: row_count >= 1",
)
def silver_raiderio_checks() -> dict:
    """Check silver_raiderio data quality."""
    from pyspark.sql import functions as F

    from dagster import AssetCheckResult

    spark = get_spark_session("check_silver_raiderio")
    try:
        path = f"s3a://{settings.MINIO_BUCKET}/silver/raiderio_runs"
        try:
            df = spark.read.parquet(path).filter(F.col("season") == settings.SEASON)
        except Exception:
            return AssetCheckResult(
                passed=False,
                metadata={"error": "Silver Raider.IO data not found"},
            )

        row_count = df.count()
        return AssetCheckResult(
            passed=row_count >= 1,
            metadata={"row_count": row_count},
        )
    finally:
        spark.stop()


@asset_check(
    asset=AssetKey(["orakel", "silver_dungeon_runs"]),
    description="Silver dungeon_runs: row_count >= 1, no null dungeon_id",
)
def silver_dungeon_runs_checks() -> dict:
    """Check silver_dungeon_runs data quality."""
    from pyspark.sql import functions as F

    from dagster import AssetCheckResult

    spark = get_spark_session("check_silver_dungeon_runs")
    try:
        path = f"s3a://{settings.MINIO_BUCKET}/silver/dungeon_runs"
        try:
            df = spark.read.parquet(path).filter(F.col("season") == settings.SEASON)
        except Exception:
            return AssetCheckResult(
                passed=False,
                metadata={"error": "Silver dungeon_runs data not found"},
            )

        row_count = df.count()
        null_dungeon_id = df.filter(F.col("dungeon_id").isNull()).count()

        return AssetCheckResult(
            passed=(row_count >= 1 and null_dungeon_id == 0),
            metadata={
                "row_count": row_count,
                "null_dungeon_id_count": null_dungeon_id,
            },
        )
    finally:
        spark.stop()


@asset_check(
    asset=AssetKey(["orakel", "silver_player_performance"]),
    description="Silver player_performance: row_count >= 5",
)
def silver_player_performance_checks() -> dict:
    """Check silver_player_performance data quality."""
    from pyspark.sql import functions as F

    from dagster import AssetCheckResult

    spark = get_spark_session("check_silver_player_perf")
    try:
        path = f"s3a://{settings.MINIO_BUCKET}/silver/player_performance"
        try:
            df = spark.read.parquet(path).filter(F.col("season") == settings.SEASON)
        except Exception:
            return AssetCheckResult(
                passed=False,
                metadata={"error": "Silver player_performance data not found"},
            )

        row_count = df.count()
        return AssetCheckResult(
            passed=row_count >= 5,
            metadata={"row_count": row_count},
        )
    finally:
        spark.stop()


# ─── Gold KPI Checks ────────────────────────────────────────────────────────────


@asset_check(
    asset=AssetKey(["orakel", "gold_kpi_death_clock"]),
    description="Gold KPI death_clock: row_count >= 1, null % on primary metric < 50%",
)
def gold_kpi_death_clock_check() -> dict:
    """Check gold_kpi_death_clock data quality."""
    from pyspark.sql import functions as F

    from dagster import AssetCheckResult

    spark = get_spark_session("check_gold_kpi_death_clock")
    try:
        path = f"s3a://{settings.MINIO_BUCKET}/gold/kpi_tank_death_clock"
        try:
            df = spark.read.parquet(path)
        except Exception:
            return AssetCheckResult(
                passed=False,
                metadata={"error": "Gold KPI death_clock data not found"},
            )

        row_count = df.count()
        null_count = df.filter(F.col("death_clock_seconds").isNull()).count()
        null_pct = null_count / row_count if row_count > 0 else 1.0

        return AssetCheckResult(
            passed=(row_count >= 1 and null_pct < 0.5),
            metadata={
                "row_count": row_count,
                "null_pct_death_clock_seconds": f"{null_pct:.4f}",
            },
        )
    finally:
        spark.stop()


@asset_check(
    asset=AssetKey(["orakel", "gold_kpi_healer_deficit"]),
    description="Gold KPI healer_deficit: row_count >= 1, null % on primary metric < 50%",
)
def gold_kpi_healer_deficit_check() -> dict:
    """Check gold_kpi_healer_deficit data quality."""
    from pyspark.sql import functions as F

    from dagster import AssetCheckResult

    spark = get_spark_session("check_gold_kpi_healer_deficit")
    try:
        path = f"s3a://{settings.MINIO_BUCKET}/gold/kpi_healer_deficit"
        try:
            df = spark.read.parquet(path)
        except Exception:
            return AssetCheckResult(
                passed=False,
                metadata={"error": "Gold KPI healer_deficit data not found"},
            )

        row_count = df.count()
        null_count = df.filter(F.col("deficit_ratio").isNull()).count()
        null_pct = null_count / row_count if row_count > 0 else 1.0

        return AssetCheckResult(
            passed=(row_count >= 1 and null_pct < 0.5),
            metadata={
                "row_count": row_count,
                "null_pct_deficit_ratio": f"{null_pct:.4f}",
            },
        )
    finally:
        spark.stop()


@asset_check(
    asset=AssetKey(["orakel", "gold_kpi_interrupt_rate"]),
    description="Gold KPI interrupt_rate: row_count >= 1, null % on primary metric < 50%",
)
def gold_kpi_interrupt_rate_check() -> dict:
    """Check gold_kpi_interrupt_rate data quality."""
    from pyspark.sql import functions as F

    from dagster import AssetCheckResult

    spark = get_spark_session("check_gold_kpi_interrupt_rate")
    try:
        path = f"s3a://{settings.MINIO_BUCKET}/gold/kpi_interrupt_rate"
        try:
            df = spark.read.parquet(path)
        except Exception:
            return AssetCheckResult(
                passed=False,
                metadata={"error": "Gold KPI interrupt_rate data not found"},
            )

        row_count = df.count()
        null_count = df.filter(F.col("interrupts_per_minute").isNull()).count()
        null_pct = null_count / row_count if row_count > 0 else 1.0

        return AssetCheckResult(
            passed=(row_count >= 1 and null_pct < 0.5),
            metadata={
                "row_count": row_count,
                "null_pct_interrupts_per_minute": f"{null_pct:.4f}",
            },
        )
    finally:
        spark.stop()


# ─── Gold Features Check (placeholder for PR2) ──────────────────────────────────


@asset_check(
    asset=AssetKey(["orakel", "gold_kpi_synergy"]),
    description="Gold KPI synergy: row_count >= 1, null % on primary metric < 50%",
)
def gold_kpi_synergy_check() -> dict:
    """Check gold_kpi_synergy data quality."""
    from pyspark.sql import functions as F

    from dagster import AssetCheckResult

    spark = get_spark_session("check_gold_kpi_synergy")
    try:
        path = f"s3a://{settings.MINIO_BUCKET}/gold/kpi_composition_synergy"
        try:
            df = spark.read.parquet(path)
        except Exception:
            return AssetCheckResult(
                passed=False,
                metadata={"error": "Gold KPI synergy data not found"},
            )

        row_count = df.count()
        null_count = df.filter(F.col("synergy_score").isNull()).count()
        null_pct = null_count / row_count if row_count > 0 else 1.0

        return AssetCheckResult(
            passed=(row_count >= 1 and null_pct < 0.5),
            metadata={
                "row_count": row_count,
                "null_pct_synergy_score": f"{null_pct:.4f}",
            },
        )
    finally:
        spark.stop()


# gold_features check will be added in PR2 (ML Training) when the asset exists
# Placeholder for task 5.7 — gold_features row_count >= 10, no null clear_time_seconds


@asset_check(
    asset=AssetKey(["orakel", "gold_features"]),
    description="Gold features: row_count >= 10, no null clear_time_seconds",
)
def gold_features_check() -> dict:
    """Check gold_features data quality.

    Validates that the feature view has at least 10 rows and no NULL values
    in the target column (clear_time_seconds).
    """
    from pyspark.sql import functions as F

    from dagster import AssetCheckResult

    spark = get_spark_session("check_gold_features")
    try:
        path = f"s3a://{settings.MINIO_BUCKET}/gold/features"
        try:
            df = spark.read.parquet(path).filter(F.col("season") == settings.SEASON)
        except Exception:
            return AssetCheckResult(
                passed=False,
                metadata={"error": "Gold features data not found"},
            )

        row_count = df.count()
        if row_count < 1:
            return AssetCheckResult(
                passed=False,
                metadata={"error": "Gold features data is empty", "row_count": row_count},
            )

        null_count = df.filter(F.col("clear_time_seconds").isNull()).count()
        null_pct = null_count / row_count if row_count > 0 else 1.0

        return AssetCheckResult(
            passed=(row_count >= 10 and null_count == 0),
            metadata={
                "row_count": row_count,
                "null_clear_time_seconds": null_count,
                "null_pct_clear_time_seconds": f"{null_pct:.4f}",
            },
        )
    finally:
        spark.stop()