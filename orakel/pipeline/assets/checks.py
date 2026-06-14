"""AssetCheck definitions — data quality gates per layer.

Each check validates row counts and null percentages on critical columns,
plus cross-layer referential integrity, completeness ratios, and schema
drift detection (SDD: Verification-Dagster-Orchestation).
 """

from __future__ import annotations

import hashlib
import logging
from typing import TYPE_CHECKING

from dagster import AssetCheckResult, AssetKey, asset_check

from orakel.config import settings
from orakel.utils.minio import get_spark_session

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import StructType

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


# ─── Core reusable check functions ──────────────────────────────────────────
# These three functions are the building blocks for PR 2's per-asset wrappers.
# They accept paths, not asset refs, so they can be unit-tested in isolation
# with mock Parquet reads.


def _season_filter(df: "DataFrame", season: str | None) -> "DataFrame":
    """Filter ``df`` by ``season`` if both ``df`` has a season column and a
    season was supplied.  Returns ``df`` unchanged otherwise.
    """
    from pyspark.sql import functions as F

    if not season or "season" not in df.columns:
        return df
    return df.filter(F.col("season") == season)


def check_referential_integrity(
    spark: "SparkSession",
    upstream_path: str,
    downstream_path: str,
    join_key: str,
    season: str | None = None,
    sample_size: int | None = None,
) -> AssetCheckResult:
    """Verify every ``join_key`` in ``downstream_path`` exists in ``upstream_path``.

    Performs a left-anti join from downstream -> upstream on ``join_key``.
    Returns ``passed=True`` if no orphans are found.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    upstream_path, downstream_path : str
        ``s3a://`` paths to the Parquet tables.
    join_key : str
        Column name shared by both tables (e.g. ``run_id``).
    season : str, optional
        If set, filter both tables to this season before counting.
    sample_size : int, optional
        If set and > 0, sample that many rows from downstream before the
        anti-join.  Useful when downstream is large (Gold > 100k rows).
        ``None`` or ``0`` means full scan (default).

    Notes
    -----
    Warning-only: failures do not block downstream assets.  Promoted to
    ``severity=BLOCKING`` after PR 2 validation period.
    """
    from pyspark.errors import AnalysisException
    from pyspark.sql import functions as F

    try:
        upstream_df = spark.read.parquet(upstream_path)
        downstream_df = spark.read.parquet(downstream_path)
    except AnalysisException as e:
        return AssetCheckResult(
            passed=False,
            metadata={"error": f"path not found: {e}"},
        )

    upstream_df = _season_filter(upstream_df, season)
    downstream_df = _season_filter(downstream_df, season)

    # Distinct keys keep the anti-join cheap.
    upstream_keys = upstream_df.select(join_key).distinct()
    downstream_keys = downstream_df.select(join_key).distinct()

    if sample_size and sample_size > 0:
        downstream_keys = downstream_keys.orderBy(F.rand()).limit(sample_size)

    orphans = downstream_keys.join(upstream_keys, on=join_key, how="left_anti")
    orphan_count = orphans.count()
    total_downstream = downstream_keys.count()

    return AssetCheckResult(
        passed=orphan_count == 0,
        metadata={
            "orphan_count": orphan_count,
            "total_downstream": total_downstream,
            "sample_size": sample_size or 0,
            "join_key": join_key,
        },
    )


def check_completeness_ratio(
    spark: "SparkSession",
    upstream_path: str,
    downstream_path: str,
    season: str | None = None,
    min_ratio: float = 0.5,
) -> AssetCheckResult:
    """Verify ``downstream_count / upstream_count >= min_ratio``.

    Returns ``passed=True`` if the ratio is at or above the threshold, or
    if the upstream partition is empty (treated as a non-failure with
    ``upstream_empty=true`` warning).

    Parameters
    ----------
    min_ratio : float
        Minimum acceptable ratio.  Default 0.5 (50% of upstream rows).
    """
    from pyspark.errors import AnalysisException

    try:
        upstream_df = spark.read.parquet(upstream_path)
        downstream_df = spark.read.parquet(downstream_path)
    except AnalysisException as e:
        return AssetCheckResult(
            passed=False,
            metadata={"error": f"path not found: {e}"},
        )

    upstream_df = _season_filter(upstream_df, season)
    downstream_df = _season_filter(downstream_df, season)

    upstream_count = upstream_df.count()
    downstream_count = downstream_df.count()

    if upstream_count == 0:
        # Empty upstream is a no-data signal, not a failure.  Surface as a
        # warning so operators can tell it apart from a real drop.
        return AssetCheckResult(
            passed=True,
            metadata={
                "ratio": 0.0,
                "upstream_count": 0,
                "downstream_count": downstream_count,
                "min_ratio": min_ratio,
                "upstream_empty": True,
            },
        )

    ratio = downstream_count / upstream_count
    return AssetCheckResult(
        passed=ratio >= min_ratio,
        metadata={
            "ratio": round(ratio, 4),
            "upstream_count": upstream_count,
            "downstream_count": downstream_count,
            "min_ratio": min_ratio,
        },
    )


def _schema_fingerprint(schema: "StructType") -> str:
    """Stable SHA-256 fingerprint of a StructType.

    Sorted ``(name, data_type)`` pairs ensure fingerprint stability across
    field reordering.  Used to compare Parquet footer schemas without
    shipping the full schema in metadata.
    """
    pairs = sorted((f.name, str(f.dataType)) for f in schema.fields)
    payload = "|".join(f"{n}:{t}" for n, t in pairs).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:16]


def check_schema_drift(
    spark: "SparkSession",
    path: str,
    expected_schema: "StructType",
    season: str | None = None,
    mode: str = "superset",
) -> AssetCheckResult:
    """Detect schema drift between a Parquet path and an expected ``StructType``.

    Reads the Parquet footer (no full scan) and compares the actual schema
    with ``expected_schema``.

    Parameters
    ----------
    path : str
        ``s3a://`` Parquet path.
    expected_schema : StructType
        Canonical schema from ``orakel.models.schemas``.
    season : str, optional
        If set, included in the result metadata for traceability.
    mode : str
        ``"exact"``  — every field name and type must match.
        ``"superset"`` — actual schema must contain every expected column
        (extra columns allowed, missing columns or type changes are drift).

    Returns
    -------
    AssetCheckResult
        ``passed=True`` when the schema conforms in the chosen mode.
        ``passed=True`` with ``path_not_found=True`` metadata when the
        path is missing (first-run scenario).
    """
    from pyspark.errors import AnalysisException

    if mode not in ("exact", "superset"):
        return AssetCheckResult(
            passed=False,
            metadata={"error": f"invalid mode '{mode}' (use 'exact' or 'superset')"},
        )

    try:
        # Parquet footer is read for .schema — no full scan happens until
        # an action like .count() is called.
        actual_schema = spark.read.parquet(path).schema
    except AnalysisException:
        return AssetCheckResult(
            passed=True,
            metadata={"path_not_found": True, "path": path},
        )

    expected_names = {f.name: f for f in expected_schema.fields}
    actual_names = {f.name: f for f in actual_schema.fields}

    if mode == "exact":
        missing = [n for n in expected_names if n not in actual_names]
        extra = [n for n in actual_names if n not in expected_names]
    else:  # superset
        missing = [n for n in expected_names if n not in actual_names]
        extra = []  # forward-compat: extra columns are allowed

    type_mismatches = [
        n
        for n in expected_names
        if n in actual_names
        and str(expected_names[n].dataType) != str(actual_names[n].dataType)
    ]

    actual_fp = _schema_fingerprint(actual_schema)
    expected_fp = _schema_fingerprint(expected_schema)

    drift_detected = bool(missing) or bool(type_mismatches)
    return AssetCheckResult(
        passed=not drift_detected,
        metadata={
            "mode": mode,
            "missing_columns": ",".join(missing) if missing else "",
            "extra_columns": ",".join(extra) if extra else "",
            "type_mismatches": ",".join(type_mismatches) if type_mismatches else "",
            "actual_schema_fingerprint": actual_fp,
            "expected_schema_fingerprint": expected_fp,
            "season": season or "",
        },
    )


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