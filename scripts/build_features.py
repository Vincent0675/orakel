"""Generate gold/features/ from current Gold KPI data in MinIO.

One-shot script — reads Gold KPIs already on MinIO, runs the feature
engineering pipeline, and writes the ML feature view to gold/features/.

Usage:
    uv run python scripts/build_features.py
"""
from __future__ import annotations

from orakel.config import settings
from orakel.ml.features import build_feature_view
from orakel.utils.minio import get_spark_session

spark = get_spark_session("feature_view_build")
try:
    df = build_feature_view(spark, settings.SEASON)
    row_count = df.count()
    print(f"\n=== Feature view built: {row_count} rows, {len(df.columns)} columns ===")
    print("Columns:", df.columns)
    print("\nSample rows:")
    df.show(5, truncate=False)
    print(f"\nWritten to: s3a://{settings.MINIO_BUCKET}/gold/features")
finally:
    spark.stop()
