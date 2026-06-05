"""Spark session factory with S3A configuration pointing to MinIO."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession

from orakel.config import settings

# Resolve JARs directory relative to project root
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_JARS_DIR = _PROJECT_ROOT / "jars"
_JARS = ",".join(str(j) for j in sorted(_JARS_DIR.glob("*.jar"))) if _JARS_DIR.exists() else ""


def get_spark_session(app_name: str = "orakel") -> SparkSession:
    """Create or retrieve a SparkSession configured for local mode with MinIO S3A.

    Configuration:
        - Master: local[*] (use all available CPU cores)
        - Driver memory: 4g
        - S3A endpoint: MinIO from settings
        - Path-style access: enabled
        - Extra JARs: hadoop-aws + AWS SDK v2 bundle for S3A support

    Args:
        app_name: Application name shown in Spark UI.

    Returns:
        Configured SparkSession.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        # S3A configuration for MinIO
        .config("spark.hadoop.fs.s3a.endpoint", settings.MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", settings.MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", settings.MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        # Use SimpleAWSCredentialsProvider which reads access.key/secret.key from config
        # This works with both AWS SDK v1 and v2 bundles
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        # Disable SSL for local MinIO
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    )

    # Add S3A JARs (hadoop-aws + AWS SDK bundle)
    if _JARS:
        builder = builder.config("spark.jars", _JARS)

    return builder.getOrCreate()