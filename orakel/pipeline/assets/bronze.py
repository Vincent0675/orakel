"""Bronze-layer Dagster assets: data ingestion from external APIs."""

import logging

from dagster import AssetExecutionContext, AssetKey, Failure, MetadataValue, Output, asset

from orakel.config import settings

logger = logging.getLogger(__name__)


@asset(key_prefix=["orakel"], required_resource_keys={"spark"})
def check_minio_state(context: AssetExecutionContext) -> Output:
    """Verify MinIO connectivity before any downstream asset runs.

    Checks that the configured MinIO bucket exists and is reachable by
    performing a write-then-read health check.  If this asset fails, no
    downstream assets execute.
    """
    spark = context.resources.spark
    try:
        from pyspark.sql import Row

        test_df = spark.createDataFrame([Row(check="ok")])
        test_path = f"s3a://{settings.MINIO_BUCKET}/.orakel_health_check"
        test_df.write.mode("overwrite").parquet(test_path)
        spark.read.parquet(test_path).collect()
        context.log.info(
            "MinIO connectivity check passed for bucket %s", settings.MINIO_BUCKET
        )
        return Output(
            value=True,
            metadata={
                "minio_endpoint": MetadataValue.text(settings.MINIO_ENDPOINT),
                "bucket": MetadataValue.text(settings.MINIO_BUCKET),
            },
        )
    except Exception as e:
        raise Failure(
            description=f"MinIO bucket {settings.MINIO_BUCKET} no accesible: {e}"
        )


@asset(
    key_prefix=["orakel"],
    deps=[AssetKey(["orakel", "check_minio_state"])],
    required_resource_keys={"spark"},
)
def bronze_rio(context: AssetExecutionContext) -> Output:
    """Ingest Raider.IO runs into Bronze Parquet on MinIO.

    Wraps ``orakel.pipeline.bronze.ingest_raiderio_runs``.
    """
    from orakel.clients.raiderio import RaiderIOClient
    from orakel.pipeline.bronze import ingest_raiderio_runs

    spark = context.resources.spark
    client = RaiderIOClient(api_key=settings.RAIDERIO_API_KEY or None)
    count = ingest_raiderio_runs(client, spark, settings.SEASON)
    context.log.info("Bronze Raider.IO: ingested %d rows", count)
    return Output(
        value=count,
        metadata={
            "row_count": MetadataValue.int(count),
            "season": MetadataValue.text(settings.SEASON),
        },
    )


@asset(
    key_prefix=["orakel"],
    deps=[AssetKey(["orakel", "check_minio_state"]), AssetKey(["orakel", "match_manifest"])],
    required_resource_keys={"spark"},
)
def bronze_wcl(context: AssetExecutionContext) -> Output:
    """Ingest WarcraftLogs data into Bronze Parquet.

    If WCL ingestion fails (API down, rate-limit, etc.), returns
    ``row_count=0`` so downstream can fall back to Raider.IO-only data.
    """
    from orakel.clients.warcraftlogs import WarcraftLogsClient

    spark = context.resources.spark
    from scripts.ingest_warcraftlogs import (
        ingest_damage_taken,
        ingest_healing,
        ingest_interrupts,
        load_match_manifest,
        write_bronze_events,
    )

    wcl_client = WarcraftLogsClient(
        client_id=settings.WCL_CLIENT_ID,
        client_secret=settings.WCL_CLIENT_SECRET,
    )

    try:
        matches = load_match_manifest(spark, settings.SEASON)
        if not matches:
            context.log.warning("No match manifest found — skipping WCL ingestion.")
            return Output(
                value=0,
                metadata={
                    "row_count": MetadataValue.int(0),
                    "season": MetadataValue.text(settings.SEASON),
                },
            )

        total_events = 0
        # Group by report_code for efficient API usage
        from collections import defaultdict

        by_report: dict[str, list[dict]] = defaultdict(list)
        for m in matches:
            by_report[m["wcl_report_code"]].append(m)

        for report_code, report_matches in by_report.items():
            fight_ids = [m["wcl_fight_id"] for m in report_matches]

            # Fetch master data for actor name resolution
            actor_map: dict[int, str] = {}
            try:
                master_data = wcl_client.get_master_data(report_code)
                for actor in master_data.get("actors", []):
                    if actor.get("type") == "Player":
                        aid = actor.get("id")
                        aname = actor.get("name", "")
                        if aid and aname:
                            actor_map[aid] = aname
            except Exception as e:
                context.log.warning(
                    "Failed to fetch masterData for %s: %s", report_code, e
                )

            # Ingest each event type
            for event_type, ingest_fn in [
                ("damage_taken", ingest_damage_taken),
                ("healing", ingest_healing),
                ("interrupts", ingest_interrupts),
            ]:
                if event_type == "damage_taken":
                    events = ingest_fn(wcl_client, report_code, fight_ids)
                elif event_type == "healing":
                    events = ingest_fn(
                        wcl_client, report_code, fight_ids, actor_map
                    )
                else:
                    events = ingest_fn(
                        wcl_client, report_code, fight_ids, actor_map
                    )
                write_bronze_events(spark, events, event_type)
                total_events += len(events)
                context.log.info(
                    "  %s: %d events for report %s",
                    event_type,
                    len(events),
                    report_code,
                )

        context.log.info("Bronze WCL: ingested %d total events", total_events)
        return Output(
            value=total_events,
            metadata={
                "row_count": MetadataValue.int(total_events),
                "season": MetadataValue.text(settings.SEASON),
            },
        )
    except Exception as e:
        context.log.warning(
            "Bronze WCL ingestion failed (%s: %s). "
            "Returning empty result — downstream will use Rio-only fallback.",
            type(e).__name__,
            e,
        )
        return Output(
            value=0,
            metadata={
                "row_count": MetadataValue.int(0),
                "season": MetadataValue.text(settings.SEASON),
                "error": MetadataValue.text(str(e)),
            },
        )


@asset(
    key_prefix=["orakel"],
    deps=[AssetKey(["orakel", "bronze_rio"])],
    required_resource_keys={"spark"},
)
def match_manifest(context: AssetExecutionContext) -> Output:
    """Create match manifest: fuzzy-join Raider.IO runs with WCL reports.

    Wraps ``scripts.match_reports.run_fuzzy_join``.
    """
    spark = context.resources.spark
    from scripts.match_reports import run_fuzzy_join, write_match_manifest

    try:
        matches = run_fuzzy_join(spark, settings.SEASON)
        context.log.info("Match manifest: %d matches", len(matches))
        write_match_manifest(spark, matches, settings.SEASON)
        return Output(
            value=len(matches),
            metadata={
                "match_count": MetadataValue.int(len(matches)),
                "season": MetadataValue.text(settings.SEASON),
            },
        )
    except Exception as e:
        context.log.warning(
            "Match manifest creation failed (%s: %s). "
            "Downstream will use Rio-only fallback.",
            type(e).__name__,
            e,
        )
        return Output(
            value=0,
            metadata={
                "match_count": MetadataValue.int(0),
                "season": MetadataValue.text(settings.SEASON),
                "error": MetadataValue.text(str(e)),
            },
        )