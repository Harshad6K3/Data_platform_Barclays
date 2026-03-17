"""
PySpark Batch Job — Raw → Curated Trade Transformation
=======================================================

Responsibilities:
  1. Read raw Iceberg trade events (S3 raw zone)
  2. Deduplicate by (trade_id, event_type, updated_at)
  3. Apply data quality checks (see DataQualityValidator)
  4. Enrich with reference data broadcast join
  5. Write to curated Iceberg table (MERGE INTO)
  6. Emit quality metrics to CloudWatch
  7. Compact small files in target partition

Schedule: Nightly at 02:00 UTC via Airflow (see 05-data-pipeline/orchestration/)
SLA: Complete by 04:00 UTC
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, DateType
import logging
import sys
from datetime import date, timedelta

logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
RAW_TABLE      = "glue_catalog.dp_raw.trade_events"
CURATED_TABLE  = "glue_catalog.dp_curated.trades"
REF_TABLE      = "glue_catalog.dp_reference.instruments"
METRICS_NS     = "DataPlatform/BatchJobs"


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("raw-to-curated-trades")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue_catalog.warehouse", "s3://dp-datalake-prod/")
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )


def read_raw(spark: SparkSession, process_date: date) -> DataFrame:
    """Read raw trade events for the given process_date."""
    return (
        spark.read.format("iceberg")
        .load(RAW_TABLE)
        .filter(F.col("trade_date") == F.lit(str(process_date)))
    )


def deduplicate(df: DataFrame) -> DataFrame:
    """
    Keep the latest version of each (trade_id, event_type).
    In a streaming ingest scenario, duplicates arise from Kafka retries.
    """
    window = Window.partitionBy("trade_id", "event_type").orderBy(F.col("updated_at").desc())
    return (
        df.withColumn("_rank", F.row_number().over(window))
          .filter(F.col("_rank") == 1)
          .drop("_rank")
    )


def validate(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Split into (valid, invalid) DataFrames.
    Invalid records go to the quarantine table for ops review.
    """
    checks = (
        F.col("trade_id").isNotNull() &
        F.col("instrument_id").isNotNull() &
        F.col("notional").isNotNull() &
        (F.col("notional") > 0) &
        F.col("currency").rlike("^[A-Z]{3}$") &
        F.col("status").isin("NEW", "AMEND", "CANCEL", "SETTLE")
    )
    valid   = df.filter(checks)
    invalid = df.filter(~checks).withColumn("quarantine_reason", F.lit("FAILED_DQ_CHECK"))
    return valid, invalid


def enrich(spark: SparkSession, df: DataFrame) -> DataFrame:
    """Broadcast join with instruments reference data."""
    instruments = spark.read.format("iceberg").load(REF_TABLE)
    instruments_bc = F.broadcast(instruments.select(
        "instrument_id", "instrument_name", "asset_class", "sector"
    ))
    return df.join(instruments_bc, on="instrument_id", how="left")


def write_curated(spark: SparkSession, df: DataFrame, process_date: date):
    """Merge enriched records into the curated Iceberg table."""
    df.createOrReplaceTempView("source_trades")

    spark.sql(f"""
        MERGE INTO {CURATED_TABLE} AS target
        USING source_trades AS source
        ON target.trade_id = source.trade_id
           AND target.event_type = source.event_type
        WHEN MATCHED AND source.updated_at > target.updated_at THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
    """)
    logger.info("Merge complete for process_date=%s", process_date)


def compact(spark: SparkSession, process_date: date):
    """
    Compact small files in the target partition.
    Target: ~128 MB files. Runs after merge.
    """
    spark.sql(f"""
        CALL glue_catalog.system.rewrite_data_files(
            table => '{CURATED_TABLE}',
            where => 'trade_date = DATE "{process_date}"',
            options => map(
                'target-file-size-bytes', '134217728',
                'min-file-size-bytes',    '33554432'
            )
        )
    """)
    logger.info("Compaction complete for trade_date=%s", process_date)


def run(process_date: date):
    spark = build_spark()
    logger.info("Starting raw→curated job for %s", process_date)

    raw_df           = read_raw(spark, process_date)
    deduped_df       = deduplicate(raw_df)
    valid_df, bad_df = validate(deduped_df)
    enriched_df      = enrich(spark, valid_df)

    # Quarantine invalid records
    if bad_df.count() > 0:
        logger.warning("Quarantining %d invalid records", bad_df.count())
        bad_df.write.format("iceberg").mode("append") \
            .save("glue_catalog.dp_quarantine.trades")

    write_curated(spark, enriched_df, process_date)
    compact(spark, process_date)

    logger.info("Job complete. Valid=%d, Quarantined=%d",
                enriched_df.count(), bad_df.count())

    spark.stop()


if __name__ == "__main__":
    process_date = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 \
                   else date.today() - timedelta(days=1)
    run(process_date)
