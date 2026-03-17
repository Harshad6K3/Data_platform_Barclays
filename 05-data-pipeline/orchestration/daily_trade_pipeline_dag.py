"""
Airflow DAG — Daily Trade Data Pipeline
========================================

Pipeline order:
  1. validate_kafka_lag       — confirm Kafka consumer group is current
  2. spark_raw_to_curated     — PySpark transformation job (EMR Serverless)
  3. dbt_run_trading_models   — dbt models: stg → dim → fct
  4. dbt_test_trading_models  — dbt tests (schema + custom)
  5. redshift_vacuum_analyze  — maintain Redshift query performance
  6. notify_success           — post to Slack #data-platform-alerts

SLA: DAG complete by 07:00 UTC. Alerts at 06:30 if not finished.
On-call: See runbook 10-leadership/runbooks/daily-pipeline-failure.md
"""

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobRunOperator
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import boto3
import logging

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner":            "data-platform",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": True,
    "email":            ["data-platform-oncall@company.com"],
    "sla":              timedelta(hours=5),  # must finish by 07:00 if started 02:00
}

EMR_SERVERLESS_APP_ID = "{{ var.value.emr_serverless_app_id }}"
EMR_EXECUTION_ROLE    = "{{ var.value.emr_execution_role_arn }}"
S3_SCRIPTS_BUCKET     = "s3://dp-scripts-prod/spark-jobs/"
DBT_CLOUD_JOB_ID      = "{{ var.value.dbt_cloud_trading_job_id }}"
SLACK_CONN_ID         = "slack_data_platform"
KAFKA_BROKERS         = "{{ var.value.msk_brokers }}"


def check_kafka_consumer_lag(**context) -> bool:
    """
    Check MSK consumer group lag. Abort pipeline if lag > threshold.
    This prevents loading stale / incomplete data.
    """
    # TODO: implement via boto3 MSK describe-cluster-v2 + Kafka AdminClient
    lag_threshold = 10_000
    current_lag   = _get_consumer_lag("flink-trade-enrichment-cg")
    if current_lag > lag_threshold:
        logger.warning("Consumer lag %d exceeds threshold %d", current_lag, lag_threshold)
        return False  # ShortCircuit — skip downstream tasks
    logger.info("Consumer lag %d within threshold", current_lag)
    return True


def _get_consumer_lag(consumer_group: str) -> int:
    """Stub: replace with Kafka AdminClient consumer group offset check."""
    return 0  # TODO


with DAG(
    dag_id        = "daily_trade_pipeline",
    default_args  = DEFAULT_ARGS,
    description   = "Daily trade data pipeline: Kafka → S3 → Redshift",
    schedule_interval = "0 2 * * 1-5",   # Weekdays 02:00 UTC
    start_date    = days_ago(1),
    catchup       = False,
    max_active_runs = 1,
    tags          = ["trading", "tier-1", "data-platform"],
) as dag:

    # ── 1. Validate Kafka lag ────────────────────────────────────────────────
    validate_lag = ShortCircuitOperator(
        task_id        = "validate_kafka_lag",
        python_callable = check_kafka_consumer_lag,
        provide_context = True,
    )

    # ── 2. Spark: raw → curated ──────────────────────────────────────────────
    spark_transform = EmrServerlessStartJobRunOperator(
        task_id              = "spark_raw_to_curated",
        application_id       = EMR_SERVERLESS_APP_ID,
        execution_role_arn   = EMR_EXECUTION_ROLE,
        job_driver           = {
            "sparkSubmit": {
                "entryPoint": f"{S3_SCRIPTS_BUCKET}raw_to_curated_trades.py",
                "entryPointArguments": ["{{ ds }}"],
                "sparkSubmitParameters": (
                    "--conf spark.executor.cores=4 "
                    "--conf spark.executor.memory=16g "
                    "--conf spark.dynamicAllocation.enabled=true "
                    "--conf spark.dynamicAllocation.maxExecutors=20"
                ),
            }
        },
        configuration_overrides = {
            "monitoringConfiguration": {
                "cloudWatchLoggingConfiguration": {"enabled": True}
            }
        },
    )

    # ── 3. dbt run ───────────────────────────────────────────────────────────
    dbt_run = DbtCloudRunJobOperator(
        task_id    = "dbt_run_trading_models",
        job_id     = DBT_CLOUD_JOB_ID,
        trigger_reason = "Airflow daily_trade_pipeline {{ ds }}",
        wait_for_termination = True,
    )

    # ── 4. dbt test ──────────────────────────────────────────────────────────
    # dbt tests are embedded in the dbt Cloud job above.
    # Add a standalone step here if using dbt Core.

    # ── 5. Redshift maintenance ──────────────────────────────────────────────
    redshift_vacuum = RedshiftSQLOperator(
        task_id      = "redshift_vacuum_analyze",
        sql          = """
            VACUUM fct_trades TO 95 PERCENT;
            ANALYZE fct_trades;
        """,
        redshift_conn_id = "redshift_prod",
    )

    # ── 6. Notify success ────────────────────────────────────────────────────
    notify_success = SlackWebhookOperator(
        task_id          = "notify_success",
        slack_webhook_conn_id = SLACK_CONN_ID,
        message          = ":white_check_mark: *daily_trade_pipeline* completed for `{{ ds }}`",
        channel          = "#data-platform-alerts",
    )

    # ── DAG wiring ───────────────────────────────────────────────────────────
    validate_lag >> spark_transform >> dbt_run >> redshift_vacuum >> notify_success
