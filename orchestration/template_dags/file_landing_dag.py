from __future__ import annotations

from datetime import timedelta
from pathlib import Path

from airflow import DAG
from airflow.utils.dates import days_ago

from adapters.factory.adapter_config import MetricAdapterType
from orchestration.operators.init_operator import InitOperator
from orchestration.operators.metric_operator import MetricPushOperator
from orchestration.operators.spark_run_operator import SparkRunOperator

_CONFIG_PATH = str(
    Path(__file__).parent.parent.parent / "config" / "pipeline_config.yaml"
)

# ── Metric queue config (edit per environment) ────────────────────────────
# Switch metric_type and metric_config to match your target queue.

# SQS example:
# _METRIC_TYPE = MetricAdapterType.CLOUD_QUEUE
# _METRIC_CONFIG = {
#     "credential_ref": "data-platform/sqs",
#     "queue_url": "https://sqs.ap-southeast-1.amazonaws.com/123/pipeline-metrics",
#     "aws_region": "ap-southeast-1",
#     "message_attributes": {"service": "file-landing"},
# }

# Redis Streams example (active):
_METRIC_TYPE = MetricAdapterType.ONPREM_QUEUE
_METRIC_CONFIG = {
    "credential_ref": "data-platform/redis",
    "host": "redis.infra.svc.cluster.local",
    "port": 6379,
    "stream_name": "pipeline-metrics",
    "max_len": 5000,
    "message_attributes": {"service": "file-landing"},
}

default_args = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
}

with DAG(
    dag_id="file_landing_pipeline",
    description="Batch ingestion — reads from source, lands to storage",
    schedule_interval="@daily",
    start_date=days_ago(1),
    default_args=default_args,
    catchup=False,
    tags=["ingestion", "file-landing"],
) as dag:

    init = InitOperator(
        task_id="init",
        config_path=_CONFIG_PATH,
        mongo_conn_id="mongo_checkpoint",
        openbao_conn_id="openbao_default",
        xcom_key="run_context",
    )

    spark_run = SparkRunOperator(
        task_id="spark_run",
        init_task_id="init",
        xcom_key="run_context",
        mongo_conn_id="mongo_checkpoint",
        metric_type=_METRIC_TYPE,
        metric_config_raw=_METRIC_CONFIG,
        spark_config={
            "spark.sql.shuffle.partitions": "200",
            "spark.executor.memory": "4g",
            "spark.driver.memory": "2g",
        },
    )

    # Standalone final task — publishes "success" after full pipeline completes
    metric_push = MetricPushOperator(
        task_id="metric_push",
        init_task_id="init",
        metric_type=_METRIC_TYPE,
        metric_config_raw=_METRIC_CONFIG,
        status="success",
        extra_payload={"stage": "complete"},
    )

    init >> spark_run >> metric_push
