"""
Demo DAG: PostgreSQL orders → Spark → Hadoop HDFS
Uses the real InitOperator, SparkRunOperator, and MetricPushOperator.
"""
from __future__ import annotations

from datetime import timedelta
from pathlib import Path

from airflow import DAG
from airflow.utils.dates import days_ago

from adapters.factory.adapter_config import MetricAdapterType
from orchestration.operators.init_operator import InitOperator
from orchestration.operators.spark_run_operator import SparkRunOperator
from orchestration.operators.metric_operator import MetricPushOperator

_CONFIG_PATH = "/opt/airflow/app/config/pipeline_config.yaml"

_METRIC_CONFIG = {
    "credential_ref": "data-platform/redis",
    "host": "redis",
    "port": 6379,
    "stream_name": "pipeline-metrics",
    "max_len": 1000,
    "message_attributes": {"service": "demo-pipeline"},
}

default_args = {
    "owner": "demo",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="demo_postgres_to_hadoop",
    description="Demo: reads orders from PostgreSQL, writes Parquet to Hadoop HDFS",
    schedule_interval=None,        # trigger manually via UI for demo
    start_date=days_ago(1),
    default_args=default_args,
    catchup=False,
    tags=["demo", "ingestion"],
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
        metric_type=MetricAdapterType.ONPREM_QUEUE,
        metric_config_raw=_METRIC_CONFIG,
        spark_config={
            "spark.master": "spark://spark-master:7077",
            "spark.sql.shuffle.partitions": "4",
            "spark.executor.memory": "1g",
            "spark.driver.memory": "2g",
            "spark.driver.maxResultSize": "2g",
            "spark.network.timeout": "600s",
            "spark.executor.heartbeatInterval": "60s",
            "spark.jars": "/home/airflow/jars/postgresql-42.7.3.jar",
        },
    )

    metric_push = MetricPushOperator(
        task_id="metric_push",
        init_task_id="init",
        metric_type=MetricAdapterType.ONPREM_QUEUE,
        metric_config_raw=_METRIC_CONFIG,
        status="success",
        extra_payload={"pipeline": "postgres_to_hadoop"},
    )

    init >> spark_run >> metric_push
