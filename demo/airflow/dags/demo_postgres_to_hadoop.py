"""
Demo DAG: PostgreSQL orders → Spark → Hadoop HDFS
Uses the real InitOperator, SparkRunOperator, and MetricPushOperator.
"""
from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import yaml
from airflow import DAG
from airflow.utils.dates import days_ago

from adapters.factory.adapter_config import MetricAdapterType
from orchestration.operators.init_operator import InitOperator
from orchestration.operators.spark_run_operator import SparkRunOperator
from orchestration.operators.metric_operator import MetricPushOperator

_CONFIG_PATH = "/opt/airflow/app/config/pipeline_config.yaml"
_cfg = yaml.safe_load(Path(_CONFIG_PATH).read_text())

_METRIC_TYPE = MetricAdapterType(_cfg["metric_type"])
_METRIC_CONFIG = _cfg["metric"]
_SPARK_CONFIG = _cfg.get("spark", {})

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
        checkpoint_conn_id="postgres_checkpoint",
        openbao_conn_id="openbao_default",
        xcom_key="run_context",
    )

    spark_run = SparkRunOperator(
        task_id="spark_run",
        init_task_id="init",
        xcom_key="run_context",
        checkpoint_conn_id="postgres_checkpoint",
        metric_type=_METRIC_TYPE,
        metric_config_raw=_METRIC_CONFIG,
        spark_config=_SPARK_CONFIG,
    )

    metric_push = MetricPushOperator(
        task_id="metric_push",
        init_task_id="init",
        spark_task_id="spark_run",
        metric_type=_METRIC_TYPE,
        metric_config_raw=_METRIC_CONFIG,
        status="success",
        extra_payload={"pipeline": "postgres_to_hadoop"},
        checkpoint_conn_id="postgres_checkpoint",
    )

    init >> spark_run >> metric_push
