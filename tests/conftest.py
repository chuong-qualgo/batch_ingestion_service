"""
Shared pytest fixtures used across all test modules.
"""
import pytest
from datetime import date, datetime
from unittest.mock import MagicMock

from adapters.source.base_read_adapter import TableSourceConfig, PathSourceConfig
from adapters.write.base_write_adapter import SinkConfig
from adapters.metric.base_metric_adapter import RedisMetricConfig, SQSMetricConfig


# ── Source configs ────────────────────────────────────────────────────────

@pytest.fixture
def table_source_config():
    return TableSourceConfig(
        credential_ref="data-processor/postgres",
        host="localhost",
        port=5432,
        database="orders_db",
        schema="public",
        table="orders",
        checkpoint_column="updated_at",
    )


@pytest.fixture
def path_source_config():
    return PathSourceConfig(
        credential_ref="data-processor/s3",
        path="s3://raw-bucket/exports/",
        file_format=PathSourceConfig.FileFormat.PARQUET,
        checkpoint_column="event_time",
    )


# ── Sink config ───────────────────────────────────────────────────────────

@pytest.fixture
def sink_config(table_source_config):
    return SinkConfig(
        endpoint="hdfs://namenode:9000",
        credential_ref="data-platform/hadoop",
        source_system_name="postgres-prod",
        ingestion_date=date(2024, 1, 15),
        ingestion_time=datetime(2024, 1, 15, 14, 30, 0),
        run_id="scheduled__2024-01-15",
        source_config=table_source_config,
    )


# ── Credentials ───────────────────────────────────────────────────────────

@pytest.fixture
def sql_credentials():
    return {"username": "admin", "password": "secret"}


@pytest.fixture
def s3_credentials():
    return {
        "aws_access_key_id": "AKIAIOSFODNN7EXAMPLE",
        "aws_secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "aws_region": "ap-southeast-1",
    }


@pytest.fixture
def sqs_credentials():
    return {
        "aws_access_key_id": "AKIAIOSFODNN7EXAMPLE",
        "aws_secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    }


@pytest.fixture
def redis_credentials():
    return {"password": "redispass"}


# ── Metric configs ────────────────────────────────────────────────────────

@pytest.fixture
def sqs_metric_config():
    return SQSMetricConfig(
        credential_ref="data-platform/sqs",
        queue_url="https://sqs.ap-southeast-1.amazonaws.com/123456789/metrics",
        aws_region="ap-southeast-1",
    )


@pytest.fixture
def redis_metric_config():
    return RedisMetricConfig(
        credential_ref="data-platform/redis",
        host="localhost",
        port=6379,
        stream_name="pipeline-metrics",
        max_len=1000,
    )


# ── Mock Spark ────────────────────────────────────────────────────────────

@pytest.fixture
def mock_spark():
    spark = MagicMock()
    spark.sparkContext.appName = "test"
    spark.sparkContext._jvm = MagicMock()
    spark.sparkContext._jsc = MagicMock()
    return spark


@pytest.fixture
def mock_df():
    df = MagicMock()
    df.schema.fields = []
    df.columns = ["id", "name", "updated_at"]
    df.filter.return_value = df
    df.select.return_value = df
    df.withColumn.return_value = df
    agg_row = MagicMock()
    agg_row.__getitem__ = lambda self, idx: "2024-01-15"
    df.agg.return_value = MagicMock(collect=MagicMock(return_value=[agg_row]))
    df.write = MagicMock()
    df.write.format.return_value = df.write
    df.write.mode.return_value = df.write
    df.write.partitionBy.return_value = df.write
    df.write.save = MagicMock()
    return df
