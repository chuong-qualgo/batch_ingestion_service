"""Tests for SourceConfigFactory, SinkConfigFactory, ReadAdapterFactory, WriteAdapterFactory, MetricAdapterFactory."""
import pytest
from datetime import date, datetime
from unittest.mock import MagicMock

from adapters.factory.adapter_config import (
    AdapterConfig, ReadAdapterType, WriteAdapterType, MetricAdapterType
)
from adapters.factory.source_config_factory import SourceConfigFactory
from adapters.factory.sink_config_factory import SinkConfigFactory
from adapters.factory.read_adapter_factory import ReadAdapterFactory
from adapters.factory.write_adapter_factory import WriteAdapterFactory
from adapters.factory.metric_adapter_factory import MetricAdapterFactory
from adapters.source.base_read_adapter import TableSourceConfig, PathSourceConfig
from adapters.metric.base_metric_adapter import SQSMetricConfig, RedisMetricConfig
from adapters.source.sql.source_postgres_adapter import SourcePostgresAdapter
from adapters.source.sql.source_mysql_adapter import SourceMySQLAdapter
from adapters.source.nosql.source_mongodb_adapter import SourceMongoDBAdapter
from adapters.source.nosql.source_dynamodb_adapter import SourceDynamoDBAdapter
from adapters.source.nosql.source_cassandra_adapter import SourceCassandraAdapter
from adapters.source.file.source_hadoop_adapter import SourceHadoopAdapter
from adapters.source.file.source_s3_adapter import SourceS3Adapter
from adapters.write.sink.sink_hadoop_adapter import SinkHadoopAdapter
from adapters.write.sink.sink_s3_adapter import SinkS3Adapter
from adapters.metric.sqs_queue_adapter import SQSQueueAdapter
from adapters.metric.redis_queue_adapter import RedisQueueAdapter


# ── SourceConfigFactory ───────────────────────────────────────────────────

@pytest.mark.parametrize("adapter_type", [
    ReadAdapterType.SQL, ReadAdapterType.MYSQL,
    ReadAdapterType.NOSQL, ReadAdapterType.DYNAMODB, ReadAdapterType.CASSANDRA,
])
def test_source_config_factory_table_types(adapter_type):
    cfg = SourceConfigFactory.create(
        adapter_type=adapter_type,
        credential_ref="ref",
        database="db", table="tbl",
    )
    assert isinstance(cfg, TableSourceConfig)


@pytest.mark.parametrize("adapter_type", [ReadAdapterType.FILE, ReadAdapterType.S3])
def test_source_config_factory_path_types(adapter_type):
    cfg = SourceConfigFactory.create(
        adapter_type=adapter_type,
        credential_ref="ref",
        file_format=PathSourceConfig.FileFormat.PARQUET,
    )
    assert isinstance(cfg, PathSourceConfig)


def test_source_config_factory_missing_required_raises():
    with pytest.raises(ValueError, match="Missing required"):
        SourceConfigFactory.create(
            adapter_type=ReadAdapterType.SQL,
            credential_ref="ref",
            database="", table="tbl",  # empty database triggers the error
        )


def test_source_config_factory_unknown_type_raises():
    with pytest.raises(TypeError):
        SourceConfigFactory.create(
            adapter_type="unknown_type",
            credential_ref="ref",
        )


# ── SinkConfigFactory ─────────────────────────────────────────────────────

def test_sink_config_factory_creates_config(table_source_config):
    cfg = SinkConfigFactory.create(
        credential_ref="ref",
        source_system_name="postgres-prod",
        source_config=table_source_config,
        ingestion_date=date(2024, 1, 15),
        ingestion_time=datetime(2024, 1, 15, 10, 0, 0),
        run_id="run-001",
    )
    # endpoint is empty until inject_connection() is called
    assert cfg.endpoint == ""
    assert cfg.run_id == "run-001"
    # simulate inject_connection
    SinkConfigFactory.inject_connection(cfg, {"endpoint": "hdfs://namenode:9000"})
    assert cfg.endpoint == "hdfs://namenode:9000"


def test_sink_config_factory_missing_source_system_raises(table_source_config):
    with pytest.raises(ValueError):
        SinkConfigFactory.create(
            credential_ref="ref",
            source_system_name="",      # missing source_system_name triggers error
            source_config=table_source_config,
            ingestion_date=date(2024, 1, 1),
            ingestion_time=datetime(2024, 1, 1),
            run_id="r",
        )


def test_sink_config_factory_invalid_source_config_raises():
    with pytest.raises(TypeError):
        SinkConfigFactory.create(
            endpoint="hdfs://",
            credential_ref="ref",
            source_system_name="sys",
            source_config={"not": "a config"},
            ingestion_date=date(2024, 1, 1),
            ingestion_time=datetime(2024, 1, 1),
            run_id="r",
        )


# ── ReadAdapterFactory ────────────────────────────────────────────────────

@pytest.mark.parametrize("read_type,expected_class", [
    (ReadAdapterType.SQL,       SourcePostgresAdapter),
    (ReadAdapterType.MYSQL,     SourceMySQLAdapter),
    (ReadAdapterType.NOSQL,     SourceMongoDBAdapter),
    (ReadAdapterType.DYNAMODB,  SourceDynamoDBAdapter),
    (ReadAdapterType.CASSANDRA, SourceCassandraAdapter),
    (ReadAdapterType.FILE,      SourceHadoopAdapter),
    (ReadAdapterType.S3,        SourceS3Adapter),
])
def test_read_adapter_factory_registry(read_type, expected_class, mock_spark, table_source_config, path_source_config):
    source_config = path_source_config if read_type in (ReadAdapterType.FILE, ReadAdapterType.S3) else table_source_config
    config = AdapterConfig(read_type=read_type)
    config.source_config = source_config
    adapter = ReadAdapterFactory.create(config=config, spark=mock_spark)
    assert isinstance(adapter, expected_class)


def test_read_adapter_factory_unknown_raises(mock_spark, table_source_config):
    config = AdapterConfig()
    config.read_type = "unknown"
    config.source_config = table_source_config
    with pytest.raises(ValueError):
        ReadAdapterFactory.create(config=config, spark=mock_spark)


# ── WriteAdapterFactory ───────────────────────────────────────────────────

@pytest.mark.parametrize("write_type,expected_class", [
    (WriteAdapterType.HADOOP, SinkHadoopAdapter),
    (WriteAdapterType.S3,     SinkS3Adapter),
])
def test_write_adapter_factory_registry(write_type, expected_class, mock_spark, sink_config):
    config = AdapterConfig(write_type=write_type)
    # endpoint already injected in conftest sink_config fixture
    adapter = WriteAdapterFactory.create(
        config=config, spark=mock_spark,
        sink_config=sink_config, credentials={},
    )
    assert isinstance(adapter, expected_class)


# ── MetricAdapterFactory ──────────────────────────────────────────────────

def test_metric_factory_sqs(sqs_metric_config, sqs_credentials):
    config = AdapterConfig(metric_type=MetricAdapterType.CLOUD_QUEUE)
    adapter = MetricAdapterFactory.create(
        config=config, metric_config=sqs_metric_config,
        credentials=sqs_credentials,
    )
    assert isinstance(adapter, SQSQueueAdapter)


def test_metric_factory_redis(redis_metric_config, redis_credentials):
    config = AdapterConfig(metric_type=MetricAdapterType.ONPREM_QUEUE)
    adapter = MetricAdapterFactory.create(
        config=config, metric_config=redis_metric_config,
        credentials=redis_credentials,
    )
    assert isinstance(adapter, RedisQueueAdapter)


def test_metric_factory_unknown_raises(sqs_metric_config):
    config = AdapterConfig()
    config.metric_type = "unknown"
    with pytest.raises(ValueError):
        MetricAdapterFactory.create(config=config, metric_config=sqs_metric_config, credentials={})
