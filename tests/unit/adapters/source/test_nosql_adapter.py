"""Tests for NoSQLAdapter and its concrete implementations."""
import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch

from adapters.source.base_read_adapter import TableSourceConfig
from adapters.source.nosql.source_mongodb_adapter import SourceMongoDBAdapter
from adapters.source.nosql.source_dynamodb_adapter import SourceDynamoDBAdapter
from adapters.source.nosql.source_cassandra_adapter import SourceCassandraAdapter


# ── Fixtures ──────────────────────────────────────────────────────────────

@pytest.fixture
def mongo_config():
    cfg = TableSourceConfig(
        credential_ref="data-processor/mongo",
        database="orders",
        table="transactions",
    )
    cfg.host = "mongo-host"   # injected from OpenBao
    cfg.port = 27017
    return cfg


@pytest.fixture
def dynamo_config():
    return TableSourceConfig(
        credential_ref="data-processor/dynamodb",
        database="",
        table="Orders",
    )


@pytest.fixture
def cassandra_config():
    cfg = TableSourceConfig(
        credential_ref="data-processor/cassandra",
        database="metrics_ks",
        table="events",
    )
    cfg.host = "cassandra-host"   # injected from OpenBao
    cfg.port = 9042
    return cfg


@pytest.fixture
def mongo_adapter(mock_spark, mongo_config):
    return SourceMongoDBAdapter(
        spark=mock_spark,
        source_config=mongo_config,
        credentials={"username": "admin", "password": "secret"},
    )


@pytest.fixture
def dynamo_adapter(mock_spark, dynamo_config):
    return SourceDynamoDBAdapter(
        spark=mock_spark,
        source_config=dynamo_config,
        credentials={
            "aws_access_key_id": "AKID",
            "aws_secret_access_key": "SECRET",
            "aws_region": "us-east-1",
        },
    )


@pytest.fixture
def cassandra_adapter(mock_spark, cassandra_config):
    return SourceCassandraAdapter(
        spark=mock_spark,
        source_config=cassandra_config,
        credentials={"username": "cassandra", "password": "cassandra"},
    )


# ── MongoDB ───────────────────────────────────────────────────────────────

def test_mongodb_spark_format(mongo_adapter):
    assert mongo_adapter.spark_format == "mongodb"


def test_mongodb_connection_options_contains_uri(mongo_adapter):
    opts = mongo_adapter._build_connection_options()
    assert "admin:secret@mongo-host:27017" in opts["spark.mongodb.read.connection.uri"]
    assert opts["spark.mongodb.read.collection"] == "transactions"


def test_mongodb_checkpoint_adds_pipeline(mongo_adapter):
    mongo_adapter.source_config.checkpoint_column = "created_at"
    mongo_adapter.checkpoint_from = datetime(2024, 1, 1)
    opts = mongo_adapter._build_connection_options()
    assert "$match" in opts["spark.mongodb.read.aggregation.pipeline"]
    assert "created_at" in opts["spark.mongodb.read.aggregation.pipeline"]


def test_mongodb_custom_query_overrides(mongo_adapter):
    mongo_adapter.source_config.query = '[{"$limit": 5}]'
    opts = mongo_adapter._build_connection_options()
    assert opts["spark.mongodb.read.aggregation.pipeline"] == '[{"$limit": 5}]'


def test_mongodb_validate_connection(mongo_adapter, mock_df):
    mock_reader = MagicMock()
    mock_reader.format.return_value = mock_reader
    mock_reader.options.return_value = mock_reader
    mock_reader.load.return_value = mock_df
    mongo_adapter.spark.read = mock_reader
    assert mongo_adapter.validate_connection() is True


def test_mongodb_get_record_count(mongo_adapter):
    mock_reader = MagicMock()
    mock_reader.format.return_value = mock_reader
    mock_reader.options.return_value = mock_reader
    count_df = MagicMock()
    count_df.collect.return_value = [{"count": 99}]
    mock_reader.load.return_value = count_df
    mongo_adapter.spark.read = mock_reader
    assert mongo_adapter.get_record_count() == 99


def test_mongodb_get_record_count_empty(mongo_adapter):
    mock_reader = MagicMock()
    mock_reader.format.return_value = mock_reader
    mock_reader.options.return_value = mock_reader
    mock_reader.load.return_value = MagicMock(collect=MagicMock(return_value=[]))
    mongo_adapter.spark.read = mock_reader
    assert mongo_adapter.get_record_count() == 0


# ── DynamoDB ──────────────────────────────────────────────────────────────

def test_dynamodb_spark_format(dynamo_adapter):
    assert dynamo_adapter.spark_format == "dynamodb"


def test_dynamodb_connection_options(dynamo_adapter):
    opts = dynamo_adapter._build_connection_options()
    assert opts["tableName"] == "Orders"
    assert opts["region"] == "us-east-1"
    assert opts["accessKey"] == "AKID"


def test_dynamodb_validate_connection(dynamo_adapter):
    with patch("boto3.client") as mock_boto:
        mock_client = MagicMock()
        mock_client.describe_table.return_value = {
            "Table": {"TableStatus": "ACTIVE"}
        }
        mock_boto.return_value = mock_client
        assert dynamo_adapter.validate_connection() is True


def test_dynamodb_get_record_count(dynamo_adapter):
    with patch("boto3.client") as mock_boto:
        mock_client = MagicMock()
        mock_client.describe_table.return_value = {
            "Table": {"ItemCount": 500}
        }
        mock_boto.return_value = mock_client
        assert dynamo_adapter.get_record_count() == 500


# ── Cassandra ─────────────────────────────────────────────────────────────

def test_cassandra_spark_format(cassandra_adapter):
    assert cassandra_adapter.spark_format == "org.apache.spark.sql.cassandra"


def test_cassandra_connection_options(cassandra_adapter):
    opts = cassandra_adapter._build_connection_options()
    assert opts["keyspace"] == "metrics_ks"
    assert opts["table"] == "events"
    assert opts["spark.cassandra.connection.host"] == "cassandra-host"


def test_cassandra_apply_filters(cassandra_adapter):
    cassandra_adapter.filters = {"partition_key": "= 'abc'"}
    cassandra_adapter.apply_filters()
    assert cassandra_adapter._read_options.get("pushdown") == "true"
    assert "partition_key" in cassandra_adapter._read_options.get("where", "")


def test_cassandra_validate_connection(cassandra_adapter):
    with patch("cassandra.cluster.Cluster") as mock_cluster_cls:
        mock_cluster = MagicMock()
        mock_session = MagicMock()
        mock_cluster.connect.return_value = mock_session
        mock_cluster_cls.return_value = mock_cluster
        result = cassandra_adapter.validate_connection()
        assert result is True
        mock_cluster.shutdown.assert_called_once()
