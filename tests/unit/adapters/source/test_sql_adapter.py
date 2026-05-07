"""Tests for SQLAdapter, SourcePostgresAdapter, SourceMySQLAdapter."""
import pytest
from unittest.mock import MagicMock, patch

from adapters.source.base_read_adapter import TableSourceConfig
from adapters.source.sql_adapter import SQLAdapter
from adapters.source.sql.source_postgres_adapter import SourcePostgresAdapter
from adapters.source.sql.source_mysql_adapter import SourceMySQLAdapter


# ── Fixtures ──────────────────────────────────────────────────────────────

@pytest.fixture
def postgres_adapter(mock_spark, table_source_config, sql_credentials):
    return SourcePostgresAdapter(
        spark=mock_spark,
        source_config=table_source_config,
        credentials=sql_credentials,
    )


@pytest.fixture
def mysql_adapter(mock_spark, sql_credentials):
    cfg = TableSourceConfig(
        credential_ref="data-processor/mysql",
        host="mysql-host",
        port=3306,
        database="mydb",
        schema="dbo",
        table="customers",
    )
    return SourceMySQLAdapter(
        spark=mock_spark,
        source_config=cfg,
        credentials=sql_credentials,
    )


# ── Driver and URL ────────────────────────────────────────────────────────

def test_postgres_driver(postgres_adapter):
    assert postgres_adapter.driver == "org.postgresql.Driver"


def test_postgres_jdbc_url(postgres_adapter):
    url = postgres_adapter._build_jdbc_url()
    assert url == "jdbc:postgresql://localhost:5432/orders_db"


def test_mysql_driver(mysql_adapter):
    assert mysql_adapter.driver == "com.mysql.cj.jdbc.Driver"


def test_mysql_jdbc_url(mysql_adapter):
    url = mysql_adapter._build_jdbc_url()
    assert "jdbc:mysql://mysql-host:3306/mydb" in url
    assert "useSSL=false" in url


# ── Query building ────────────────────────────────────────────────────────

def test_build_read_query_full_table(postgres_adapter):
    postgres_adapter.source_config.checkpoint_column = None
    query = postgres_adapter._build_read_query()
    assert "orders" in query
    assert "WHERE" not in query


def test_build_read_query_with_checkpoint(postgres_adapter):
    postgres_adapter.checkpoint_from = "2024-01-01"
    query = postgres_adapter._build_read_query()
    assert "updated_at > '2024-01-01'" in query


def test_build_read_query_uses_custom_query(postgres_adapter):
    postgres_adapter.source_config.query = "SELECT id FROM public.orders LIMIT 10"
    query = postgres_adapter._build_read_query()
    assert "SELECT id FROM public.orders LIMIT 10" in query
    assert "WHERE" not in query


# ── apply_filters ─────────────────────────────────────────────────────────

def test_apply_filters_empty(postgres_adapter):
    postgres_adapter.filters = {}
    postgres_adapter.apply_filters()
    assert "pushDownPredicate" not in postgres_adapter._jdbc_options


def test_apply_filters_sets_options(postgres_adapter):
    postgres_adapter.filters = {"status": "= 'active'", "age": "> 18"}
    postgres_adapter.apply_filters()
    assert "pushDownPredicate" in postgres_adapter._jdbc_options


# ── read ──────────────────────────────────────────────────────────────────


def test_read_calls_spark_jdbc(postgres_adapter, mock_df):
    mock_reader = MagicMock()
    mock_reader.format.return_value = mock_reader
    mock_reader.option.return_value = mock_reader
    mock_reader.schema.return_value = mock_reader
    mock_reader.load.return_value = mock_df
    postgres_adapter.spark.read = mock_reader
    agg_row = MagicMock()
    agg_row.__getitem__ = lambda self, i: "2024-01-15"
    mock_df.agg.return_value.collect.return_value = [agg_row]

    result = postgres_adapter.read()

    mock_reader.format.assert_called_with("jdbc")
    assert result is mock_df


def test_read_updates_checkpoint_to(postgres_adapter, mock_df):
    mock_reader = MagicMock()
    mock_reader.format.return_value = mock_reader
    mock_reader.option.return_value = mock_reader
    mock_reader.load.return_value = mock_df
    postgres_adapter.spark.read = mock_reader
    agg_row = MagicMock()
    agg_row.__getitem__ = lambda self, i: "2024-01-20"
    mock_df.agg.return_value.collect.return_value = [agg_row]

    postgres_adapter.read()
    assert postgres_adapter.checkpoint_to == "2024-01-20"


def test_validate_connection(postgres_adapter):
    mock_reader = MagicMock()
    mock_reader.format.return_value = mock_reader
    mock_reader.option.return_value = mock_reader
    mock_reader.load.return_value = MagicMock(count=MagicMock(return_value=1))
    postgres_adapter.spark.read = mock_reader
    result = postgres_adapter.validate_connection()
    assert result is True


# ── get_record_count ──────────────────────────────────────────────────────

def test_get_record_count(postgres_adapter):
    mock_reader = MagicMock()
    mock_reader.format.return_value = mock_reader
    mock_reader.option.return_value = mock_reader
    count_df = MagicMock()
    count_df.collect.return_value = [{"cnt": 42}]
    mock_reader.load.return_value = count_df
    postgres_adapter.spark.read = mock_reader

    count = postgres_adapter.get_record_count()
    assert count == 42
