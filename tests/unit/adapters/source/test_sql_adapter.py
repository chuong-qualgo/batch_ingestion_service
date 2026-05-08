"""Tests for SQLAdapter, SourcePostgresAdapter, SourceMySQLAdapter."""
import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch

from adapters.source.base_read_adapter import TableSourceConfig
from adapters.source.sql_adapter import SQLAdapter
from adapters.source.sql.source_postgres_adapter import SourcePostgresAdapter
from adapters.source.sql.source_mysql_adapter import SourceMySQLAdapter


# ── Fixtures ──────────────────────────────────────────────────────────────

@pytest.fixture
def postgres_adapter(mock_spark, table_source_config, sql_credentials):
    # host/port already injected via conftest table_source_config fixture
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


def test_build_read_query_with_datetime_checkpoint(postgres_adapter):
    postgres_adapter.checkpoint_from = datetime(2024, 1, 1)
    postgres_adapter.checkpoint_to   = datetime(2024, 1, 31)
    query = postgres_adapter._build_read_query()
    assert "updated_at > '2024-01-01 00:00:00'" in query
    assert "updated_at <= '2024-01-31 00:00:00'" in query


def test_build_read_query_with_int_checkpoint(postgres_adapter):
    postgres_adapter.checkpoint_from = 100
    postgres_adapter.checkpoint_to   = 200
    query = postgres_adapter._build_read_query()
    assert "updated_at > 100" in query
    assert "updated_at <= 200" in query


def test_build_read_query_no_where_without_both_bounds(postgres_adapter):
    # WHERE clause requires both bounds; only from set → full scan
    postgres_adapter.checkpoint_from = 100
    postgres_adapter.checkpoint_to   = None
    query = postgres_adapter._build_read_query()
    assert "WHERE" not in query


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

    result = postgres_adapter.read()

    mock_reader.format.assert_called_with("jdbc")
    assert result is mock_df


def test_read_does_not_modify_checkpoint_to(postgres_adapter, mock_df):
    # checkpoint_to is resolved by InitOperator before the run; read() must not change it
    mock_reader = MagicMock()
    mock_reader.format.return_value = mock_reader
    mock_reader.option.return_value = mock_reader
    mock_reader.load.return_value = mock_df
    postgres_adapter.spark.read = mock_reader
    postgres_adapter.checkpoint_to = datetime(2024, 1, 20)

    postgres_adapter.read()

    assert postgres_adapter.checkpoint_to == datetime(2024, 1, 20)


def test_validate_connection(postgres_adapter):
    mock_reader = MagicMock()
    mock_reader.format.return_value = mock_reader
    mock_reader.option.return_value = mock_reader
    mock_df = MagicMock()
    mock_df.limit.return_value.collect.return_value = [MagicMock()]
    mock_reader.load.return_value = mock_df
    postgres_adapter.spark.read = mock_reader
    result = postgres_adapter.validate_connection()
    assert result is True


# ── _format_checkpoint ────────────────────────────────────────────────────

def test_format_checkpoint_int(postgres_adapter):
    assert postgres_adapter._format_checkpoint(42) == "42"
    assert postgres_adapter._format_checkpoint(0) == "0"


def test_format_checkpoint_datetime(postgres_adapter):
    dt = datetime(2024, 6, 15, 9, 30, 0)
    assert postgres_adapter._format_checkpoint(dt) == "'2024-06-15 09:30:00'"


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
