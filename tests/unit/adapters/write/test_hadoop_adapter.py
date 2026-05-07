"""Tests for HadoopAdapter, SinkHadoopAdapter, and build_write_path."""
import pytest
from datetime import date, datetime
from unittest.mock import MagicMock, patch, call

from adapters.write.base_write_adapter import SinkConfig, WriteMode
from adapters.write.sink.sink_hadoop_adapter import SinkHadoopAdapter
from adapters.source.base_read_adapter import TableSourceConfig, PathSourceConfig


# ── Fixtures ──────────────────────────────────────────────────────────────

@pytest.fixture
def hadoop_sink(mock_spark, sink_config):
    return SinkHadoopAdapter(
        spark=mock_spark,
        sink_config=sink_config,
        credentials={"hdfs_user": "hadoop"},
    )


# ── build_write_path ──────────────────────────────────────────────────────

def test_build_write_path_table_source(hadoop_sink):
    path = hadoop_sink.build_write_path()
    assert "postgres-prod" in path
    assert "orders_db" in path
    assert "orders" in path
    assert "ingestion_date=2024-01-15" in path
    assert "ingestion_time=14-30-00" in path
    assert "run_id=scheduled__2024-01-15" in path


def test_build_write_path_path_source(mock_spark):
    path_cfg = PathSourceConfig(
        credential_ref="ref",
        path="s3a://raw-bucket/exports/",
    )
    sink_cfg = SinkConfig(
        endpoint="hdfs://namenode:9000",
        credential_ref="ref",
        source_system_name="sftp-partner",
        ingestion_date=date(2024, 3, 10),
        ingestion_time=datetime(2024, 3, 10, 9, 0, 0),
        run_id="manual__2024-03-10",
        source_config=path_cfg,
    )
    adapter = SinkHadoopAdapter(
        spark=mock_spark,
        sink_config=sink_cfg,
        credentials={"hdfs_user": "hadoop"},
    )
    path = adapter.build_write_path()
    assert "sftp-partner" in path
    assert "ingestion_date=2024-03-10" in path
    assert "ingestion_time=09-00-00" in path


# ── validate_schema ───────────────────────────────────────────────────────

def test_validate_schema_no_schema_set(hadoop_sink, mock_df):
    hadoop_sink.schema = None
    assert hadoop_sink.validate_schema(mock_df) is True


def test_validate_schema_matching(hadoop_sink, mock_df):
    from pyspark.sql.types import StructType, StructField, StringType
    schema = StructType([StructField("id", StringType())])
    hadoop_sink.schema = schema
    mock_df.schema.fields = [StructField("id", StringType())]
    assert hadoop_sink.validate_schema(mock_df) is True


def test_validate_schema_mismatch_raises(hadoop_sink, mock_df):
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType
    hadoop_sink.schema = StructType([StructField("id", IntegerType())])
    mock_df.schema.fields = [StructField("id", StringType())]
    with pytest.raises(ValueError, match="Schema validation failed"):
        hadoop_sink.validate_schema(mock_df)


# ── pre_write ─────────────────────────────────────────────────────────────

def test_pre_write_no_schema(hadoop_sink, mock_df):
    hadoop_sink.schema = None
    result = hadoop_sink.pre_write(mock_df)
    assert result is mock_df


def test_pre_write_selects_expected_columns(hadoop_sink, mock_df):
    from pyspark.sql.types import StructType, StructField, StringType
    hadoop_sink.schema = StructType([StructField("id", StringType())])
    hadoop_sink.pre_write(mock_df)
    mock_df.select.assert_called()


# ── configure_spark ───────────────────────────────────────────────────────

def test_configure_spark_sets_env(hadoop_sink):
    with patch.dict("os.environ", {}, clear=False):
        hadoop_sink._configure_spark_for_filesystem()
        import os
        assert os.environ.get("HADOOP_USER_NAME") == "hadoop"


# ── write ─────────────────────────────────────────────────────────────────

def test_write_calls_spark_writer(hadoop_sink, mock_df):
    with patch.dict("os.environ", {}):
        hadoop_sink._configure_spark_for_filesystem = MagicMock()
        hadoop_sink.validate_schema = MagicMock(return_value=True)
        hadoop_sink.pre_write = MagicMock(return_value=mock_df)
        hadoop_sink.post_write = MagicMock()
        hadoop_sink.write(mock_df)

    mock_df.write.format.assert_called_with("parquet")
    mock_df.write.save.assert_called()


def test_write_calls_on_error_on_failure(hadoop_sink, mock_df):
    hadoop_sink._configure_spark_for_filesystem = MagicMock()
    hadoop_sink.validate_schema = MagicMock(side_effect=RuntimeError("disk full"))
    hadoop_sink.on_error = MagicMock()

    with pytest.raises(RuntimeError):
        hadoop_sink.write(mock_df)

    hadoop_sink.on_error.assert_called_once()


def test_write_with_partition_by(mock_spark, sink_config, mock_df):
    adapter = SinkHadoopAdapter(
        spark=mock_spark,
        sink_config=sink_config,
        credentials={"hdfs_user": "hadoop"},
        partition_by=["region", "date"],
    )
    adapter._configure_spark_for_filesystem = MagicMock()
    adapter.validate_schema = MagicMock(return_value=True)
    adapter.pre_write = MagicMock(return_value=mock_df)
    adapter.post_write = MagicMock()
    adapter.write(mock_df)
    mock_df.write.partitionBy.assert_called_with("region", "date")


# ── validate_connection ───────────────────────────────────────────────────

def test_validate_connection_path_exists(hadoop_sink):
    hadoop_sink._configure_spark_for_filesystem = MagicMock()
    mock_fs = MagicMock()
    mock_fs.exists.return_value = True
    mock_path = MagicMock()
    mock_path.getFileSystem.return_value = mock_fs
    hadoop_sink.spark.sparkContext._jvm.org.apache.hadoop.fs.Path.return_value = mock_path
    hadoop_sink.spark.sparkContext._jvm.org.apache.hadoop.conf.Configuration.return_value = MagicMock()

    assert hadoop_sink.validate_connection() is True
