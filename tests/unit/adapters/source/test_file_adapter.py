"""Tests for FileAdapter, SourceHadoopAdapter, SourceS3Adapter."""
import pytest
from unittest.mock import MagicMock, patch, PropertyMock

from adapters.source.base_read_adapter import PathSourceConfig
from adapters.source.file.source_hadoop_adapter import SourceHadoopAdapter
from adapters.source.file.source_s3_adapter import SourceS3Adapter


# ── Fixtures ──────────────────────────────────────────────────────────────

@pytest.fixture
def hadoop_adapter(mock_spark, path_source_config):
    return SourceHadoopAdapter(
        spark=mock_spark,
        source_config=path_source_config,
        credentials={"hdfs_user": "hadoop"},
    )


@pytest.fixture
def s3_adapter(mock_spark, s3_credentials):
    cfg = PathSourceConfig(
        credential_ref="data-processor/s3",
        path="s3://raw-bucket/exports/",
        file_format=PathSourceConfig.FileFormat.PARQUET,
    )
    return SourceS3Adapter(
        spark=mock_spark,
        source_config=cfg,
        credentials=s3_credentials,
    )


# ── Hadoop ────────────────────────────────────────────────────────────────

def test_hadoop_configure_sets_env(hadoop_adapter):
    with patch.dict("os.environ", {}, clear=False):
        hadoop_adapter._configure_spark_for_filesystem()
        import os
        assert os.environ.get("HADOOP_USER_NAME") == "hadoop"


def test_hadoop_configure_kerberos(mock_spark):
    adapter = SourceHadoopAdapter(
        spark=mock_spark,
        source_config=PathSourceConfig(
            credential_ref="ref",
            path="hdfs://nn:9000/data/",
        ),
        credentials={
            "hdfs_user": "hadoop",
            "kerberos_principal": "hadoop@REALM",
            "kerberos_keytab": "/etc/hadoop.keytab",
        },
    )
    mock_hadoop_conf = MagicMock()
    mock_spark.sparkContext._jsc.hadoopConfiguration.return_value = mock_hadoop_conf
    mock_ugi = MagicMock()
    mock_spark.sparkContext._jvm.org.apache.hadoop.security.UserGroupInformation = mock_ugi

    adapter._configure_spark_for_filesystem()
    mock_ugi.loginUserFromKeytab.assert_called_once_with(
        "hadoop@REALM", "/etc/hadoop.keytab"
    )


def test_hadoop_validate_connection(hadoop_adapter):
    mock_fs = MagicMock()
    mock_fs.exists.return_value = True
    mock_path = MagicMock()
    mock_path.getFileSystem.return_value = mock_fs
    hadoop_adapter.spark.sparkContext._jvm.org.apache.hadoop.fs.Path.return_value = mock_path
    hadoop_adapter.spark.sparkContext._jvm.org.apache.hadoop.conf.Configuration.return_value = MagicMock()

    with patch.dict("os.environ", {}):
        result = hadoop_adapter.validate_connection()
    assert result is True


def test_hadoop_get_record_count(hadoop_adapter):
    mock_status1 = MagicMock()
    mock_status1.isDirectory.return_value = False
    mock_status1.getPath.return_value = "hdfs://nn/data/file1.parquet"
    mock_status2 = MagicMock()
    mock_status2.isDirectory.return_value = False
    mock_status2.getPath.return_value = "hdfs://nn/data/file2.parquet"

    mock_fs = MagicMock()
    mock_fs.listStatus.return_value = [mock_status1, mock_status2]
    mock_path = MagicMock()
    mock_path.getFileSystem.return_value = mock_fs
    hadoop_adapter.spark.sparkContext._jvm.org.apache.hadoop.fs.Path.return_value = mock_path
    hadoop_adapter.spark.sparkContext._jvm.org.apache.hadoop.conf.Configuration.return_value = MagicMock()

    with patch.dict("os.environ", {}):
        count = hadoop_adapter.get_record_count()
    assert count == 2



def test_hadoop_read(hadoop_adapter, mock_df):
    mock_reader = MagicMock()
    mock_reader.format.return_value = mock_reader
    mock_reader.schema.return_value = mock_reader
    mock_reader.load.return_value = mock_df
    hadoop_adapter.spark.read = mock_reader
    agg_row = MagicMock()
    agg_row.__getitem__ = lambda self, i: "2024-01-15"
    mock_df.agg.return_value.collect.return_value = [agg_row]

    with patch.dict("os.environ", {}):
        result = hadoop_adapter.read()

    mock_reader.format.assert_called_with("parquet")
    assert result is mock_df


def test_s3_normalises_path(s3_adapter):
    assert s3_adapter.source_config.path.startswith("s3a://")


def test_s3_configure_sets_hadoop_conf(s3_adapter):
    mock_conf = MagicMock()
    s3_adapter.spark.sparkContext._jsc.hadoopConfiguration.return_value = mock_conf
    s3_adapter._configure_spark_for_filesystem()

    calls = {call[0][0]: call[0][1] for call in mock_conf.set.call_args_list}
    assert calls["fs.s3a.access.key"] == "AKIAIOSFODNN7EXAMPLE"
    assert calls["fs.s3a.impl"] == "org.apache.hadoop.fs.s3a.S3AFileSystem"


def test_s3_configure_sets_endpoint_for_minio(mock_spark, s3_credentials):
    creds = {**s3_credentials, "aws_endpoint": "http://minio:9000"}
    adapter = SourceS3Adapter(
        spark=mock_spark,
        source_config=PathSourceConfig(credential_ref="ref", path="s3://bucket/"),
        credentials=creds,
    )
    mock_conf = MagicMock()
    mock_spark.sparkContext._jsc.hadoopConfiguration.return_value = mock_conf
    adapter._configure_spark_for_filesystem()

    calls = {call[0][0]: call[0][1] for call in mock_conf.set.call_args_list}
    assert calls.get("fs.s3a.endpoint") == "http://minio:9000"
    assert calls.get("fs.s3a.path.style.access") == "true"


def test_s3_apply_filters(s3_adapter):
    s3_adapter.filters = {"region": "= 'APAC'"}
    s3_adapter.apply_filters()
    assert "post_filters" in s3_adapter._read_options
