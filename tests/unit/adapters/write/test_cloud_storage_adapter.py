"""Tests for SinkS3Adapter."""
import pytest
from unittest.mock import MagicMock, patch

from adapters.write.base_write_adapter import SinkConfig, WriteMode
from adapters.write.sink.sink_s3_adapter import SinkS3Adapter
from adapters.source.base_read_adapter import TableSourceConfig
from datetime import date, datetime


# ── Fixtures ──────────────────────────────────────────────────────────────

@pytest.fixture
def s3_sink_config(table_source_config):
    return SinkConfig(
        endpoint="s3://my-bucket",
        credential_ref="data-platform/s3",
        source_system_name="postgres-prod",
        ingestion_date=date(2024, 1, 15),
        ingestion_time=datetime(2024, 1, 15, 14, 30, 0),
        run_id="run-001",
        source_config=table_source_config,
    )


@pytest.fixture
def s3_sink(mock_spark, s3_sink_config, s3_credentials):
    return SinkS3Adapter(
        spark=mock_spark,
        sink_config=s3_sink_config,
        credentials=s3_credentials,
    )


# ── Path normalisation ────────────────────────────────────────────────────

def test_s3_endpoint_normalised_to_s3a(s3_sink):
    assert s3_sink.sink_config.endpoint.startswith("s3a://")


def test_s3_build_write_path_contains_bucket(s3_sink):
    path = s3_sink.build_write_path()
    assert "s3a://my-bucket" in path
    assert "postgres-prod" in path
    assert "ingestion_date=2024-01-15" in path


# ── configure_spark ───────────────────────────────────────────────────────

def test_s3_configure_sets_credentials(s3_sink):
    mock_conf = MagicMock()
    s3_sink.spark.sparkContext._jsc.hadoopConfiguration.return_value = mock_conf
    s3_sink._configure_spark_for_filesystem()

    calls = {c[0][0]: c[0][1] for c in mock_conf.set.call_args_list}
    assert calls["fs.s3a.access.key"] == "AKIAIOSFODNN7EXAMPLE"
    assert calls["fs.s3a.secret.key"] == "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
    assert calls["fs.s3a.fast.upload"] == "true"


def test_s3_configure_minio_endpoint(mock_spark, table_source_config):
    sink_cfg = SinkConfig(
        endpoint="s3://minio-bucket",
        credential_ref="ref",
        source_system_name="test",
        ingestion_date=date(2024, 1, 1),
        ingestion_time=datetime(2024, 1, 1, 0, 0, 0),
        run_id="r1",
        source_config=table_source_config,
    )
    adapter = SinkS3Adapter(
        spark=mock_spark,
        sink_config=sink_cfg,
        credentials={
            "aws_access_key_id": "key",
            "aws_secret_access_key": "secret",
            "aws_region": "us-east-1",
            "aws_endpoint": "http://minio:9000",
        },
    )
    mock_conf = MagicMock()
    mock_spark.sparkContext._jsc.hadoopConfiguration.return_value = mock_conf
    adapter._configure_spark_for_filesystem()

    calls = {c[0][0]: c[0][1] for c in mock_conf.set.call_args_list}
    assert calls.get("fs.s3a.endpoint") == "http://minio:9000"
    assert calls.get("fs.s3a.path.style.access") == "true"


# ── validate_connection ───────────────────────────────────────────────────

def test_validate_connection_boto3(s3_sink):
    with patch("boto3.client") as mock_boto:
        mock_client = MagicMock()
        mock_client.head_bucket.return_value = {
            "ResponseMetadata": {"HTTPStatusCode": 200}
        }
        mock_boto.return_value = mock_client
        assert s3_sink.validate_connection() is True


# ── write ─────────────────────────────────────────────────────────────────

def test_s3_write(s3_sink, mock_df):
    s3_sink._configure_spark_for_filesystem = MagicMock()
    s3_sink.validate_schema = MagicMock(return_value=True)
    s3_sink.pre_write = MagicMock(return_value=mock_df)
    s3_sink.post_write = MagicMock()
    s3_sink.write(mock_df)
    mock_df.write.format.assert_called_with("parquet")
    mock_df.write.save.assert_called()
