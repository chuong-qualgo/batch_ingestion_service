"""
Integration tests for the full pipeline:
    InitOperator → SparkRunOperator → MetricPushOperator

All external I/O is mocked at the boundary:
  - OpenBaoHook.get_secret        (secret store)
  - MongoDB (checkpoint read/write)
  - Spark (source read + sink write)
  - SQS / Redis (metric publish)

Tests verify that the three operators wire together correctly — data flows
from YAML config through XCom into the Spark job and the metric queue.
"""
import json
import tempfile
import os
import pytest
import yaml
from datetime import date, datetime
from unittest.mock import MagicMock, patch, call

import fakeredis
from moto import mock_aws
import boto3

from adapters.factory.adapter_config import (
    ReadAdapterType, WriteAdapterType, MetricAdapterType,
)
from adapters.source.base_read_adapter import TableSourceConfig, PathSourceConfig
from adapters.write.base_write_adapter import SinkConfig
from orchestration.operators.init_operator import InitOperator
from orchestration.operators.spark_run_operator import SparkRunOperator
from orchestration.operators.metric_operator import MetricPushOperator
from orchestration.operators.run_context import RunContext


# ── Shared YAML configs ───────────────────────────────────────────────────

SQL_TO_HADOOP_CFG = {
    "read_type": "sql",
    "write_type": "hadoop",
    "metric_type": "onprem_queue",
    "source": {
        "credential_ref": "data-processor/postgres",
        "host": "pg-host",
        "port": 5432,
        "database": "orders_db",
        "schema": "public",
        "table": "orders",
        "checkpoint_column": "updated_at",
    },
    "sink": {
        "credential_ref": "data-platform/hadoop",
        "endpoint": "hdfs://namenode:9000",
        "source_system_name": "postgres-prod",
    },
    "metric": {
        "credential_ref": "data-platform/redis",
        "host": "redis-host",
        "port": 6379,
        "stream_name": "pipeline-metrics",
        "max_len": 1000,
    },
}

S3_TO_S3_CFG = {
    "read_type": "s3",
    "write_type": "s3",
    "metric_type": "cloud_queue",
    "source": {
        "credential_ref": "data-processor/s3",
        "path": "s3://raw-bucket/exports/",
        "file_format": "parquet",
    },
    "sink": {
        "credential_ref": "data-platform/s3",
        "endpoint": "s3://landing-bucket",
        "source_system_name": "sftp-partner",
    },
    "metric": {
        "credential_ref": "data-platform/sqs",
        "queue_url": "https://sqs.ap-southeast-1.amazonaws.com/123/metrics",
        "aws_region": "ap-southeast-1",
    },
}


# ── Shared credential map ─────────────────────────────────────────────────

CREDENTIALS = {
    "data-processor/postgres": {"username": "pg_user",   "password": "pg_pass"},
    "data-platform/hadoop":    {"hdfs_user": "hadoop"},
    "data-platform/redis":     {"password": "redis_pass"},
    "data-processor/s3":       {"aws_access_key_id": "AKID", "aws_secret_access_key": "SECRET", "aws_region": "ap-southeast-1"},
    "data-platform/s3":        {"aws_access_key_id": "AKID", "aws_secret_access_key": "SECRET", "aws_region": "ap-southeast-1"},
    "data-platform/sqs":       {"aws_access_key_id": "AKID", "aws_secret_access_key": "SECRET"},
}


# ── Fixtures ──────────────────────────────────────────────────────────────

@pytest.fixture
def make_yaml(tmp_path):
    """Factory — write a config dict to a temp YAML file, return path."""
    def _make(cfg):
        p = tmp_path / "pipeline_config.yaml"
        p.write_text(yaml.dump(cfg))
        return str(p)
    return _make


@pytest.fixture
def airflow_context():
    xcom_store = {}

    ti = MagicMock()
    ti.xcom_push.side_effect = lambda key, value: xcom_store.update({key: value})
    ti.xcom_pull.side_effect = lambda task_ids=None, key=None: xcom_store.get(key)

    return {
        "dag":                  MagicMock(dag_id="test_pipeline"),
        "run_id":               "scheduled__2024-01-15T14:30:00",
        "data_interval_start":  datetime(2024, 1, 15, 14, 30, 0),
        "ti":                   ti,
        "_xcom_store":          xcom_store,
    }


@pytest.fixture
def mock_openbao():
    with patch("orchestration.operators.init_operator.OpenBaoHook") as cls:
        hook = MagicMock()
        hook.get_secret.side_effect = lambda ref: CREDENTIALS.get(ref, {})
        cls.return_value = hook
        yield hook


@pytest.fixture
def mock_mongo_no_checkpoint():
    """MongoDB returns no existing checkpoint (full read)."""
    with patch.object(InitOperator, "_fetch_checkpoint_from", return_value=None):
        yield


@pytest.fixture
def mock_mongo_with_checkpoint():
    """MongoDB returns an existing checkpoint value."""
    with patch.object(
        InitOperator, "_fetch_checkpoint_from",
        return_value="2024-01-10 00:00:00"
    ):
        yield


@pytest.fixture
def mock_record_count():
    with patch.object(InitOperator, "_fetch_source_record_count", return_value=500):
        yield


@pytest.fixture
def mock_spark_pipeline(mock_df):
    """
    Mocks the full Spark pipeline inside SparkRunOperator:
    - SparkSession build
    - Source adapter read()
    - Sink adapter write()
    - checkpoint_to set after read
    """
    with patch(
        "orchestration.operators.spark_run_operator.SparkRunOperator._build_spark_session"
    ) as mock_spark_build, patch(
        "adapters.factory.read_adapter_factory.ReadAdapterFactory.create"
    ) as mock_read_factory, patch(
        "adapters.factory.write_adapter_factory.WriteAdapterFactory.create"
    ) as mock_write_factory, patch.object(
        SparkRunOperator, "_save_checkpoint"
    ) as mock_save_ckpt:

        mock_spark = MagicMock()
        mock_spark_build.return_value = mock_spark

        mock_reader = MagicMock()
        mock_reader.checkpoint_to = "2024-01-15 14:30:00"
        mock_reader.read.return_value = mock_df
        mock_reader.validate_connection.return_value = True
        mock_read_factory.return_value = mock_reader

        mock_writer = MagicMock()
        mock_writer.validate_connection.return_value = True
        mock_writer.build_write_path.return_value = (
            "hdfs://namenode:9000/postgres-prod/orders_db/public/orders/"
            "ingestion_date=2024-01-15/ingestion_time=14-30-00/"
            "run_id=scheduled__2024-01-15T14:30:00"
        )
        mock_write_factory.return_value = mock_writer

        yield {
            "spark":         mock_spark,
            "reader":        mock_reader,
            "writer":        mock_writer,
            "save_checkpoint": mock_save_ckpt,
        }


# ── Helper: build init operator ───────────────────────────────────────────

def make_init(config_path, metric_type="onprem_queue"):
    return InitOperator(
        task_id="init",
        config_path=config_path,
        mongo_conn_id="mongo_checkpoint",
        openbao_conn_id="openbao_default",
    )


# ═════════════════════════════════════════════════════════════════════════
# 1. InitOperator — verifies YAML parsing, credential fetching, XCom output
# ═════════════════════════════════════════════════════════════════════════

class TestInitOperator:

    def test_init_builds_run_context(
        self, make_yaml, airflow_context, mock_openbao,
        mock_mongo_no_checkpoint, mock_record_count
    ):
        """execute() pushes a complete RunContext dict to XCom."""
        op = make_init(make_yaml(SQL_TO_HADOOP_CFG))
        result = op.execute(airflow_context)

        assert result["dag_id"] == "test_pipeline"
        assert result["read_type"] == "sql"
        assert result["write_type"] == "hadoop"
        assert result["metric_type"] == "onprem_queue"

    def test_init_source_credentials_fetched(
        self, make_yaml, airflow_context, mock_openbao,
        mock_mongo_no_checkpoint, mock_record_count
    ):
        op = make_init(make_yaml(SQL_TO_HADOOP_CFG))
        result = op.execute(airflow_context)
        assert result["source_credentials"]["username"] == "pg_user"

    def test_init_sink_credentials_fetched(
        self, make_yaml, airflow_context, mock_openbao,
        mock_mongo_no_checkpoint, mock_record_count
    ):
        op = make_init(make_yaml(SQL_TO_HADOOP_CFG))
        result = op.execute(airflow_context)
        assert result["sink_credentials"]["hdfs_user"] == "hadoop"

    def test_init_metric_credentials_fetched(
        self, make_yaml, airflow_context, mock_openbao,
        mock_mongo_no_checkpoint, mock_record_count
    ):
        """Metric credentials are fetched from OpenBao (step 6)."""
        op = make_init(make_yaml(SQL_TO_HADOOP_CFG))
        result = op.execute(airflow_context)
        assert result["metric_credentials"]["password"] == "redis_pass"

    def test_init_metric_config_raw_in_context(
        self, make_yaml, airflow_context, mock_openbao,
        mock_mongo_no_checkpoint, mock_record_count
    ):
        """Raw metric section from YAML is preserved in RunContext."""
        op = make_init(make_yaml(SQL_TO_HADOOP_CFG))
        result = op.execute(airflow_context)
        assert result["metric_config_raw"]["stream_name"] == "pipeline-metrics"
        assert result["metric_config_raw"]["host"] == "redis-host"

    def test_init_no_checkpoint_returns_none(
        self, make_yaml, airflow_context, mock_openbao,
        mock_mongo_no_checkpoint, mock_record_count
    ):
        op = make_init(make_yaml(SQL_TO_HADOOP_CFG))
        result = op.execute(airflow_context)
        assert result["checkpoint_from"] is None

    def test_init_existing_checkpoint_passed_through(
        self, make_yaml, airflow_context, mock_openbao,
        mock_mongo_with_checkpoint, mock_record_count
    ):
        op = make_init(make_yaml(SQL_TO_HADOOP_CFG))
        result = op.execute(airflow_context)
        assert result["checkpoint_from"] == "2024-01-10 00:00:00"

    def test_init_xcom_pushed(
        self, make_yaml, airflow_context, mock_openbao,
        mock_mongo_no_checkpoint, mock_record_count
    ):
        op = make_init(make_yaml(SQL_TO_HADOOP_CFG))
        op.execute(airflow_context)
        airflow_context["ti"].xcom_push.assert_called_once()
        key = airflow_context["ti"].xcom_push.call_args[1]["key"]
        assert key == "run_context"

    def test_init_s3_source_config(
        self, make_yaml, airflow_context, mock_openbao,
        mock_mongo_no_checkpoint, mock_record_count
    ):
        """File/S3 source produces PathSourceConfig, not TableSourceConfig."""
        op = make_init(make_yaml(S3_TO_S3_CFG))
        result = op.execute(airflow_context)
        assert result["source_config"]["path"] == "s3://raw-bucket/exports/"
        assert result["read_type"] == "s3"


# ═════════════════════════════════════════════════════════════════════════
# 2. InitOperator → SparkRunOperator handoff
# ═════════════════════════════════════════════════════════════════════════

class TestInitToSparkHandoff:

    def test_spark_receives_xcom_from_init(
        self, make_yaml, airflow_context, mock_openbao,
        mock_mongo_no_checkpoint, mock_record_count, mock_spark_pipeline, mock_df
    ):
        """SparkRunOperator pulls RunContext written by InitOperator."""
        init_op = make_init(make_yaml(SQL_TO_HADOOP_CFG))
        init_op.execute(airflow_context)

        spark_op = SparkRunOperator(
            task_id="spark_run",
            init_task_id="init",
            mongo_conn_id="mongo_checkpoint",
        )
        spark_op.execute(airflow_context)

        mock_spark_pipeline["reader"].read.assert_called_once()
        mock_spark_pipeline["writer"].write.assert_called_once_with(mock_df)

    def test_spark_passes_checkpoint_from_to_reader(
        self, make_yaml, airflow_context, mock_openbao,
        mock_mongo_with_checkpoint, mock_record_count, mock_spark_pipeline
    ):
        """checkpoint_from from MongoDB is forwarded to the source adapter."""
        init_op = make_init(make_yaml(SQL_TO_HADOOP_CFG))
        init_op.execute(airflow_context)

        spark_op = SparkRunOperator(
            task_id="spark_run",
            init_task_id="init",
            mongo_conn_id="mongo_checkpoint",
        )
        spark_op.execute(airflow_context)

        reader = mock_spark_pipeline["reader"]
        assert reader.checkpoint_from == "2024-01-10 00:00:00"

    def test_spark_saves_checkpoint_after_read(
        self, make_yaml, airflow_context, mock_openbao,
        mock_mongo_no_checkpoint, mock_record_count, mock_spark_pipeline
    ):
        """checkpoint_to produced by the reader is saved to MongoDB."""
        init_op = make_init(make_yaml(SQL_TO_HADOOP_CFG))
        init_op.execute(airflow_context)

        spark_op = SparkRunOperator(
            task_id="spark_run",
            init_task_id="init",
            mongo_conn_id="mongo_checkpoint",
        )
        spark_op.execute(airflow_context)

        mock_spark_pipeline["save_checkpoint"].assert_called_once_with(
            "test_pipeline", "2024-01-15 14:30:00"
        )

    def test_spark_validates_connections_before_read(
        self, make_yaml, airflow_context, mock_openbao,
        mock_mongo_no_checkpoint, mock_record_count, mock_spark_pipeline
    ):
        """validate_connection() is called on both source and sink before read."""
        init_op = make_init(make_yaml(SQL_TO_HADOOP_CFG))
        init_op.execute(airflow_context)

        spark_op = SparkRunOperator(
            task_id="spark_run",
            init_task_id="init",
            mongo_conn_id="mongo_checkpoint",
        )
        spark_op.execute(airflow_context)

        mock_spark_pipeline["reader"].validate_connection.assert_called_once()
        mock_spark_pipeline["writer"].validate_connection.assert_called_once()

    def test_spark_pushes_failure_metric_on_error(
        self, make_yaml, airflow_context, mock_openbao,
        mock_mongo_no_checkpoint, mock_record_count
    ):
        """On pipeline failure, push_metric_inline() is called (non-fatal)."""
        fake_redis = fakeredis.FakeRedis(decode_responses=True)

        init_op = make_init(make_yaml(SQL_TO_HADOOP_CFG))
        init_op.execute(airflow_context)

        with patch(
            "orchestration.operators.spark_run_operator.SparkRunOperator._build_spark_session"
        ) as mock_spark_build, patch(
            "adapters.factory.read_adapter_factory.ReadAdapterFactory.create"
        ) as mock_read_factory, patch(
            "orchestration.operators.metric_operator.OpenBaoHook"
        ) as mock_bao_cls:
            mock_spark_build.return_value = MagicMock()
            mock_reader = MagicMock()
            mock_reader.validate_connection.side_effect = ConnectionError("source down")
            mock_read_factory.return_value = mock_reader

            mock_bao = MagicMock()
            mock_bao.get_secret.return_value = {"password": "redis_pass"}
            mock_bao_cls.return_value = mock_bao

            with patch(
                "adapters.metric.redis_queue_adapter.RedisQueueAdapter._get_client",
                return_value=fake_redis
            ):
                spark_op = SparkRunOperator(
                    task_id="spark_run",
                    init_task_id="init",
                    mongo_conn_id="mongo_checkpoint",
                    metric_type=MetricAdapterType.ONPREM_QUEUE,
                    metric_config_raw=SQL_TO_HADOOP_CFG["metric"],
                )
                with pytest.raises(ConnectionError):
                    spark_op.execute(airflow_context)

            # Failure metric must have been published to Redis stream
            entries = fake_redis.xrange("pipeline-metrics")
            assert len(entries) == 1
            _, fields = entries[0]
            assert fields["status"] == "failed"
            assert fields["dag_id"] == "test_pipeline"

    def test_spark_no_checkpoint_save_when_none(
        self, make_yaml, airflow_context, mock_openbao,
        mock_mongo_no_checkpoint, mock_record_count, mock_spark_pipeline
    ):
        """If checkpoint_to is None (no checkpoint_column), MongoDB is not written."""
        mock_spark_pipeline["reader"].checkpoint_to = None

        init_op = make_init(make_yaml(SQL_TO_HADOOP_CFG))
        init_op.execute(airflow_context)

        spark_op = SparkRunOperator(
            task_id="spark_run",
            init_task_id="init",
            mongo_conn_id="mongo_checkpoint",
        )
        spark_op.execute(airflow_context)

        mock_spark_pipeline["save_checkpoint"].assert_not_called()


# ═════════════════════════════════════════════════════════════════════════
# 3. SparkRunOperator → MetricPushOperator handoff
# ═════════════════════════════════════════════════════════════════════════

class TestSparkToMetricHandoff:

    def test_metric_push_redis_success(
        self, make_yaml, airflow_context, mock_openbao,
        mock_mongo_no_checkpoint, mock_record_count, mock_spark_pipeline
    ):
        """Full pipeline: init → spark → metric push to Redis Stream."""
        fake_redis = fakeredis.FakeRedis(decode_responses=True)

        init_op = make_init(make_yaml(SQL_TO_HADOOP_CFG))
        init_op.execute(airflow_context)

        spark_op = SparkRunOperator(
            task_id="spark_run",
            init_task_id="init",
            mongo_conn_id="mongo_checkpoint",
        )
        spark_op.execute(airflow_context)

        with patch(
            "orchestration.operators.metric_operator.OpenBaoHook"
        ) as mock_bao_cls, patch(
            "adapters.metric.redis_queue_adapter.RedisQueueAdapter._get_client",
            return_value=fake_redis
        ):
            mock_bao = MagicMock()
            mock_bao.get_secret.return_value = {"password": "redis_pass"}
            mock_bao_cls.return_value = mock_bao

            metric_op = MetricPushOperator(
                task_id="metric_push",
                init_task_id="init",
                metric_type=MetricAdapterType.ONPREM_QUEUE,
                metric_config_raw=SQL_TO_HADOOP_CFG["metric"],
                status="success",
                extra_payload={"stage": "complete"},
            )
            metric_op.execute(airflow_context)

        entries = fake_redis.xrange("pipeline-metrics")
        assert len(entries) == 1
        _, fields = entries[0]
        assert fields["status"] == "success"
        assert fields["dag_id"] == "test_pipeline"
        assert fields["stage"] == "complete"

    @mock_aws
    def test_metric_push_sqs_success(
        self, make_yaml, airflow_context, mock_openbao,
        mock_mongo_no_checkpoint, mock_record_count, mock_spark_pipeline
    ):
        """Full pipeline: init → spark → metric push to SQS."""
        sqs_client = boto3.client("sqs", region_name="ap-southeast-1")
        queue = sqs_client.create_queue(QueueName="metrics")
        queue_url = queue["QueueUrl"]

        cfg = {
            **S3_TO_S3_CFG,
            "metric": {
                "credential_ref": "data-platform/sqs",
                "queue_url": queue_url,
                "aws_region": "ap-southeast-1",
            },
        }

        init_op = make_init(make_yaml(cfg))
        init_op.execute(airflow_context)

        spark_op = SparkRunOperator(
            task_id="spark_run",
            init_task_id="init",
            mongo_conn_id="mongo_checkpoint",
        )
        spark_op.execute(airflow_context)

        with patch(
            "orchestration.operators.metric_operator.OpenBaoHook"
        ) as mock_bao_cls:
            mock_bao = MagicMock()
            mock_bao.get_secret.return_value = CREDENTIALS["data-platform/sqs"]
            mock_bao_cls.return_value = mock_bao

            metric_op = MetricPushOperator(
                task_id="metric_push",
                init_task_id="init",
                metric_type=MetricAdapterType.CLOUD_QUEUE,
                metric_config_raw=cfg["metric"],
                status="success",
            )
            metric_op.execute(airflow_context)

        msgs = sqs_client.receive_message(
            QueueUrl=queue_url, MaxNumberOfMessages=1
        ).get("Messages", [])
        assert len(msgs) == 1
        body = json.loads(msgs[0]["Body"])
        assert body["status"] == "success"
        assert body["dag_id"] == "test_pipeline"

    def test_metric_push_enriches_payload_from_run_context(
        self, make_yaml, airflow_context, mock_openbao,
        mock_mongo_no_checkpoint, mock_record_count, mock_spark_pipeline
    ):
        """MetricPushOperator pulls dag_id, run_id, read_type from XCom."""
        fake_redis = fakeredis.FakeRedis(decode_responses=True)

        init_op = make_init(make_yaml(SQL_TO_HADOOP_CFG))
        init_op.execute(airflow_context)

        spark_op = SparkRunOperator(
            task_id="spark_run",
            init_task_id="init",
            mongo_conn_id="mongo_checkpoint",
        )
        spark_op.execute(airflow_context)

        with patch(
            "orchestration.operators.metric_operator.OpenBaoHook"
        ) as mock_bao_cls, patch(
            "adapters.metric.redis_queue_adapter.RedisQueueAdapter._get_client",
            return_value=fake_redis
        ):
            mock_bao = MagicMock()
            mock_bao.get_secret.return_value = {"password": "redis_pass"}
            mock_bao_cls.return_value = mock_bao

            metric_op = MetricPushOperator(
                task_id="metric_push",
                init_task_id="init",
                metric_type=MetricAdapterType.ONPREM_QUEUE,
                metric_config_raw=SQL_TO_HADOOP_CFG["metric"],
                status="success",
            )
            metric_op.execute(airflow_context)

        _, fields = fake_redis.xrange("pipeline-metrics")[0]
        assert fields["run_id"] == "scheduled__2024-01-15T14:30:00"
        assert fields["read_type"] == "sql"
        assert fields["ingestion_date"] == "2024-01-15"


# ═════════════════════════════════════════════════════════════════════════
# 4. RunContext — XCom serialisation across operator boundaries
# ═════════════════════════════════════════════════════════════════════════

class TestRunContextXcom:

    def test_run_context_roundtrip_preserves_all_fields(
        self, make_yaml, airflow_context, mock_openbao,
        mock_mongo_no_checkpoint, mock_record_count
    ):
        """RunContext survives to_dict → XCom → from_dict with all fields intact."""
        init_op = make_init(make_yaml(SQL_TO_HADOOP_CFG))
        serialised = init_op.execute(airflow_context)

        restored = RunContext.from_dict(serialised)

        assert restored.dag_id == "test_pipeline"
        assert restored.read_type == ReadAdapterType.SQL
        assert restored.write_type == WriteAdapterType.HADOOP
        assert restored.metric_type == MetricAdapterType.ONPREM_QUEUE
        assert restored.source_credentials["username"] == "pg_user"
        assert restored.sink_credentials["hdfs_user"] == "hadoop"
        assert restored.metric_credentials["password"] == "redis_pass"
        assert restored.metric_config_raw["stream_name"] == "pipeline-metrics"
        assert restored.checkpoint_from is None

    def test_run_context_source_config_type_preserved(
        self, make_yaml, airflow_context, mock_openbao,
        mock_mongo_no_checkpoint, mock_record_count
    ):
        """SQL source → TableSourceConfig, S3 source → PathSourceConfig after roundtrip."""
        init_op = make_init(make_yaml(SQL_TO_HADOOP_CFG))
        serialised = init_op.execute(airflow_context)
        restored = RunContext.from_dict(serialised)
        assert isinstance(restored.source_config, TableSourceConfig)

        init_op2 = make_init(make_yaml(S3_TO_S3_CFG))
        serialised2 = init_op2.execute(airflow_context)
        restored2 = RunContext.from_dict(serialised2)
        assert isinstance(restored2.source_config, PathSourceConfig)

    def test_run_context_ingestion_dates_correct(
        self, make_yaml, airflow_context, mock_openbao,
        mock_mongo_no_checkpoint, mock_record_count
    ):
        init_op = make_init(make_yaml(SQL_TO_HADOOP_CFG))
        serialised = init_op.execute(airflow_context)
        restored = RunContext.from_dict(serialised)

        assert restored.ingestion_date == date(2024, 1, 15)
        assert restored.ingestion_time == datetime(2024, 1, 15, 14, 30, 0)
