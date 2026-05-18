"""
Unit tests for MetricPushOperator, push_metric_inline, and _build_metric_adapter.

All external I/O is mocked:
  - OpenBaoHook.get_secret
  - MetricAdapterFactory.create / adapter methods
  - BaseHook.get_connection  (Airflow connection for PostgreSQL checkpoint store)
  - psycopg2.connect         (PostgreSQL)
  - Airflow context (dag, run_id, xcom)
"""
import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, call

from adapters.factory.adapter_config import MetricAdapterType
from orchestration.operators.metric_operator import (
    MetricPushOperator,
    _build_metric_adapter,
    _push_redis_metric,
    push_metric_inline,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

METRIC_CONFIG_REDIS = {
    "credential_ref": "data-platform/redis",
    "stream_name": "pipeline-metrics",
    "max_len": 1000,
}

REDIS_METRIC_CONFIG = {
    "credential_ref": "data-platform/redis-events",
    "stream_name": "pipeline-events",
    "max_len": 5000,
}

REDIS_METRIC_CREDS = {
    "host": "redis-events-host",
    "port": "6380",
    "password": "events_pass",
}

METRIC_CONFIG_SQS = {
    "credential_ref": "data-platform/sqs",
    "message_attributes": {"env": "test"},
}

RAW_CONTEXT_INT_CKPT = {
    "checkpoint_to": {"t": "int", "v": 999},
}

RAW_CONTEXT_TS_CKPT = {
    "checkpoint_to": {"t": "ts", "v": "2024-03-31T23:59:59"},
}

RAW_CONTEXT_NO_CKPT = {}

RAW_CONTEXT_BOTH_CKPT = {
    "checkpoint_from": {"t": "int", "v": 100},
    "checkpoint_to":   {"t": "int", "v": 999},
}


_DAG_RUN_START = datetime(2024, 1, 15, 14, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def airflow_context():
    ctx = {
        "dag":                 MagicMock(dag_id="test_dag"),
        "run_id":              "scheduled__2024-01-15T14:30:00",
        "data_interval_start": datetime(2024, 1, 15, 14, 30, 0),
        "dag_run":             MagicMock(start_date=_DAG_RUN_START),
        "ti":                  MagicMock(),
    }
    return ctx


@pytest.fixture
def mock_adapter():
    adapter = MagicMock()
    adapter.validate_connection.return_value = True
    return adapter


def _make_operator(metric_config=None, checkpoint_conn_id=None, status="success", extra_payload=None):
    return MetricPushOperator(
        task_id="metric_push",
        init_task_id="init",
        metric_type=MetricAdapterType.ONPREM_QUEUE,
        metric_config_raw=metric_config or METRIC_CONFIG_REDIS,
        status=status,
        extra_payload=extra_payload,
        checkpoint_conn_id=checkpoint_conn_id,
    )


# ── _build_metric_adapter ─────────────────────────────────────────────────────

class TestBuildMetricAdapter:
    def test_builds_redis_adapter_for_onprem_queue(self):
        creds = {"host": "redis-host", "port": "6379", "password": "secret"}
        with patch(
            "orchestration.operators.metric_operator.OpenBaoHook"
        ) as mock_hook_cls, patch(
            "orchestration.operators.metric_operator.MetricAdapterFactory"
        ) as mock_factory:
            mock_hook_cls.return_value.get_secret.return_value = creds
            _build_metric_adapter(MetricAdapterType.ONPREM_QUEUE, METRIC_CONFIG_REDIS, "openbao_default")

        mock_factory.create.assert_called_once()
        call_kwargs = mock_factory.create.call_args[1]
        assert call_kwargs["metric_config"].host == "redis-host"
        assert call_kwargs["metric_config"].stream_name == "pipeline-metrics"

    def test_builds_sqs_adapter_for_cloud_queue(self):
        creds = {
            "queue_url": "https://sqs.ap-southeast-1.amazonaws.com/123/q",
            "aws_region": "ap-southeast-1",
        }
        with patch(
            "orchestration.operators.metric_operator.OpenBaoHook"
        ) as mock_hook_cls, patch(
            "orchestration.operators.metric_operator.MetricAdapterFactory"
        ) as mock_factory:
            mock_hook_cls.return_value.get_secret.return_value = creds
            _build_metric_adapter(MetricAdapterType.CLOUD_QUEUE, METRIC_CONFIG_SQS, "openbao_default")

        call_kwargs = mock_factory.create.call_args[1]
        assert call_kwargs["metric_config"].queue_url == creds["queue_url"]
        assert call_kwargs["metric_config"].aws_region == "ap-southeast-1"

    def test_raises_for_unsupported_metric_type(self):
        with patch("orchestration.operators.metric_operator.OpenBaoHook") as mock_hook_cls:
            mock_hook_cls.return_value.get_secret.return_value = {}
            with pytest.raises(ValueError, match="Unsupported metric_type"):
                _build_metric_adapter("bad_type", {}, "openbao_default")

    def test_empty_credential_ref_skips_openbao_call(self):
        config_no_ref = {"stream_name": "metrics"}
        with patch(
            "orchestration.operators.metric_operator.OpenBaoHook"
        ) as mock_hook_cls, patch(
            "orchestration.operators.metric_operator.MetricAdapterFactory"
        ):
            hook = MagicMock()
            mock_hook_cls.return_value = hook
            _build_metric_adapter(MetricAdapterType.ONPREM_QUEUE, config_no_ref, "openbao_default")

        hook.get_secret.assert_not_called()

    def test_message_attributes_passed_to_factory(self):
        config = {**METRIC_CONFIG_SQS}
        creds = {"queue_url": "https://sqs.example.com/q", "aws_region": "us-east-1"}
        with patch(
            "orchestration.operators.metric_operator.OpenBaoHook"
        ) as mock_hook_cls, patch(
            "orchestration.operators.metric_operator.MetricAdapterFactory"
        ) as mock_factory:
            mock_hook_cls.return_value.get_secret.return_value = creds
            _build_metric_adapter(MetricAdapterType.CLOUD_QUEUE, config, "openbao_default")

        call_kwargs = mock_factory.create.call_args[1]
        assert call_kwargs["message_attributes"] == {"env": "test"}


# ── push_metric_inline ────────────────────────────────────────────────────────

class TestPushMetricInline:
    def test_publishes_payload(self, mock_adapter):
        payload = {"status": "failed", "dag_id": "my_dag"}
        with patch(
            "orchestration.operators.metric_operator._build_metric_adapter",
            return_value=mock_adapter,
        ):
            push_metric_inline(payload, MetricAdapterType.ONPREM_QUEUE, METRIC_CONFIG_REDIS)

        mock_adapter.validate_connection.assert_called_once()
        mock_adapter.publish.assert_called_once_with(payload)

    def test_does_not_raise_on_adapter_error(self):
        with patch(
            "orchestration.operators.metric_operator._build_metric_adapter",
            side_effect=RuntimeError("connection refused"),
        ):
            push_metric_inline({"status": "failed"}, MetricAdapterType.ONPREM_QUEUE, METRIC_CONFIG_REDIS)
        # No exception propagated — fire-and-forget contract

    def test_does_not_raise_on_publish_error(self, mock_adapter):
        mock_adapter.publish.side_effect = RuntimeError("queue full")
        with patch(
            "orchestration.operators.metric_operator._build_metric_adapter",
            return_value=mock_adapter,
        ):
            push_metric_inline({"status": "failed"}, MetricAdapterType.ONPREM_QUEUE, METRIC_CONFIG_REDIS)


# ── MetricPushOperator.execute — payload ──────────────────────────────────────

class TestMetricPushOperatorPayload:
    def test_payload_contains_required_fields(self, airflow_context, mock_adapter):
        airflow_context["ti"].xcom_pull.return_value = RAW_CONTEXT_NO_CKPT
        op = _make_operator()

        with patch(
            "orchestration.operators.metric_operator._build_metric_adapter",
            return_value=mock_adapter,
        ):
            op.execute(airflow_context)

        published = mock_adapter.publish.call_args[0][0]
        assert published["dag_id"] == "test_dag"
        assert published["run_id"] == "scheduled__2024-01-15T14:30:00"
        assert published["status"] == "success"
        assert published["start_time"] == _DAG_RUN_START.isoformat()
        assert "stop_time" in published
        assert published["checkpoint_from"] is None
        assert published["checkpoint_to"] is None

    def test_status_field_reflects_operator_status(self, airflow_context, mock_adapter):
        airflow_context["ti"].xcom_pull.return_value = RAW_CONTEXT_NO_CKPT
        op = _make_operator(status="failed")

        with patch(
            "orchestration.operators.metric_operator._build_metric_adapter",
            return_value=mock_adapter,
        ):
            op.execute(airflow_context)

        assert mock_adapter.publish.call_args[0][0]["status"] == "failed"

    def test_extra_payload_merged_into_published_payload(self, airflow_context, mock_adapter):
        airflow_context["ti"].xcom_pull.return_value = RAW_CONTEXT_NO_CKPT
        op = _make_operator(extra_payload={"stage": "complete", "env": "prod"})

        with patch(
            "orchestration.operators.metric_operator._build_metric_adapter",
            return_value=mock_adapter,
        ):
            op.execute(airflow_context)

        published = mock_adapter.publish.call_args[0][0]
        assert published["stage"] == "complete"
        assert published["env"] == "prod"

    def test_int_checkpoint_to_extracted_as_string(self, airflow_context, mock_adapter):
        airflow_context["ti"].xcom_pull.return_value = RAW_CONTEXT_INT_CKPT
        op = _make_operator()

        with patch(
            "orchestration.operators.metric_operator._build_metric_adapter",
            return_value=mock_adapter,
        ), patch.object(op, "_save_checkpoint"):
            op.execute(airflow_context)

        published = mock_adapter.publish.call_args[0][0]
        assert published["checkpoint_to"] == "999"

    def test_timestamp_checkpoint_to_extracted_as_string(self, airflow_context, mock_adapter):
        airflow_context["ti"].xcom_pull.return_value = RAW_CONTEXT_TS_CKPT
        op = _make_operator()

        with patch(
            "orchestration.operators.metric_operator._build_metric_adapter",
            return_value=mock_adapter,
        ), patch.object(op, "_save_checkpoint"):
            op.execute(airflow_context)

        published = mock_adapter.publish.call_args[0][0]
        assert published["checkpoint_to"] == "2024-03-31T23:59:59"

    def test_int_checkpoint_from_extracted_as_string(self, airflow_context, mock_adapter):
        airflow_context["ti"].xcom_pull.return_value = RAW_CONTEXT_BOTH_CKPT
        op = _make_operator()

        with patch(
            "orchestration.operators.metric_operator._build_metric_adapter",
            return_value=mock_adapter,
        ), patch.object(op, "_save_checkpoint"):
            op.execute(airflow_context)

        published = mock_adapter.publish.call_args[0][0]
        assert published["checkpoint_from"] == "100"
        assert published["checkpoint_to"] == "999"

    def test_checkpoint_from_none_when_absent_in_context(self, airflow_context, mock_adapter):
        airflow_context["ti"].xcom_pull.return_value = RAW_CONTEXT_INT_CKPT
        op = _make_operator()

        with patch(
            "orchestration.operators.metric_operator._build_metric_adapter",
            return_value=mock_adapter,
        ), patch.object(op, "_save_checkpoint"):
            op.execute(airflow_context)

        assert mock_adapter.publish.call_args[0][0]["checkpoint_from"] is None

    def test_no_xcom_context_still_publishes(self, airflow_context, mock_adapter):
        airflow_context["ti"].xcom_pull.return_value = None
        op = _make_operator()

        with patch(
            "orchestration.operators.metric_operator._build_metric_adapter",
            return_value=mock_adapter,
        ):
            op.execute(airflow_context)

        mock_adapter.publish.assert_called_once()
        published = mock_adapter.publish.call_args[0][0]
        assert published["dag_id"] == "test_dag"
        assert published["checkpoint_from"] is None
        assert published["checkpoint_to"] is None

    def test_validate_connection_called_before_publish(self, airflow_context, mock_adapter):
        airflow_context["ti"].xcom_pull.return_value = RAW_CONTEXT_NO_CKPT
        call_order = []
        mock_adapter.validate_connection.side_effect = lambda: call_order.append("validate")
        mock_adapter.publish.side_effect = lambda p: call_order.append("publish")
        op = _make_operator()

        with patch(
            "orchestration.operators.metric_operator._build_metric_adapter",
            return_value=mock_adapter,
        ):
            op.execute(airflow_context)

        assert call_order == ["validate", "publish"]


# ── MetricPushOperator.execute — checkpoint save ──────────────────────────────

class TestMetricPushOperatorCheckpointSave:
    def test_saves_checkpoint_when_checkpoint_conn_id_set(self, airflow_context, mock_adapter):
        airflow_context["ti"].xcom_pull.return_value = RAW_CONTEXT_INT_CKPT
        op = _make_operator(checkpoint_conn_id="postgres_checkpoint")

        with patch(
            "orchestration.operators.metric_operator._build_metric_adapter",
            return_value=mock_adapter,
        ), patch.object(op, "_save_checkpoint") as mock_save:
            op.execute(airflow_context)

        mock_save.assert_called_once_with("test_dag", "999")

    def test_skips_checkpoint_when_checkpoint_conn_id_is_none(self, airflow_context, mock_adapter):
        airflow_context["ti"].xcom_pull.return_value = RAW_CONTEXT_INT_CKPT
        op = _make_operator(checkpoint_conn_id=None)

        with patch(
            "orchestration.operators.metric_operator._build_metric_adapter",
            return_value=mock_adapter,
        ), patch.object(op, "_save_checkpoint") as mock_save:
            op.execute(airflow_context)

        mock_save.assert_not_called()

    def test_skips_checkpoint_when_checkpoint_to_is_absent(self, airflow_context, mock_adapter):
        airflow_context["ti"].xcom_pull.return_value = RAW_CONTEXT_NO_CKPT
        op = _make_operator(checkpoint_conn_id="postgres_checkpoint")

        with patch(
            "orchestration.operators.metric_operator._build_metric_adapter",
            return_value=mock_adapter,
        ), patch.object(op, "_save_checkpoint") as mock_save:
            op.execute(airflow_context)

        mock_save.assert_not_called()

    def test_skips_checkpoint_when_xcom_is_none(self, airflow_context, mock_adapter):
        airflow_context["ti"].xcom_pull.return_value = None
        op = _make_operator(checkpoint_conn_id="postgres_checkpoint")

        with patch(
            "orchestration.operators.metric_operator._build_metric_adapter",
            return_value=mock_adapter,
        ), patch.object(op, "_save_checkpoint") as mock_save:
            op.execute(airflow_context)

        mock_save.assert_not_called()

    def test_saves_timestamp_checkpoint_as_string(self, airflow_context, mock_adapter):
        airflow_context["ti"].xcom_pull.return_value = RAW_CONTEXT_TS_CKPT
        op = _make_operator(checkpoint_conn_id="postgres_checkpoint")

        with patch(
            "orchestration.operators.metric_operator._build_metric_adapter",
            return_value=mock_adapter,
        ), patch.object(op, "_save_checkpoint") as mock_save:
            op.execute(airflow_context)

        mock_save.assert_called_once_with("test_dag", "2024-03-31T23:59:59")

    def test_xcom_pull_uses_correct_task_id_and_key(self, airflow_context, mock_adapter):
        airflow_context["ti"].xcom_pull.return_value = RAW_CONTEXT_NO_CKPT
        op = MetricPushOperator(
            task_id="metric_push",
            init_task_id="my_init",
            metric_type=MetricAdapterType.ONPREM_QUEUE,
            metric_config_raw=METRIC_CONFIG_REDIS,
            xcom_key="my_context_key",
        )
        with patch(
            "orchestration.operators.metric_operator._build_metric_adapter",
            return_value=mock_adapter,
        ):
            op.execute(airflow_context)

        airflow_context["ti"].xcom_pull.assert_called_once_with(
            task_ids="my_init", key="my_context_key"
        )


# ── _save_checkpoint ──────────────────────────────────────────────────────────

class TestSaveCheckpoint:
    def _make_mock_conn_cfg(self, port=5432):
        conn = MagicMock()
        conn.host = "pg-host"
        conn.port = port
        conn.login = "pg_user"
        conn.password = "pg_pass"
        conn.schema = "checkpoints_db"
        return conn

    def test_connects_with_airflow_connection_params(self):
        op = _make_operator(checkpoint_conn_id="postgres_checkpoint")
        mock_conn_cfg = self._make_mock_conn_cfg()
        mock_conn = MagicMock()

        with patch(
            "orchestration.operators.metric_operator.BaseHook.get_connection",
            return_value=mock_conn_cfg,
        ) as mock_get_conn, patch(
            "orchestration.operators.metric_operator.psycopg2.connect",
            return_value=mock_conn,
        ) as mock_connect:
            op._save_checkpoint("my_dag", "999")

        mock_get_conn.assert_called_once_with("postgres_checkpoint")
        mock_connect.assert_called_once_with(
            host="pg-host",
            port=5432,
            dbname="checkpoints_db",
            user="pg_user",
            password="pg_pass",
        )

    def test_executes_upsert_with_correct_params(self):
        op = _make_operator(checkpoint_conn_id="postgres_checkpoint")
        mock_conn_cfg = self._make_mock_conn_cfg()
        mock_conn = MagicMock()

        with patch(
            "orchestration.operators.metric_operator.BaseHook.get_connection",
            return_value=mock_conn_cfg,
        ), patch(
            "orchestration.operators.metric_operator.psycopg2.connect",
            return_value=mock_conn,
        ):
            op._save_checkpoint("my_dag", "999")

        mock_cursor = mock_conn.cursor.return_value.__enter__.return_value
        sql, params = mock_cursor.execute.call_args[0]
        assert "INSERT INTO public.checkpoints" in sql
        assert "ON CONFLICT" in sql
        assert params == ("my_dag", "999")

    def test_commits_after_execute(self):
        op = _make_operator(checkpoint_conn_id="postgres_checkpoint")
        mock_conn_cfg = self._make_mock_conn_cfg()
        mock_conn = MagicMock()

        with patch(
            "orchestration.operators.metric_operator.BaseHook.get_connection",
            return_value=mock_conn_cfg,
        ), patch(
            "orchestration.operators.metric_operator.psycopg2.connect",
            return_value=mock_conn,
        ):
            op._save_checkpoint("my_dag", "999")

        mock_conn.commit.assert_called_once()

    def test_connection_closed_after_save(self):
        op = _make_operator(checkpoint_conn_id="postgres_checkpoint")
        mock_conn_cfg = self._make_mock_conn_cfg()
        mock_conn = MagicMock()

        with patch(
            "orchestration.operators.metric_operator.BaseHook.get_connection",
            return_value=mock_conn_cfg,
        ), patch(
            "orchestration.operators.metric_operator.psycopg2.connect",
            return_value=mock_conn,
        ):
            op._save_checkpoint("my_dag", "999")

        mock_conn.close.assert_called_once()

    def test_connection_closed_even_on_error(self):
        op = _make_operator(checkpoint_conn_id="postgres_checkpoint")
        mock_conn_cfg = self._make_mock_conn_cfg()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value.execute.side_effect = RuntimeError("write failed")

        with patch(
            "orchestration.operators.metric_operator.BaseHook.get_connection",
            return_value=mock_conn_cfg,
        ), patch(
            "orchestration.operators.metric_operator.psycopg2.connect",
            return_value=mock_conn,
        ):
            with pytest.raises(RuntimeError):
                op._save_checkpoint("my_dag", "100")

        mock_conn.close.assert_called_once()

    def test_defaults_port_to_5432_when_zero(self):
        op = _make_operator(checkpoint_conn_id="postgres_checkpoint")
        mock_conn_cfg = self._make_mock_conn_cfg(port=0)
        mock_conn = MagicMock()

        with patch(
            "orchestration.operators.metric_operator.BaseHook.get_connection",
            return_value=mock_conn_cfg,
        ), patch(
            "orchestration.operators.metric_operator.psycopg2.connect",
            return_value=mock_conn,
        ) as mock_connect:
            op._save_checkpoint("my_dag", "999")

        assert mock_connect.call_args[1]["port"] == 5432


# ── _push_redis_metric ────────────────────────────────────────────────────────

class TestPushRedisMetric:
    def test_builds_adapter_and_publishes(self):
        payload = {"dag_id": "d", "run_id": "r", "status": "success"}
        with patch(
            "orchestration.operators.metric_operator.RedisQueueAdapter"
        ) as mock_cls:
            mock_adapter = MagicMock()
            mock_adapter.validate_connection.return_value = True
            mock_cls.return_value = mock_adapter

            _push_redis_metric(payload, REDIS_METRIC_CONFIG, REDIS_METRIC_CREDS)

        mock_cls.assert_called_once()
        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs["config"].host == "redis-events-host"
        assert call_kwargs["config"].port == 6380
        assert call_kwargs["config"].stream_name == "pipeline-events"
        assert call_kwargs["config"].max_len == 5000
        mock_adapter.validate_connection.assert_called_once()
        mock_adapter.publish.assert_called_once_with(payload)

    def test_does_not_raise_on_connection_error(self):
        with patch(
            "orchestration.operators.metric_operator.RedisQueueAdapter",
            side_effect=RuntimeError("connection refused"),
        ):
            _push_redis_metric({"status": "ok"}, REDIS_METRIC_CONFIG, REDIS_METRIC_CREDS)

    def test_does_not_raise_on_publish_error(self):
        with patch(
            "orchestration.operators.metric_operator.RedisQueueAdapter"
        ) as mock_cls:
            mock_adapter = MagicMock()
            mock_adapter.publish.side_effect = RuntimeError("stream full")
            mock_cls.return_value = mock_adapter

            _push_redis_metric({"status": "ok"}, REDIS_METRIC_CONFIG, REDIS_METRIC_CREDS)

    def test_uses_credentials_for_host_port_password(self):
        creds = {"host": "my-redis", "port": "6399", "password": "pw"}
        with patch(
            "orchestration.operators.metric_operator.RedisQueueAdapter"
        ) as mock_cls:
            mock_cls.return_value.validate_connection.return_value = True
            _push_redis_metric({}, REDIS_METRIC_CONFIG, creds)

        cfg = mock_cls.call_args[1]["config"]
        assert cfg.host == "my-redis"
        assert cfg.port == 6399
        assert mock_cls.call_args[1]["credentials"] == creds

    def test_falls_back_to_config_raw_host_when_creds_missing(self):
        config_with_host = {**REDIS_METRIC_CONFIG, "host": "fallback-host", "port": 6500}
        with patch(
            "orchestration.operators.metric_operator.RedisQueueAdapter"
        ) as mock_cls:
            mock_cls.return_value.validate_connection.return_value = True
            _push_redis_metric({}, config_with_host, {})

        cfg = mock_cls.call_args[1]["config"]
        assert cfg.host == "fallback-host"
        assert cfg.port == 6500


# ── MetricPushOperator — secondary Redis push ─────────────────────────────────

class TestMetricPushOperatorRedisMetric:
    def _raw_context_with_redis(self, extra_ckpt=None):
        ctx = {
            "redis_metric_config_raw":   REDIS_METRIC_CONFIG,
            "redis_metric_credentials":  REDIS_METRIC_CREDS,
        }
        if extra_ckpt:
            ctx.update(extra_ckpt)
        return ctx

    def test_secondary_redis_push_called_when_config_present(self, airflow_context, mock_adapter):
        airflow_context["ti"].xcom_pull.return_value = self._raw_context_with_redis()
        op = _make_operator()

        with patch(
            "orchestration.operators.metric_operator._build_metric_adapter",
            return_value=mock_adapter,
        ), patch(
            "orchestration.operators.metric_operator._push_redis_metric"
        ) as mock_redis_push:
            op.execute(airflow_context)

        mock_redis_push.assert_called_once()
        args = mock_redis_push.call_args[0]
        assert args[1] == REDIS_METRIC_CONFIG
        assert args[2] == REDIS_METRIC_CREDS

    def test_secondary_redis_push_receives_full_payload(self, airflow_context, mock_adapter):
        airflow_context["ti"].xcom_pull.return_value = self._raw_context_with_redis()
        op = _make_operator()

        with patch(
            "orchestration.operators.metric_operator._build_metric_adapter",
            return_value=mock_adapter,
        ), patch(
            "orchestration.operators.metric_operator._push_redis_metric"
        ) as mock_redis_push:
            op.execute(airflow_context)

        payload = mock_redis_push.call_args[0][0]
        assert "dag_id" in payload
        assert "run_id" in payload
        assert "status" in payload
        assert "start_time" in payload
        assert "stop_time" in payload
        assert "checkpoint_from" in payload
        assert "checkpoint_to" in payload

    def test_secondary_redis_push_skipped_when_no_config(self, airflow_context, mock_adapter):
        airflow_context["ti"].xcom_pull.return_value = RAW_CONTEXT_NO_CKPT
        op = _make_operator()

        with patch(
            "orchestration.operators.metric_operator._build_metric_adapter",
            return_value=mock_adapter,
        ), patch(
            "orchestration.operators.metric_operator._push_redis_metric"
        ) as mock_redis_push:
            op.execute(airflow_context)

        mock_redis_push.assert_not_called()

    def test_secondary_redis_push_skipped_when_xcom_is_none(self, airflow_context, mock_adapter):
        airflow_context["ti"].xcom_pull.return_value = None
        op = _make_operator()

        with patch(
            "orchestration.operators.metric_operator._build_metric_adapter",
            return_value=mock_adapter,
        ), patch(
            "orchestration.operators.metric_operator._push_redis_metric"
        ) as mock_redis_push:
            op.execute(airflow_context)

        mock_redis_push.assert_not_called()

    def test_primary_publish_still_called_alongside_redis_push(self, airflow_context, mock_adapter):
        airflow_context["ti"].xcom_pull.return_value = self._raw_context_with_redis()
        op = _make_operator()

        with patch(
            "orchestration.operators.metric_operator._build_metric_adapter",
            return_value=mock_adapter,
        ), patch("orchestration.operators.metric_operator._push_redis_metric"):
            op.execute(airflow_context)

        mock_adapter.publish.assert_called_once()
