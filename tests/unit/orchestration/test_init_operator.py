"""
Unit tests for InitOperator and RunContext.

All external I/O is mocked:
  - OpenBaoHook.get_secret
  - InitOperator._fetch_checkpoint_from  (MongoDB)
  - InitOperator._fetch_source_record_count  (Spark)
  - InitOperator._load_config  (YAML file)
  - Airflow context (dag, run_id, xcom)
"""
import pytest
import tempfile
import os
import yaml
from datetime import date, datetime
from unittest.mock import MagicMock, patch, call

from adapters.factory.adapter_config import (
    ReadAdapterType, WriteAdapterType, MetricAdapterType
)
from adapters.source.base_read_adapter import TableSourceConfig, PathSourceConfig
from orchestration.operators.init_operator import InitOperator
from orchestration.operators.run_context import RunContext


# ── Shared YAML config fixture ────────────────────────────────────────────

BASE_CFG = {
    "read_type": "sql",
    "write_type": "hadoop",
    "metric_type": "onprem_queue",
    "source": {
        "credential_ref": "data-processor/postgres",
        "host": "localhost",
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
        "host": "redis.infra.svc.cluster.local",
        "port": 6379,
        "stream_name": "pipeline-metrics",
        "max_len": 5000,
    },
}


@pytest.fixture
def yaml_config_file():
    """Write BASE_CFG to a temp YAML file and return the path."""
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", delete=False
    ) as f:
        yaml.dump(BASE_CFG, f)
        path = f.name
    yield path
    os.unlink(path)


@pytest.fixture
def yaml_config_no_metric():
    """YAML with no metric section."""
    cfg = {k: v for k, v in BASE_CFG.items() if k != "metric"}
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(cfg, f)
        path = f.name
    yield path
    os.unlink(path)


@pytest.fixture
def operator(yaml_config_file):
    return InitOperator(
        task_id="init",
        config_path=yaml_config_file,
        mongo_conn_id="mongo_checkpoint",
        openbao_conn_id="openbao_default",
    )


@pytest.fixture
def airflow_context():
    """Minimal Airflow context dict."""
    ctx = {
        "dag": MagicMock(dag_id="test_dag"),
        "run_id": "scheduled__2024-01-15T14:30:00",
        "data_interval_start": datetime(2024, 1, 15, 14, 30, 0),
        "ti": MagicMock(),
    }
    return ctx


# ── _load_config ──────────────────────────────────────────────────────────

def test_load_config_parses_yaml(yaml_config_file):
    cfg = InitOperator._load_config(yaml_config_file)
    assert cfg["read_type"] == "sql"
    assert cfg["source"]["table"] == "orders"
    assert cfg["metric"]["stream_name"] == "pipeline-metrics"


def test_load_config_missing_file_raises():
    with pytest.raises(FileNotFoundError):
        InitOperator._load_config("/nonexistent/path.yaml")


# ── execute — credential fetching ─────────────────────────────────────────

def test_execute_fetches_metric_credentials(operator, airflow_context):
    """Step 6: metric credentials are fetched from OpenBao."""
    with patch.object(
        operator, "_load_config", return_value=BASE_CFG
    ), patch(
        "orchestration.operators.init_operator.OpenBaoHook"
    ) as mock_hook_cls, patch.object(
        operator, "_fetch_checkpoint_from", return_value=None
    ), patch.object(
        operator, "_fetch_source_record_count", return_value=100
    ):
        mock_hook = MagicMock()
        mock_hook.get_secret.side_effect = lambda ref: {
            "data-processor/postgres": {"username": "pg", "password": "pg_pass"},
            "data-platform/hadoop":    {"hdfs_user": "hadoop"},
            "data-platform/redis":     {"password": "redis_pass"},
        }[ref]
        mock_hook_cls.return_value = mock_hook

        operator.execute(airflow_context)

        # All three credential refs must be fetched
        calls = [c[0][0] for c in mock_hook.get_secret.call_args_list]
        assert "data-processor/postgres" in calls
        assert "data-platform/hadoop" in calls
        assert "data-platform/redis" in calls


def test_execute_metric_credentials_in_run_context(operator, airflow_context):
    """metric_credentials must be stored in the XCom RunContext payload."""
    with patch.object(
        operator, "_load_config", return_value=BASE_CFG
    ), patch(
        "orchestration.operators.init_operator.OpenBaoHook"
    ) as mock_hook_cls, patch.object(
        operator, "_fetch_checkpoint_from", return_value=None
    ), patch.object(
        operator, "_fetch_source_record_count", return_value=0
    ):
        mock_hook = MagicMock()
        mock_hook.get_secret.side_effect = lambda ref: {
            "data-processor/postgres": {"username": "pg", "password": "pg_pass"},
            "data-platform/hadoop":    {"hdfs_user": "hadoop"},
            "data-platform/redis":     {"password": "redis_pass"},
        }[ref]
        mock_hook_cls.return_value = mock_hook

        result = operator.execute(airflow_context)

        assert result["metric_credentials"] == {"password": "redis_pass"}


def test_execute_metric_config_raw_in_run_context(operator, airflow_context):
    """metric_config_raw (full YAML metric section) must be in the XCom payload."""
    with patch.object(
        operator, "_load_config", return_value=BASE_CFG
    ), patch(
        "orchestration.operators.init_operator.OpenBaoHook"
    ) as mock_hook_cls, patch.object(
        operator, "_fetch_checkpoint_from", return_value=None
    ), patch.object(
        operator, "_fetch_source_record_count", return_value=0
    ):
        mock_hook = MagicMock()
        mock_hook.get_secret.return_value = {"key": "val"}
        mock_hook_cls.return_value = mock_hook

        result = operator.execute(airflow_context)

        assert result["metric_config_raw"]["stream_name"] == "pipeline-metrics"
        assert result["metric_config_raw"]["host"] == "redis.infra.svc.cluster.local"
        assert result["metric_config_raw"]["credential_ref"] == "data-platform/redis"


def test_execute_no_metric_section_skips_credential_fetch(
    operator, airflow_context, yaml_config_no_metric
):
    """If no metric section in YAML, metric credentials should be empty dict."""
    operator.config_path = yaml_config_no_metric
    cfg_no_metric = {k: v for k, v in BASE_CFG.items() if k != "metric"}

    with patch.object(
        operator, "_load_config", return_value=cfg_no_metric
    ), patch(
        "orchestration.operators.init_operator.OpenBaoHook"
    ) as mock_hook_cls, patch.object(
        operator, "_fetch_checkpoint_from", return_value=None
    ), patch.object(
        operator, "_fetch_source_record_count", return_value=0
    ):
        mock_hook = MagicMock()
        mock_hook.get_secret.side_effect = lambda ref: {
            "data-processor/postgres": {"username": "pg", "password": "pg_pass"},
            "data-platform/hadoop":    {"hdfs_user": "hadoop"},
        }[ref]
        mock_hook_cls.return_value = mock_hook

        result = operator.execute(airflow_context)

        # metric section absent → credentials empty, config_raw empty
        assert result["metric_credentials"] == {}
        assert result["metric_config_raw"] == {}

        # OpenBao should only be called for source and sink, NOT for metric
        calls = [c[0][0] for c in mock_hook.get_secret.call_args_list]
        assert "data-platform/redis" not in calls
        assert len(calls) == 2


def test_execute_metric_credential_ref_missing_skips(operator, airflow_context):
    """metric section present but no credential_ref → no OpenBao call for metric."""
    cfg_no_ref = {
        **BASE_CFG,
        "metric": {
            "host": "redis",
            "port": 6379,
            "stream_name": "metrics",
            # no credential_ref
        },
    }
    with patch.object(
        operator, "_load_config", return_value=cfg_no_ref
    ), patch(
        "orchestration.operators.init_operator.OpenBaoHook"
    ) as mock_hook_cls, patch.object(
        operator, "_fetch_checkpoint_from", return_value=None
    ), patch.object(
        operator, "_fetch_source_record_count", return_value=0
    ):
        mock_hook = MagicMock()
        mock_hook.get_secret.side_effect = lambda ref: {
            "data-processor/postgres": {"username": "pg", "password": "pg_pass"},
            "data-platform/hadoop":    {"hdfs_user": "hadoop"},
        }.get(ref, {})
        mock_hook_cls.return_value = mock_hook

        result = operator.execute(airflow_context)

        assert result["metric_credentials"] == {}
        calls = [c[0][0] for c in mock_hook.get_secret.call_args_list]
        assert len(calls) == 2   # source + sink only


# ── execute — full RunContext shape ───────────────────────────────────────

def test_execute_run_context_has_all_fields(operator, airflow_context):
    """Returned dict must contain all RunContext fields."""
    with patch.object(
        operator, "_load_config", return_value=BASE_CFG
    ), patch(
        "orchestration.operators.init_operator.OpenBaoHook"
    ) as mock_hook_cls, patch.object(
        operator, "_fetch_checkpoint_from", return_value="2024-01-01 00:00:00"
    ), patch.object(
        operator, "_fetch_source_record_count", return_value=42
    ):
        mock_hook = MagicMock()
        mock_hook.get_secret.return_value = {"key": "val"}
        mock_hook_cls.return_value = mock_hook

        result = operator.execute(airflow_context)

    required_keys = [
        "dag_id", "run_id", "ingestion_date", "ingestion_time",
        "read_type", "write_type", "metric_type",
        "source_config", "sink_config",
        "source_credentials", "sink_credentials",
        "metric_credentials", "metric_config_raw",
        "checkpoint_from",
    ]
    for key in required_keys:
        assert key in result, f"Missing key in RunContext: {key}"


def test_execute_xcom_push_called(operator, airflow_context):
    """XCom push must be called exactly once with the correct key."""
    with patch.object(
        operator, "_load_config", return_value=BASE_CFG
    ), patch(
        "orchestration.operators.init_operator.OpenBaoHook"
    ) as mock_hook_cls, patch.object(
        operator, "_fetch_checkpoint_from", return_value=None
    ), patch.object(
        operator, "_fetch_source_record_count", return_value=0
    ):
        mock_hook = MagicMock()
        mock_hook.get_secret.return_value = {}
        mock_hook_cls.return_value = mock_hook

        operator.execute(airflow_context)

    # Verify xcom_push was called once with the correct key
    assert airflow_context["ti"].xcom_push.call_count == 1
    call_kwargs = airflow_context["ti"].xcom_push.call_args[1]
    assert call_kwargs["key"] == "run_context"
    assert isinstance(call_kwargs["value"], dict)


def test_execute_checkpoint_from_in_result(operator, airflow_context):
    """checkpoint_from fetched from MongoDB must appear in the result."""
    with patch.object(
        operator, "_load_config", return_value=BASE_CFG
    ), patch(
        "orchestration.operators.init_operator.OpenBaoHook"
    ) as mock_hook_cls, patch.object(
        operator, "_fetch_checkpoint_from", return_value="2024-01-10 08:00:00"
    ), patch.object(
        operator, "_fetch_source_record_count", return_value=0
    ):
        mock_hook = MagicMock()
        mock_hook.get_secret.return_value = {}
        mock_hook_cls.return_value = mock_hook

        result = operator.execute(airflow_context)

    assert result["checkpoint_from"] == "2024-01-10 08:00:00"


def test_execute_no_checkpoint_returns_none(operator, airflow_context):
    with patch.object(
        operator, "_load_config", return_value=BASE_CFG
    ), patch(
        "orchestration.operators.init_operator.OpenBaoHook"
    ) as mock_hook_cls, patch.object(
        operator, "_fetch_checkpoint_from", return_value=None
    ), patch.object(
        operator, "_fetch_source_record_count", return_value=0
    ):
        mock_hook = MagicMock()
        mock_hook.get_secret.return_value = {}
        mock_hook_cls.return_value = mock_hook

        result = operator.execute(airflow_context)

    assert result["checkpoint_from"] is None


# ── RunContext serialisation ──────────────────────────────────────────────

def test_run_context_to_dict_includes_metric_fields(
    table_source_config, sink_config
):
    ctx = RunContext(
        dag_id="test_dag",
        run_id="run-001",
        ingestion_date=date(2024, 1, 15),
        ingestion_time=datetime(2024, 1, 15, 14, 30, 0),
        read_type=ReadAdapterType.SQL,
        write_type=WriteAdapterType.HADOOP,
        metric_type=MetricAdapterType.ONPREM_QUEUE,
        source_config=table_source_config,
        sink_config=sink_config,
        source_credentials={"username": "pg"},
        sink_credentials={"hdfs_user": "hadoop"},
        metric_credentials={"password": "redis_pass"},
        metric_config_raw={
            "host": "redis",
            "stream_name": "metrics",
            "credential_ref": "data-platform/redis",
        },
        checkpoint_from="2024-01-01",
    )
    d = ctx.to_dict()
    assert d["metric_credentials"] == {"password": "redis_pass"}
    assert d["metric_config_raw"]["stream_name"] == "metrics"
    assert d["metric_type"] == "onprem_queue"


def test_run_context_from_dict_roundtrip(table_source_config, sink_config):
    """to_dict() → from_dict() must preserve metric fields."""
    ctx = RunContext(
        dag_id="test_dag",
        run_id="run-001",
        ingestion_date=date(2024, 1, 15),
        ingestion_time=datetime(2024, 1, 15, 14, 30, 0),
        read_type=ReadAdapterType.SQL,
        write_type=WriteAdapterType.HADOOP,
        metric_type=MetricAdapterType.ONPREM_QUEUE,
        source_config=table_source_config,
        sink_config=sink_config,
        source_credentials={"username": "pg"},
        sink_credentials={"hdfs_user": "hadoop"},
        metric_credentials={"password": "redis_pass"},
        metric_config_raw={
            "host": "redis",
            "stream_name": "metrics",
            "credential_ref": "data-platform/redis",
        },
        checkpoint_from="2024-01-10",
    )

    restored = RunContext.from_dict(ctx.to_dict())

    assert restored.metric_credentials == {"password": "redis_pass"}
    assert restored.metric_config_raw["stream_name"] == "metrics"
    assert restored.metric_type == MetricAdapterType.ONPREM_QUEUE
    assert restored.checkpoint_from == "2024-01-10"


def test_run_context_from_dict_defaults_empty_metric(table_source_config, sink_config):
    """from_dict() must default metric fields to {} if absent (backwards compat)."""
    ctx = RunContext(
        dag_id="test_dag",
        run_id="run-001",
        ingestion_date=date(2024, 1, 15),
        ingestion_time=datetime(2024, 1, 15, 14, 30, 0),
        read_type=ReadAdapterType.SQL,
        write_type=WriteAdapterType.HADOOP,
        metric_type=MetricAdapterType.CLOUD_QUEUE,
        source_config=table_source_config,
        sink_config=sink_config,
        source_credentials={},
        sink_credentials={},
        metric_credentials={},
        metric_config_raw={},
    )
    d = ctx.to_dict()
    # Simulate old XCom payload without metric fields
    d.pop("metric_credentials", None)
    d.pop("metric_config_raw", None)

    restored = RunContext.from_dict(d)
    assert restored.metric_credentials == {}
    assert restored.metric_config_raw == {}
