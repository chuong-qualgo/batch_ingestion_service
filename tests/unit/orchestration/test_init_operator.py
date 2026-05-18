"""
Unit tests for InitOperator and RunContext.

All external I/O is mocked:
  - OpenBaoHook.get_secret
  - InitOperator._fetch_checkpoint_from  (MongoDB)
  - InitOperator._fetch_checkpoint_to  (Spark)
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
        # host and port come from OpenBao, not YAML
        "database": "orders_db",
        "schema": "public",
        "table": "orders",
        "checkpoint_column": "updated_at",
    },
    "sink": {
        "credential_ref": "data-platform/hadoop",
        # endpoint comes from OpenBao, not YAML
        "source_system_name": "postgres-prod",
    },
    "metric": {
        "credential_ref": "data-platform/redis",
        # host and port come from OpenBao, not YAML
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
        checkpoint_conn_id="postgres_checkpoint",
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
        operator, "_fetch_checkpoint_to", return_value=100
    ):
        mock_hook = MagicMock()
        mock_hook.get_secret.side_effect = lambda ref: {
            "data-processor/postgres": {"host": "localhost", "port": "5432", "username": "pg", "password": "pg_pass"},
            "data-platform/hadoop":    {"endpoint": "hdfs://namenode:9000", "hdfs_user": "hadoop"},
            "data-platform/redis":     {"host": "redis-host", "port": "6379", "password": "redis_pass"},
        }[ref]
        mock_hook_cls.return_value = mock_hook

        operator.execute(airflow_context)

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
        operator, "_fetch_checkpoint_to", return_value=0
    ):
        mock_hook = MagicMock()
        mock_hook.get_secret.side_effect = lambda ref: {
            "data-processor/postgres": {"host": "localhost", "port": "5432", "username": "pg", "password": "pg_pass"},
            "data-platform/hadoop":    {"endpoint": "hdfs://namenode:9000", "hdfs_user": "hadoop"},
            "data-platform/redis":     {"host": "redis-host", "port": "6379", "password": "redis_pass"},
        }[ref]
        mock_hook_cls.return_value = mock_hook

        result = operator.execute(airflow_context)

        assert result["metric_credentials"]["password"] == "redis_pass"
        assert result["metric_credentials"]["host"] == "redis-host"


def test_execute_metric_config_raw_in_run_context(operator, airflow_context):
    """metric_config_raw (full YAML metric section) must be in the XCom payload."""
    with patch.object(
        operator, "_load_config", return_value=BASE_CFG
    ), patch(
        "orchestration.operators.init_operator.OpenBaoHook"
    ) as mock_hook_cls, patch.object(
        operator, "_fetch_checkpoint_from", return_value=None
    ), patch.object(
        operator, "_fetch_checkpoint_to", return_value=0
    ):
        mock_hook = MagicMock()
        mock_hook.get_secret.side_effect = lambda ref: {
            "data-processor/postgres": {"host": "localhost", "port": "5432", "username": "pg", "password": "pg_pass"},
            "data-platform/hadoop":    {"endpoint": "hdfs://namenode:9000", "hdfs_user": "hadoop"},
            "data-platform/redis":     {"host": "redis-host", "port": "6379", "password": "redis_pass"},
        }.get(ref, {"key": "val"})
        mock_hook_cls.return_value = mock_hook

        result = operator.execute(airflow_context)

        assert result["metric_config_raw"]["stream_name"] == "pipeline-metrics"
        # host is no longer in YAML/metric_config_raw — it comes from OpenBao credentials
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
        operator, "_fetch_checkpoint_to", return_value=0
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
        operator, "_fetch_checkpoint_to", return_value=0
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
        operator, "_fetch_checkpoint_from", return_value=42
    ), patch.object(
        operator, "_fetch_checkpoint_to", return_value=1000
    ):
        mock_hook = MagicMock()
        mock_hook.get_secret.side_effect = lambda ref: {
            "data-processor/postgres": {"host": "localhost", "port": "5432", "username": "pg", "password": "pg_pass"},
            "data-platform/hadoop":    {"endpoint": "hdfs://namenode:9000", "hdfs_user": "hadoop"},
            "data-platform/redis":     {"host": "redis-host", "port": "6379", "password": "redis_pass"},
        }.get(ref, {"key": "val"})
        mock_hook_cls.return_value = mock_hook

        result = operator.execute(airflow_context)

    required_keys = [
        "dag_id", "run_id", "ingestion_date", "ingestion_time",
        "read_type", "write_type", "metric_type",
        "source_config", "sink_config",
        "source_credentials", "sink_credentials",
        "metric_credentials", "metric_config_raw",
        "checkpoint_from",
        "checkpoint_to",
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
        operator, "_fetch_checkpoint_to", return_value=0
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


def test_execute_checkpoint_from_int_in_result(operator, airflow_context):
    """Integer checkpoint_from is serialised as {"t": "int", "v": <n>} in XCom."""
    with patch.object(
        operator, "_load_config", return_value=BASE_CFG
    ), patch(
        "orchestration.operators.init_operator.OpenBaoHook"
    ) as mock_hook_cls, patch.object(
        operator, "_fetch_checkpoint_from", return_value=500
    ), patch.object(
        operator, "_fetch_checkpoint_to", return_value=0
    ):
        mock_hook = MagicMock()
        mock_hook.get_secret.return_value = {}
        mock_hook_cls.return_value = mock_hook

        result = operator.execute(airflow_context)

    assert result["checkpoint_from"] == {"t": "int", "v": 500}


def test_execute_checkpoint_from_datetime_in_result(operator, airflow_context):
    """Datetime checkpoint_from is serialised as {"t": "ts", "v": <iso>} in XCom."""
    ckpt_dt = datetime(2024, 1, 10, 8, 0, 0)
    with patch.object(
        operator, "_load_config", return_value=BASE_CFG
    ), patch(
        "orchestration.operators.init_operator.OpenBaoHook"
    ) as mock_hook_cls, patch.object(
        operator, "_fetch_checkpoint_from", return_value=ckpt_dt
    ), patch.object(
        operator, "_fetch_checkpoint_to", return_value=0
    ):
        mock_hook = MagicMock()
        mock_hook.get_secret.return_value = {}
        mock_hook_cls.return_value = mock_hook

        result = operator.execute(airflow_context)

    assert result["checkpoint_from"] == {"t": "ts", "v": ckpt_dt.isoformat()}


def test_execute_no_checkpoint_returns_none(operator, airflow_context):
    with patch.object(
        operator, "_load_config", return_value=BASE_CFG
    ), patch(
        "orchestration.operators.init_operator.OpenBaoHook"
    ) as mock_hook_cls, patch.object(
        operator, "_fetch_checkpoint_from", return_value=None
    ), patch.object(
        operator, "_fetch_checkpoint_to", return_value=0
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
        checkpoint_from=datetime(2024, 1, 1),
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
        checkpoint_from=datetime(2024, 1, 10),
    )

    restored = RunContext.from_dict(ctx.to_dict())

    assert restored.metric_credentials == {"password": "redis_pass"}
    assert restored.metric_config_raw["stream_name"] == "metrics"
    assert restored.metric_type == MetricAdapterType.ONPREM_QUEUE
    assert restored.checkpoint_from == datetime(2024, 1, 10)


def test_run_context_checkpoint_int_roundtrip(table_source_config, sink_config):
    """Integer checkpoint values survive to_dict() → from_dict() unchanged."""
    ctx = RunContext(
        dag_id="d", run_id="r",
        ingestion_date=date(2024, 1, 1),
        ingestion_time=datetime(2024, 1, 1),
        read_type=ReadAdapterType.SQL, write_type=WriteAdapterType.HADOOP,
        metric_type=MetricAdapterType.CLOUD_QUEUE,
        source_config=table_source_config, sink_config=sink_config,
        source_credentials={}, sink_credentials={},
        metric_credentials={}, metric_config_raw={},
        checkpoint_from=100, checkpoint_to=999,
    )
    restored = RunContext.from_dict(ctx.to_dict())
    assert restored.checkpoint_from == 100
    assert restored.checkpoint_to == 999


def test_run_context_checkpoint_datetime_roundtrip(table_source_config, sink_config):
    """Datetime checkpoint values survive to_dict() → from_dict() unchanged."""
    dt_from = datetime(2024, 3, 1, 0, 0, 0)
    dt_to   = datetime(2024, 3, 31, 23, 59, 59)
    ctx = RunContext(
        dag_id="d", run_id="r",
        ingestion_date=date(2024, 3, 1),
        ingestion_time=datetime(2024, 3, 1),
        read_type=ReadAdapterType.SQL, write_type=WriteAdapterType.HADOOP,
        metric_type=MetricAdapterType.CLOUD_QUEUE,
        source_config=table_source_config, sink_config=sink_config,
        source_credentials={}, sink_credentials={},
        metric_credentials={}, metric_config_raw={},
        checkpoint_from=dt_from, checkpoint_to=dt_to,
    )
    restored = RunContext.from_dict(ctx.to_dict())
    assert restored.checkpoint_from == dt_from
    assert restored.checkpoint_to == dt_to


def test_run_context_checkpoint_none_roundtrip(table_source_config, sink_config):
    """None checkpoints (full-read mode) survive serialisation as None."""
    ctx = RunContext(
        dag_id="d", run_id="r",
        ingestion_date=date(2024, 1, 1),
        ingestion_time=datetime(2024, 1, 1),
        read_type=ReadAdapterType.SQL, write_type=WriteAdapterType.HADOOP,
        metric_type=MetricAdapterType.CLOUD_QUEUE,
        source_config=table_source_config, sink_config=sink_config,
        source_credentials={}, sink_credentials={},
        metric_credentials={}, metric_config_raw={},
    )
    restored = RunContext.from_dict(ctx.to_dict())
    assert restored.checkpoint_from is None
    assert restored.checkpoint_to is None


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


# ── _fetch_checkpoint_from (PostgreSQL) ──────────────────────────────────

class TestFetchCheckpointFromMongo:
    """Tests for InitOperator._fetch_checkpoint_from (PostgreSQL implementation)."""

    def _make_mock_conn_cfg(self, port=5432):
        conn = MagicMock()
        conn.host = "pg-host"
        conn.port = port
        conn.login = "pg_user"
        conn.password = "pg_pass"
        conn.schema = "checkpoints_db"
        return conn

    def _make_mock_pg(self, fetchone_return):
        mock_conn = MagicMock()
        cur = mock_conn.cursor.return_value.__enter__.return_value
        cur.fetchone.return_value = fetchone_return
        return mock_conn, cur

    def test_returns_int_checkpoint_from_doc(self):
        mock_pg, cur = self._make_mock_pg(fetchone_return=("999",))
        with patch(
            "orchestration.operators.init_operator.BaseHook.get_connection",
            return_value=self._make_mock_conn_cfg(),
        ), patch(
            "orchestration.operators.init_operator.psycopg2.connect",
            return_value=mock_pg,
        ):
            result = InitOperator._fetch_checkpoint_from("test_dag", "postgres_checkpoint")
        assert result == 999

    def test_returns_datetime_checkpoint_from_doc(self):
        dt = datetime(2024, 3, 31, 23, 59, 59)
        mock_pg, cur = self._make_mock_pg(fetchone_return=(dt.isoformat(),))
        with patch(
            "orchestration.operators.init_operator.BaseHook.get_connection",
            return_value=self._make_mock_conn_cfg(),
        ), patch(
            "orchestration.operators.init_operator.psycopg2.connect",
            return_value=mock_pg,
        ):
            result = InitOperator._fetch_checkpoint_from("test_dag", "postgres_checkpoint")
        assert result == dt

    def test_returns_none_when_no_document(self):
        mock_pg, cur = self._make_mock_pg(fetchone_return=None)
        with patch(
            "orchestration.operators.init_operator.BaseHook.get_connection",
            return_value=self._make_mock_conn_cfg(),
        ), patch(
            "orchestration.operators.init_operator.psycopg2.connect",
            return_value=mock_pg,
        ):
            result = InitOperator._fetch_checkpoint_from("test_dag", "postgres_checkpoint")
        assert result is None

    def test_queries_by_dag_id(self):
        mock_pg, cur = self._make_mock_pg(fetchone_return=None)
        with patch(
            "orchestration.operators.init_operator.BaseHook.get_connection",
            return_value=self._make_mock_conn_cfg(),
        ), patch(
            "orchestration.operators.init_operator.psycopg2.connect",
            return_value=mock_pg,
        ):
            InitOperator._fetch_checkpoint_from("my_dag", "postgres_checkpoint")

        sql_call = cur.execute.call_args[0]
        assert "dag_id" in sql_call[0]
        assert sql_call[1] == ("my_dag",)

    def test_uses_conn_schema_as_database(self):
        conn_cfg = self._make_mock_conn_cfg()
        conn_cfg.schema = "pipeline_config"
        mock_pg, _ = self._make_mock_pg(fetchone_return=None)
        with patch(
            "orchestration.operators.init_operator.BaseHook.get_connection",
            return_value=conn_cfg,
        ), patch(
            "orchestration.operators.init_operator.psycopg2.connect",
            return_value=mock_pg,
        ) as mock_connect:
            InitOperator._fetch_checkpoint_from("dag", "postgres_checkpoint")

        assert mock_connect.call_args[1]["dbname"] == "pipeline_config"

    def test_defaults_port_to_5432_when_zero(self):
        mock_pg, _ = self._make_mock_pg(fetchone_return=None)
        with patch(
            "orchestration.operators.init_operator.BaseHook.get_connection",
            return_value=self._make_mock_conn_cfg(port=0),
        ), patch(
            "orchestration.operators.init_operator.psycopg2.connect",
            return_value=mock_pg,
        ) as mock_connect:
            InitOperator._fetch_checkpoint_from("dag", "postgres_checkpoint")

        assert mock_connect.call_args[1]["port"] == 5432

    def test_client_closed_after_query(self):
        mock_pg, _ = self._make_mock_pg(fetchone_return=None)
        with patch(
            "orchestration.operators.init_operator.BaseHook.get_connection",
            return_value=self._make_mock_conn_cfg(),
        ), patch(
            "orchestration.operators.init_operator.psycopg2.connect",
            return_value=mock_pg,
        ):
            InitOperator._fetch_checkpoint_from("dag", "postgres_checkpoint")

        mock_pg.close.assert_called_once()

    def test_client_closed_even_on_error(self):
        mock_pg = MagicMock()
        mock_pg.cursor.return_value.__enter__.return_value.execute.side_effect = RuntimeError("db error")
        with patch(
            "orchestration.operators.init_operator.BaseHook.get_connection",
            return_value=self._make_mock_conn_cfg(),
        ), patch(
            "orchestration.operators.init_operator.psycopg2.connect",
            return_value=mock_pg,
        ):
            with pytest.raises(RuntimeError):
                InitOperator._fetch_checkpoint_from("dag", "postgres_checkpoint")

        mock_pg.close.assert_called_once()

    def test_connects_with_airflow_connection_params(self):
        mock_pg, _ = self._make_mock_pg(fetchone_return=None)
        with patch(
            "orchestration.operators.init_operator.BaseHook.get_connection",
            return_value=self._make_mock_conn_cfg(),
        ) as mock_get_conn, patch(
            "orchestration.operators.init_operator.psycopg2.connect",
            return_value=mock_pg,
        ) as mock_connect:
            InitOperator._fetch_checkpoint_from("dag", "postgres_checkpoint")

        mock_get_conn.assert_called_once_with("postgres_checkpoint")
        mock_connect.assert_called_once_with(
            host="pg-host",
            port=5432,
            dbname="checkpoints_db",
            user="pg_user",
            password="pg_pass",
        )
