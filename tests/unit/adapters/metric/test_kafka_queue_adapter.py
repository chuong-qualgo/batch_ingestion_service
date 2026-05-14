"""Tests for KafkaQueueAdapter using unittest.mock."""
import json
import pytest
from unittest.mock import MagicMock, patch

from adapters.metric.kafka_queue_adapter import KafkaQueueAdapter
from adapters.metric.base_metric_adapter import KafkaMetricConfig


@pytest.fixture
def mock_producer():
    producer = MagicMock()
    producer.partitions_for.return_value = {0, 1, 2}
    return producer


@pytest.fixture
def adapter(kafka_metric_config, kafka_credentials, mock_producer, monkeypatch):
    kafka_metric_config.bootstrap_servers = kafka_credentials["bootstrap_servers"]
    inst = KafkaQueueAdapter(
        config=kafka_metric_config,
        credentials=kafka_credentials,
        message_attributes={"env": "test"},
    )
    monkeypatch.setattr(inst, "_get_producer", lambda: mock_producer)
    return inst


# ── build_message ─────────────────────────────────────────────────────────

def test_build_message_returns_bytes(adapter):
    assert isinstance(adapter.build_message({"status": "ok"}), bytes)


def test_build_message_contains_payload(adapter):
    parsed = json.loads(adapter.build_message({"dag_id": "my_dag", "count": 42}))
    assert parsed["dag_id"] == "my_dag"
    assert parsed["count"] == 42


def test_build_message_includes_message_attributes(adapter):
    parsed = json.loads(adapter.build_message({"x": 1}))
    assert parsed["env"] == "test"


def test_build_message_includes_published_at(adapter):
    parsed = json.loads(adapter.build_message({"x": 1}))
    assert "published_at" in parsed


def test_build_message_serialises_nested_values(adapter):
    parsed = json.loads(adapter.build_message({"nested": {"a": 1}, "lst": [1, 2]}))
    assert parsed["nested"] == {"a": 1}
    assert parsed["lst"] == [1, 2]


# ── validate_connection ───────────────────────────────────────────────────

def test_validate_connection_returns_true(adapter, mock_producer):
    assert adapter.validate_connection() is True
    mock_producer.partitions_for.assert_called_once_with("pipeline-metrics")
    mock_producer.close.assert_called_once()


def test_validate_connection_returns_false_when_no_partitions(adapter, mock_producer):
    mock_producer.partitions_for.return_value = None
    assert adapter.validate_connection() is False


def test_validate_connection_closes_producer_on_error(adapter, mock_producer):
    mock_producer.partitions_for.side_effect = Exception("unreachable")
    with pytest.raises(Exception):
        adapter.validate_connection()
    mock_producer.close.assert_called_once()


# ── publish ───────────────────────────────────────────────────────────────

def test_publish_sends_to_topic(adapter, mock_producer):
    adapter.publish({"status": "success", "dag_id": "test_dag"})
    mock_producer.send.assert_called_once()
    call_kwargs = mock_producer.send.call_args
    assert call_kwargs.args[0] == "pipeline-metrics"
    assert json.loads(call_kwargs.kwargs["value"])["dag_id"] == "test_dag"


def test_publish_encodes_key(adapter, mock_producer):
    adapter.publish({"x": 1})
    assert mock_producer.send.call_args.kwargs["key"] == b"batch-ingestion"


def test_publish_no_key_sends_none(kafka_credentials, mock_producer, monkeypatch):
    config = KafkaMetricConfig(
        credential_ref="ref",
        bootstrap_servers="broker:9092",
        topic="pipeline-metrics",
        key=None,
    )
    inst = KafkaQueueAdapter(config=config, credentials=kafka_credentials)
    monkeypatch.setattr(inst, "_get_producer", lambda: mock_producer)
    inst.publish({"x": 1})
    assert mock_producer.send.call_args.kwargs["key"] is None


def test_publish_flushes_and_closes(adapter, mock_producer):
    adapter.publish({"x": 1})
    mock_producer.flush.assert_called_once()
    mock_producer.close.assert_called_once()


def test_publish_calls_on_error_on_failure(adapter, mock_producer):
    mock_producer.send.side_effect = Exception("broker down")
    called = []
    adapter.on_error = lambda exc: called.append(exc)
    with pytest.raises(Exception, match="broker down"):
        adapter.publish({"x": 1})
    assert len(called) == 1


# ── on_error ─────────────────────────────────────────────────────────────

def test_on_error_logs(adapter, capsys):
    adapter.on_error(ConnectionError("timeout"))
    out = capsys.readouterr().out
    assert "KafkaQueueAdapter" in out
    assert "pipeline-metrics" in out


# ── _get_producer SASL wiring ─────────────────────────────────────────────

def test_get_producer_passes_sasl_credentials(kafka_metric_config, kafka_credentials):
    inst = KafkaQueueAdapter(config=kafka_metric_config, credentials=kafka_credentials)
    with patch("adapters.metric.kafka_queue_adapter.KafkaProducer") as MockProducer:
        MockProducer.return_value = MagicMock()
        inst._get_producer()
        _, kwargs = MockProducer.call_args
        assert kwargs["sasl_mechanism"] == "PLAIN"
        assert kwargs["sasl_plain_username"] == "metrics-user"
        assert kwargs["sasl_plain_password"] == "metrics-pass"


def test_get_producer_no_sasl_when_credentials_absent(kafka_metric_config):
    inst = KafkaQueueAdapter(config=kafka_metric_config, credentials={})
    with patch("adapters.metric.kafka_queue_adapter.KafkaProducer") as MockProducer:
        MockProducer.return_value = MagicMock()
        inst._get_producer()
        _, kwargs = MockProducer.call_args
        assert "sasl_mechanism" not in kwargs
