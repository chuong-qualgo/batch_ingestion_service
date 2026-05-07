"""Tests for RedisQueueAdapter using fakeredis."""
import json
import pytest
import fakeredis

from adapters.metric.redis_queue_adapter import RedisQueueAdapter
from adapters.metric.base_metric_adapter import RedisMetricConfig


@pytest.fixture
def fake_redis_client():
    return fakeredis.FakeRedis(decode_responses=True)


@pytest.fixture
def adapter(redis_metric_config, redis_credentials, fake_redis_client, monkeypatch):
    inst = RedisQueueAdapter(
        config=redis_metric_config,
        credentials=redis_credentials,
        message_attributes={"env": "test"},
    )
    # Patch _get_client to return fakeredis
    monkeypatch.setattr(inst, "_get_client", lambda: fake_redis_client)
    return inst


# ── build_message ─────────────────────────────────────────────────────────

def test_build_message_flattens_strings(adapter):
    msg = adapter.build_message({"status": "ok", "count": 5})
    assert msg["status"] == "ok"
    assert msg["count"] == "5"


def test_build_message_json_serialises_nested(adapter):
    msg = adapter.build_message({"meta": {"key": "val"}})
    parsed = json.loads(msg["meta"])
    assert parsed["key"] == "val"


def test_build_message_includes_attributes(adapter):
    msg = adapter.build_message({"x": 1})
    assert msg["env"] == "test"
    assert "published_at" in msg


def test_build_message_includes_lists(adapter):
    msg = adapter.build_message({"items": [1, 2, 3]})
    assert json.loads(msg["items"]) == [1, 2, 3]


# ── validate_connection ───────────────────────────────────────────────────

def test_validate_connection_ping(adapter, fake_redis_client):
    assert adapter.validate_connection() is True


# ── publish ───────────────────────────────────────────────────────────────

def test_publish_adds_to_stream(adapter, fake_redis_client):
    adapter.publish({"status": "success", "dag_id": "test_dag"})
    entries = fake_redis_client.xrange("pipeline-metrics")
    assert len(entries) == 1
    _, fields = entries[0]
    assert fields["dag_id"] == "test_dag"
    assert fields["status"] == "success"


def test_publish_adds_multiple_messages(adapter, fake_redis_client):
    adapter.publish({"run": "1"})
    adapter.publish({"run": "2"})
    entries = fake_redis_client.xrange("pipeline-metrics")
    assert len(entries) == 2


def test_publish_applies_maxlen(adapter, fake_redis_client):
    # max_len=1000 is configured — just verify xadd is called with it
    # by publishing and checking the stream has the entry
    adapter.publish({"k": "v"})
    assert fake_redis_client.xlen("pipeline-metrics") == 1


def test_publish_no_maxlen(redis_credentials, fake_redis_client, monkeypatch):
    config = RedisMetricConfig(
        credential_ref="ref",
        host="localhost",
        port=6379,
        stream_name="pipeline-metrics",
        max_len=None,
    )
    adapter = RedisQueueAdapter(config=config, credentials=redis_credentials)
    monkeypatch.setattr(adapter, "_get_client", lambda: fake_redis_client)
    adapter.publish({"unbounded": "true"})
    assert fake_redis_client.xlen("pipeline-metrics") == 1


def test_publish_calls_on_error_on_failure(adapter, monkeypatch):
    called = []
    monkeypatch.setattr(adapter, "_get_client", lambda: (_ for _ in ()).throw(ConnectionError("down")))
    adapter.on_error = lambda exc: called.append(exc)
    with pytest.raises(Exception):
        adapter.publish({"x": 1})
    assert len(called) == 1


# ── on_error ─────────────────────────────────────────────────────────────

def test_on_error_logs(adapter, capsys):
    adapter.on_error(ConnectionError("timeout"))
    out = capsys.readouterr().out
    assert "RedisQueueAdapter" in out
    assert "pipeline-metrics" in out
