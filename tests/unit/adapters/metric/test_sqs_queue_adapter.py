"""Tests for SQSQueueAdapter using moto for SQS mocking."""
import json
import pytest
import boto3
from moto import mock_aws as mock_sqs

from adapters.metric.sqs_queue_adapter import SQSQueueAdapter
from adapters.metric.base_metric_adapter import SQSMetricConfig


AWS_REGION = "ap-southeast-1"
QUEUE_NAME = "test-metrics"


@pytest.fixture
def sqs_queue_url():
    """Create a real moto-mocked SQS queue and return its URL."""
    with mock_sqs():
        client = boto3.client("sqs", region_name=AWS_REGION)
        response = client.create_queue(QueueName=QUEUE_NAME)
        yield response["QueueUrl"]


@pytest.fixture
def adapter(sqs_queue_url, sqs_credentials):
    # queue_url injected from OpenBao credentials into config
    creds_with_url = {**sqs_credentials, "queue_url": sqs_queue_url, "aws_region": AWS_REGION}
    config = SQSMetricConfig(
        credential_ref="ref",
        queue_url=creds_with_url["queue_url"],
        aws_region=creds_with_url["aws_region"],
    )
    return SQSQueueAdapter(
        config=config,
        credentials=creds_with_url,
        message_attributes={"service": "file-landing"},
    )


# ── build_message ─────────────────────────────────────────────────────────

def test_build_message_structure(adapter, sqs_queue_url):
    payload = {"status": "success", "count": 42}
    with mock_sqs():
        msg = adapter.build_message(payload)

    assert msg["QueueUrl"] == sqs_queue_url
    body = json.loads(msg["MessageBody"])
    assert body["status"] == "success"
    assert body["count"] == 42


def test_build_message_includes_attributes(adapter):
    with mock_sqs():
        msg = adapter.build_message({"k": "v"})
    attrs = msg["MessageAttributes"]
    assert "service" in attrs
    assert attrs["service"]["StringValue"] == "file-landing"
    assert "published_at" in attrs


def test_build_message_no_extra_attributes():
    config = SQSMetricConfig(
        credential_ref="ref",
        queue_url="https://sqs.us-east-1.amazonaws.com/123/q",
        aws_region="us-east-1",
    )
    adapter = SQSQueueAdapter(config=config, credentials={})
    with mock_sqs():
        msg = adapter.build_message({"x": 1})
    assert "published_at" in msg["MessageAttributes"]


# ── validate_connection ───────────────────────────────────────────────────

def test_validate_connection_success(adapter, sqs_queue_url):
    with mock_sqs():
        boto3.client("sqs", region_name=AWS_REGION).create_queue(QueueName=QUEUE_NAME)
        assert adapter.validate_connection() is True


# ── publish ───────────────────────────────────────────────────────────────

def test_publish_sends_message(adapter, sqs_queue_url):
    with mock_sqs():
        boto3.client("sqs", region_name=AWS_REGION).create_queue(QueueName=QUEUE_NAME)
        adapter.publish({"status": "success", "dag_id": "test_dag"})

        client = boto3.client("sqs", region_name=AWS_REGION)
        msgs = client.receive_message(QueueUrl=sqs_queue_url, MaxNumberOfMessages=1)
        assert len(msgs.get("Messages", [])) == 1
        body = json.loads(msgs["Messages"][0]["Body"])
        assert body["dag_id"] == "test_dag"


def test_publish_calls_on_error_on_failure(adapter):
    adapter.on_error = pytest.raises  # will be replaced
    adapter.on_error = lambda exc: None  # capture
    called = []
    adapter.on_error = lambda exc: called.append(exc)

    # Point to invalid queue
    adapter.config.queue_url = "https://sqs.us-east-1.amazonaws.com/999/nonexistent"
    with mock_sqs():
        with pytest.raises(Exception):
            adapter.publish({"x": 1})
    assert len(called) == 1


# ── on_error ─────────────────────────────────────────────────────────────

def test_on_error_does_not_raise(adapter, capsys):
    adapter.on_error(RuntimeError("oops"))
    captured = capsys.readouterr()
    assert "SQSQueueAdapter" in captured.out
    assert "oops" in captured.out
