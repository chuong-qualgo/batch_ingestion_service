import json
from datetime import datetime, timezone
from typing import Optional

from adapters.metric.base_metric_adapter import (
    BaseMetricAdapter,
    SQSMetricConfig,
)


class SQSQueueAdapter(BaseMetricAdapter):
    """
    Publishes metric messages to AWS SQS standard queue.

    Required credentials keys (fetched from OpenBao by Airflow):
        aws_access_key_id     : str
        aws_secret_access_key : str

    Required package: boto3
    """

    config: SQSMetricConfig

    def build_message(self, payload: dict) -> dict:
        """
        Wrap payload in an SQS-compatible message envelope.
        Merges message_attributes as SQS MessageAttributes (String type).
        """
        message_attributes = {
            k: {"DataType": "String", "StringValue": str(v)}
            for k, v in self.message_attributes.items()
        }
        message_attributes["published_at"] = {
            "DataType": "String",
            "StringValue": datetime.now(tz=timezone.utc).isoformat(),
        }
        return {
            "QueueUrl": self.config.queue_url,
            "MessageBody": json.dumps(payload),
            "MessageAttributes": message_attributes,
        }

    def validate_connection(self) -> bool:
        """Validate by fetching queue attributes via boto3."""
        client = self._get_client()
        response = client.get_queue_attributes(
            QueueUrl=self.config.queue_url,
            AttributeNames=["QueueArn"],
        )
        return "QueueArn" in response.get("Attributes", {})

    def publish(self, payload: dict) -> None:
        """Send a single metric message to SQS."""
        try:
            message = self.build_message(payload)
            client = self._get_client()
            client.send_message(**message)
        except Exception as exc:
            self.on_error(exc)
            raise

    def on_error(self, exception: Exception) -> None:
        """Log SQS publish failure. Override for dead-letter or SNS alert."""
        print(
            f"[SQSQueueAdapter] Failed to publish to {self.config.queue_url}: "
            f"{exception}"
        )

    def _get_client(self):
        import boto3
        return boto3.client(
            "sqs",
            region_name=self.config.aws_region,
            aws_access_key_id=self.credentials.get("aws_access_key_id"),
            aws_secret_access_key=self.credentials.get("aws_secret_access_key"),
        )
