import json
from datetime import datetime, timezone

from kafka import KafkaProducer

from adapters.metric.base_metric_adapter import (
    BaseMetricAdapter,
    KafkaMetricConfig,
)


class KafkaQueueAdapter(BaseMetricAdapter):
    """
    Publishes metric messages to an Apache Kafka topic.

    Required credentials keys (fetched from OpenBao by Airflow):
        bootstrap_servers : str  — comma-separated broker list
        sasl_username     : str, optional  — SASL/PLAIN username
        sasl_password     : str, optional  — SASL/PLAIN password

    Required package: kafka-python
    """

    config: KafkaMetricConfig

    def build_message(self, payload: dict) -> bytes:
        """
        Serialize the payload to JSON bytes.
        Injects published_at and merges message_attributes before encoding.
        """
        envelope = dict(payload)
        envelope.update(self.message_attributes)
        envelope["published_at"] = datetime.now(tz=timezone.utc).isoformat()
        return json.dumps(envelope, default=str).encode("utf-8")

    def validate_connection(self) -> bool:
        """Validate by fetching partition metadata for the target topic."""
        producer = self._get_producer()
        try:
            partitions = producer.partitions_for(self.config.topic)
            return partitions is not None
        finally:
            producer.close()

    def publish(self, payload: dict) -> None:
        """Send a single metric message to the Kafka topic and flush."""
        producer = self._get_producer()
        try:
            message = self.build_message(payload)
            key = self.config.key.encode("utf-8") if self.config.key else None
            producer.send(self.config.topic, value=message, key=key)
            producer.flush()
        except Exception as exc:
            self.on_error(exc)
            raise
        finally:
            producer.close()

    def on_error(self, exception: Exception) -> None:
        """Log Kafka publish failure. Override for retry or alerting."""
        print(
            f"[KafkaQueueAdapter] Failed to publish to topic "
            f"'{self.config.topic}' on {self.config.bootstrap_servers}: {exception}"
        )

    def _get_producer(self):
        kwargs = {
            "bootstrap_servers": self.config.bootstrap_servers,
        }
        sasl_username = self.credentials.get("sasl_username")
        sasl_password = self.credentials.get("sasl_password")
        if sasl_username and sasl_password:
            kwargs.update({
                "security_protocol": "SASL_PLAINTEXT",
                "sasl_mechanism": "PLAIN",
                "sasl_plain_username": sasl_username,
                "sasl_plain_password": sasl_password,
            })
        return KafkaProducer(**kwargs)
