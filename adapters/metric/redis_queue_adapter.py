import json
from datetime import datetime, timezone
from typing import Optional

from adapters.metric.base_metric_adapter import (
    BaseMetricAdapter,
    RedisMetricConfig,
)


class RedisQueueAdapter(BaseMetricAdapter):
    """
    Publishes metric messages to a Redis Stream via XADD.

    Required credentials keys (fetched from OpenBao by Airflow):
        password : str, optional  — Redis AUTH password (omit if no auth)

    Required package: redis
    """

    config: RedisMetricConfig

    def build_message(self, payload: dict) -> dict:
        """
        Flatten the payload dict into a Redis Stream field-value mapping.
        Redis Streams require all values to be strings.
        Nested dicts are JSON-serialised. Merges message_attributes.
        """
        message = {
            k: json.dumps(v) if isinstance(v, (dict, list)) else str(v)
            for k, v in payload.items()
        }
        message.update({
            k: str(v) for k, v in self.message_attributes.items()
        })
        message["published_at"] = datetime.now(tz=timezone.utc).isoformat()
        return message

    def validate_connection(self) -> bool:
        """Validate by sending a Redis PING."""
        client = self._get_client()
        return client.ping()

    def publish(self, payload: dict) -> None:
        """
        Push a metric message to the Redis Stream via XADD.
        Applies max_len trimming if configured.
        """
        try:
            message = self.build_message(payload)
            client = self._get_client()
            xadd_kwargs = {
                "name": self.config.stream_name,
                "fields": message,
            }
            if self.config.max_len is not None:
                xadd_kwargs["maxlen"] = self.config.max_len
                xadd_kwargs["approximate"] = True
            client.xadd(**xadd_kwargs)
        except Exception as exc:
            self.on_error(exc)
            raise

    def on_error(self, exception: Exception) -> None:
        """Log Redis publish failure. Override for retry or alerting."""
        print(
            f"[RedisQueueAdapter] Failed to publish to stream "
            f"'{self.config.stream_name}' on {self.config.host}: {exception}"
        )

    def _get_client(self):
        import redis
        return redis.Redis(
            host=self.config.host,
            port=self.config.port,
            password=self.credentials.get("password"),
            decode_responses=True,
        )
