from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class MetricConfig:
    """
    Base config shared by all metric adapter types.
    Do not instantiate directly — use RedisMetricConfig or SQSMetricConfig.
    """
    credential_ref: str          # OpenBao key for fetching queue credentials
    extra: dict = field(default_factory=dict)


@dataclass
class RedisMetricConfig(MetricConfig):
    """
    Config for Redis Streams metric sink.

    Attributes
    ----------
    host : str
        Redis server hostname — populated from OpenBao secret at runtime.
    port : int
        Redis server port — populated from OpenBao secret at runtime.
    stream_name : str
        Redis stream key to publish messages to (XADD target).
        Non-sensitive — kept in YAML config.
    max_len : int, optional
        Max stream length — older entries are trimmed automatically.
        None means unbounded.
    """
    host: str = ""
    port: int = 6379
    stream_name: str = ""
    max_len: Optional[int] = 1000


@dataclass
class SQSMetricConfig(MetricConfig):
    """
    Config for AWS SQS standard queue metric sink.

    Attributes
    ----------
    queue_url : str
        Full SQS queue URL — populated from OpenBao secret at runtime.
        Example: https://sqs.ap-southeast-1.amazonaws.com/123456789/my-queue
    aws_region : str
        AWS region — populated from OpenBao secret at runtime.
    """
    queue_url: str = ""
    aws_region: str = ""


class BaseMetricAdapter(ABC):
    """
    Abstract base class for all metric/event adapters.
    Implementations run in plain Python — no Spark dependency.

    Attributes
    ----------
    config : RedisMetricConfig | SQSMetricConfig
        Queue connection config for the specific backend.
    credentials : dict
        Already-fetched credentials passed in from Airflow via OpenBaoHook.
    message_attributes : dict
        Optional metadata/headers attached to every published message.
        Example: {"source": "file-landing", "env": "prod"}
    """

    def __init__(
        self,
        config: MetricConfig,
        credentials: dict,
        message_attributes: Optional[dict] = None,
    ) -> None:
        self.config = config
        self.credentials = credentials
        self.message_attributes = message_attributes or {}

    @abstractmethod
    def publish(self, payload: dict) -> None:
        """Push a metric payload dict to the target queue."""
        pass

    @abstractmethod
    def validate_connection(self) -> bool:
        """Verify the queue is reachable before publishing. Raises on failure."""
        pass

    @abstractmethod
    def build_message(self, payload: dict) -> dict:
        """
        Format the raw payload into the queue-specific message envelope.
        Returns the formatted message ready to send.
        """
        pass

    @abstractmethod
    def on_error(self, exception: Exception) -> None:
        """
        Error hook invoked when publish() fails.
        Implement retry, dead-letter, or alerting logic here.
        """
        pass
