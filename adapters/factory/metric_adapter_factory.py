from adapters.factory.adapter_config import AdapterConfig, MetricAdapterType
from adapters.metric.base_metric_adapter import BaseMetricAdapter, MetricConfig
from adapters.metric.sqs_queue_adapter import SQSQueueAdapter
from adapters.metric.redis_queue_adapter import RedisQueueAdapter
from adapters.metric.kafka_queue_adapter import KafkaQueueAdapter


class MetricAdapterFactory:
    """
    Factory that produces a concrete BaseMetricAdapter (plain Python)
    based on the `metric_type` field in AdapterConfig.

    Registry
    --------
    MetricAdapterType.CLOUD_QUEUE  → SQSQueueAdapter   (AWS SQS)
    MetricAdapterType.ONPREM_QUEUE → RedisQueueAdapter  (Redis Streams)
    MetricAdapterType.KAFKA_QUEUE  → KafkaQueueAdapter  (Apache Kafka)

    Raises
    ------
    ValueError
        If the `metric_type` in the config is not a recognised MetricAdapterType.
    """

    _registry: dict[MetricAdapterType, type[BaseMetricAdapter]] = {
        MetricAdapterType.CLOUD_QUEUE:  SQSQueueAdapter,
        MetricAdapterType.ONPREM_QUEUE: RedisQueueAdapter,
        MetricAdapterType.KAFKA_QUEUE:  KafkaQueueAdapter,
    }

    @classmethod
    def create(
        cls,
        config: AdapterConfig,
        metric_config: MetricConfig,
        credentials: dict,
        message_attributes: dict = None,
    ) -> BaseMetricAdapter:
        adapter_class = cls._registry.get(config.metric_type)
        if adapter_class is None:
            raise ValueError(
                f"Unknown metric adapter type: '{config.metric_type}'. "
                f"Valid types: {[t.value for t in MetricAdapterType]}"
            )
        return adapter_class(
            config=metric_config,
            credentials=credentials,
            message_attributes=message_attributes or {},
        )
