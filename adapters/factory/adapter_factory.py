from __future__ import annotations

from pyspark.sql import SparkSession

from adapters.factory.adapter_config import AdapterConfig
from adapters.factory.metric_adapter_factory import MetricAdapterFactory
from adapters.factory.read_adapter_factory import ReadAdapterFactory
from adapters.factory.write_adapter_factory import WriteAdapterFactory
from adapters.metric.base_metric_adapter import BaseMetricAdapter
from adapters.source.base_read_adapter import BaseReadAdapter
from adapters.write.base_write_adapter import BaseWriteAdapter


class AdapterFactory:
    """
    Top-level factory that delegates to the three specialised factories.

    Usage
    -----
    config = AdapterConfig(
        read_type=ReadAdapterType.SQL,
        write_type=WriteAdapterType.HADOOP,
        metric_type=MetricAdapterType.CLOUD_QUEUE,
        options={...},
    )
    spark = SparkSession.builder.getOrCreate()

    reader  = AdapterFactory.create_reader(config, spark)
    writer  = AdapterFactory.create_writer(config, spark)
    metrics = AdapterFactory.create_metric(config)
    """

    @staticmethod
    def create_reader(config: AdapterConfig, spark: SparkSession) -> BaseReadAdapter:
        """Return a concrete read adapter for the given config."""
        return ReadAdapterFactory.create(config, spark)

    @staticmethod
    def create_writer(config: AdapterConfig, spark: SparkSession) -> BaseWriteAdapter:
        """Return a concrete write adapter for the given config."""
        return WriteAdapterFactory.create(config, spark)

    @staticmethod
    def create_metric(config: AdapterConfig) -> BaseMetricAdapter:
        """Return a concrete metric adapter for the given config (no Spark needed)."""
        return MetricAdapterFactory.create(config)
