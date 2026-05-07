from pyspark.sql import SparkSession

from adapters.factory.adapter_config import AdapterConfig, WriteAdapterType
from adapters.write.base_write_adapter import BaseWriteAdapter, SinkConfig
from adapters.write.sink.sink_hadoop_adapter import SinkHadoopAdapter
from adapters.write.sink.sink_s3_adapter import SinkS3Adapter


class WriteAdapterFactory:
    """
    Factory that produces a concrete BaseWriteAdapter (PySpark)
    based on the `write_type` field in AdapterConfig.

    Registry
    --------
    WriteAdapterType.HADOOP → SinkHadoopAdapter
    WriteAdapterType.S3     → SinkS3Adapter

    Raises
    ------
    ValueError
        If the `write_type` in the config is not a recognised WriteAdapterType.
    """

    _registry: dict[WriteAdapterType, type[BaseWriteAdapter]] = {
        WriteAdapterType.HADOOP: SinkHadoopAdapter,
        WriteAdapterType.S3:     SinkS3Adapter,
    }

    @classmethod
    def create(
        cls,
        config: AdapterConfig,
        spark: SparkSession,
        sink_config: SinkConfig,
        credentials: dict,
    ) -> BaseWriteAdapter:
        adapter_class = cls._registry.get(config.write_type)
        if adapter_class is None:
            raise ValueError(
                f"Unknown write adapter type: '{config.write_type}'. "
                f"Valid types: {[t.value for t in WriteAdapterType]}"
            )
        return adapter_class(
            spark=spark,
            sink_config=sink_config,
            credentials=credentials,
        )
