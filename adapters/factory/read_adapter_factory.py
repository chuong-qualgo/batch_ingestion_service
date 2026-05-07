from pyspark.sql import SparkSession

from adapters.factory.adapter_config import AdapterConfig, ReadAdapterType
from adapters.source.base_read_adapter import BaseReadAdapter
from adapters.source.file.source_hadoop_adapter import SourceHadoopAdapter
from adapters.source.file.source_s3_adapter import SourceS3Adapter
from adapters.source.nosql.source_cassandra_adapter import SourceCassandraAdapter
from adapters.source.nosql.source_dynamodb_adapter import SourceDynamoDBAdapter
from adapters.source.nosql.source_mongodb_adapter import SourceMongoDBAdapter
from adapters.source.sql.source_mysql_adapter import SourceMySQLAdapter
from adapters.source.sql.source_postgres_adapter import SourcePostgresAdapter


class ReadAdapterFactory:
    """
    Factory that produces a concrete BaseReadAdapter (PySpark)
    based on the `read_type` field in AdapterConfig.

    Registry
    --------
    ReadAdapterType.SQL        → SourcePostgresAdapter  (default SQL)
    ReadAdapterType.MYSQL      → SourceMySQLAdapter
    ReadAdapterType.NOSQL      → SourceMongoDBAdapter   (default NoSQL)
    ReadAdapterType.DYNAMODB   → SourceDynamoDBAdapter
    ReadAdapterType.CASSANDRA  → SourceCassandraAdapter
    ReadAdapterType.FILE       → SourceHadoopAdapter    (default File)
    ReadAdapterType.S3         → SourceS3Adapter

    Raises
    ------
    ValueError
        If the `read_type` in the config is not a recognised ReadAdapterType.
    """

    _registry: dict[ReadAdapterType, type[BaseReadAdapter]] = {
        ReadAdapterType.SQL:       SourcePostgresAdapter,
        ReadAdapterType.MYSQL:     SourceMySQLAdapter,
        ReadAdapterType.NOSQL:     SourceMongoDBAdapter,
        ReadAdapterType.DYNAMODB:  SourceDynamoDBAdapter,
        ReadAdapterType.CASSANDRA: SourceCassandraAdapter,
        ReadAdapterType.FILE:      SourceHadoopAdapter,
        ReadAdapterType.S3:        SourceS3Adapter,
    }

    @classmethod
    def create(cls, config: AdapterConfig, spark: SparkSession) -> BaseReadAdapter:
        adapter_class = cls._registry.get(config.read_type)
        if adapter_class is None:
            raise ValueError(
                f"Unknown read adapter type: '{config.read_type}'. "
                f"Valid types: {[t.value for t in ReadAdapterType]}"
            )
        credentials = getattr(config, 'credentials', {})
        return adapter_class(spark, config.source_config, credentials)
