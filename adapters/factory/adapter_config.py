from dataclasses import dataclass, field
from enum import Enum


class ReadAdapterType(str, Enum):
    # SQL
    SQL       = "sql"        # PostgreSQL (default SQL)
    MYSQL     = "mysql"
    # NoSQL
    NOSQL     = "nosql"      # MongoDB (default NoSQL)
    DYNAMODB  = "dynamodb"
    CASSANDRA = "cassandra"
    # File / Cloud Storage
    FILE      = "file"       # Hadoop HDFS (default File)
    S3        = "s3"


class WriteAdapterType(str, Enum):
    HADOOP = "hadoop"
    S3     = "s3"


class MetricAdapterType(str, Enum):
    CLOUD_QUEUE  = "cloud_queue"
    ONPREM_QUEUE = "onprem_queue"
    KAFKA_QUEUE  = "kafka_queue"


@dataclass
class AdapterConfig:
    """
    Unified configuration passed to any adapter factory.

    The factory uses `read_type`, `write_type`, and `metric_type` to decide
    which concrete adapter to instantiate. All extra adapter-specific options
    (connection strings, bucket names, queue URLs, etc.) are placed in `options`.

    Example
    -------
    AdapterConfig(
        read_type=ReadAdapterType.SQL,
        write_type=WriteAdapterType.HADOOP,
        metric_type=MetricAdapterType.CLOUD_QUEUE,
        options={
            "jdbc_url": "jdbc:postgresql://host:5432/db",
            "hdfs_path": "hdfs://namenode:9000/data/landing",
            "queue_url": "https://sqs.ap-southeast-1.amazonaws.com/123/metrics",
        },
    )
    """

    read_type: ReadAdapterType = ReadAdapterType.SQL
    write_type: WriteAdapterType = WriteAdapterType.HADOOP
    metric_type: MetricAdapterType = MetricAdapterType.CLOUD_QUEUE
    options: dict = field(default_factory=dict)
