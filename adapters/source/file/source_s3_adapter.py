from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from adapters.source.file_adapter import FileAdapter
from adapters.source.base_read_adapter import PathSourceConfig


class SourceS3Adapter(FileAdapter):
    """
    Reads files from AWS S3 via Spark using the s3a:// protocol.
    Expects a PathSourceConfig with an S3 path (s3://bucket/prefix/).
    Internally rewrites s3:// to s3a:// for Hadoop S3A connector compatibility.

    Required credentials keys (fetched from OpenBao by Airflow):
        aws_access_key_id     : str
        aws_secret_access_key : str
        aws_region            : str
        aws_endpoint          : str, optional — for S3-compatible stores (e.g. MinIO)

    Required package:
        org.apache.hadoop:hadoop-aws:<version>

    Supports all FileFormat values: parquet, csv, json, avro, orc, delta, text.
    """

    def __init__(
        self,
        spark: SparkSession,
        source_config: PathSourceConfig,
        credentials: dict,
        batch_size: int = 10_000,
        schema: Optional[StructType] = None,
        filters: Optional[dict] = None,
        checkpoint_from: Optional[str] = None,
        checkpoint_to: Optional[str] = None,
    ) -> None:
        super().__init__(
            spark=spark,
            source_config=source_config,
            credentials=credentials,
            batch_size=batch_size,
            schema=schema,
            filters=filters,
            checkpoint_from=checkpoint_from,
            checkpoint_to=checkpoint_to,
        )
        # Normalise s3:// → s3a:// for Hadoop S3A connector
        if self.source_config.path.startswith("s3://"):
            self.source_config.path = self.source_config.path.replace("s3://", "s3a://", 1)

    def _configure_spark_for_filesystem(self) -> None:
        """
        Inject AWS credentials and S3A settings into the Hadoop configuration
        on the active SparkContext. Also sets the S3A endpoint for S3-compatible
        object stores such as MinIO.
        """
        hadoop_conf = self.spark.sparkContext._jsc.hadoopConfiguration()

        hadoop_conf.set(
            "fs.s3a.access.key",
            self.credentials.get("aws_access_key_id", ""),
        )
        hadoop_conf.set(
            "fs.s3a.secret.key",
            self.credentials.get("aws_secret_access_key", ""),
        )
        hadoop_conf.set(
            "fs.s3a.endpoint.region",
            self.credentials.get("aws_region", "us-east-1"),
        )
        hadoop_conf.set(
            "fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        hadoop_conf.set(
            "fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )

        # Optional: override endpoint for MinIO or other S3-compatible stores
        aws_endpoint = self.credentials.get("aws_endpoint")
        if aws_endpoint:
            hadoop_conf.set("fs.s3a.endpoint", aws_endpoint)
            hadoop_conf.set("fs.s3a.path.style.access", "true")
