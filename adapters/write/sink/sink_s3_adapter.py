from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from adapters.write.cloud_storage_adapter import CloudStorageAdapter
from adapters.write.base_write_adapter import SinkConfig, WriteMode


class SinkS3Adapter(CloudStorageAdapter):
    """
    Writes a Spark DataFrame to AWS S3 (or S3-compatible stores e.g. MinIO)
    using the S3A connector via Spark native writer.

    Internally rewrites s3:// to s3a:// in the endpoint for Hadoop S3A
    connector compatibility.

    Builds the output path via SinkConfig.build_write_path():
        s3a://bucket/{source_system_name}/{database}/{schema}/{table}/
        ingestion_date={date}/ingestion_time={time}/run_id={run_id}/

    Required credentials keys (fetched from OpenBao by Airflow):
        aws_access_key_id     : str
        aws_secret_access_key : str
        aws_region            : str
        aws_endpoint          : str, optional — for S3-compatible stores (e.g. MinIO)

    Parameters
    ----------
    file_format : str
        Output file format. Defaults to 'parquet'.
        Supported: parquet, orc, avro, csv, json, delta, text.

    Required package:
        org.apache.hadoop:hadoop-aws:<version>
    """

    def __init__(
        self,
        spark: SparkSession,
        sink_config: SinkConfig,
        credentials: dict,
        write_mode: WriteMode = WriteMode.APPEND,
        partition_by: Optional[list[str]] = None,
        batch_size: int = 10_000,
        schema: Optional[StructType] = None,
        file_format: str = "parquet",
    ) -> None:
        super().__init__(
            spark=spark,
            sink_config=sink_config,
            credentials=credentials,
            write_mode=write_mode,
            partition_by=partition_by,
            batch_size=batch_size,
            schema=schema,
            file_format=file_format,
        )
        # Normalise s3:// → s3a:// on the endpoint for Hadoop S3A connector
        if self.sink_config.endpoint.startswith("s3://"):
            self.sink_config.endpoint = self.sink_config.endpoint.replace(
                "s3://", "s3a://", 1
            )

    def _configure_spark_for_filesystem(self) -> None:
        """
        Inject AWS credentials and S3A settings into the Hadoop configuration
        on the active SparkContext.
        Supports path-style access for S3-compatible stores like MinIO.
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
        # Fast upload — required for writing large Spark partitions to S3
        hadoop_conf.set("fs.s3a.fast.upload", "true")
        hadoop_conf.set("fs.s3a.fast.upload.buffer", "bytebuffer")

        # Optional: override endpoint for MinIO or other S3-compatible stores
        aws_endpoint = self.credentials.get("aws_endpoint")
        if aws_endpoint:
            hadoop_conf.set("fs.s3a.endpoint", aws_endpoint)
            hadoop_conf.set("fs.s3a.path.style.access", "true")

    def validate_connection(self) -> bool:
        """
        Validate S3 access by checking the bucket exists via boto3.
        Falls back to Hadoop FileSystem exists check for S3-compatible stores.
        """
        aws_endpoint = self.credentials.get("aws_endpoint")

        if aws_endpoint:
            # S3-compatible store — use Hadoop FileSystem API
            return super().validate_connection()

        import boto3
        client = boto3.client(
            "s3",
            region_name=self.credentials.get("aws_region"),
            aws_access_key_id=self.credentials.get("aws_access_key_id"),
            aws_secret_access_key=self.credentials.get("aws_secret_access_key"),
        )
        # Extract bucket name from s3a://bucket/... endpoint
        bucket = self.sink_config.endpoint.replace("s3a://", "").split("/")[0]
        response = client.head_bucket(Bucket=bucket)
        return response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def post_write(self) -> None:
        """
        Log the successful write path after commit.
        Override to write an S3 manifest object or trigger an S3 event notification.
        """
        write_path = self.build_write_path()
        print(f"[SinkS3Adapter] Write complete → {write_path}")

    def on_error(self, exception: Exception) -> None:
        """
        Log the failure and the target S3 path.
        Override to send an SNS alert or write to a dead-letter S3 prefix.
        """
        write_path = self.build_write_path()
        print(f"[SinkS3Adapter] Write failed → {write_path} | error: {exception}")
