import os
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from adapters.write.hadoop_adapter import HadoopAdapter
from adapters.write.base_write_adapter import SinkConfig, WriteMode


class SinkHadoopAdapter(HadoopAdapter):
    """
    Writes a Spark DataFrame to Hadoop HDFS.

    Builds the output path via SinkConfig.build_write_path():
        hdfs://namenode:9000/{source_system_name}/{database}/{schema}/{table}/
        ingestion_date={date}/ingestion_time={time}/run_id={run_id}/

    Required credentials keys (fetched from OpenBao by Airflow):
        hdfs_user          : str            — Hadoop user (sets HADOOP_USER_NAME)
        kerberos_principal : str, optional  — for Kerberised clusters
        kerberos_keytab    : str, optional  — path to the keytab file

    Parameters
    ----------
    file_format : str
        Output file format. Defaults to 'parquet'.
        Supported: parquet, orc, avro, csv, json, delta, text.
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

    def _configure_spark_for_filesystem(self) -> None:
        """
        Set HADOOP_USER_NAME for simple auth clusters.
        Configure Kerberos principal and keytab for secure clusters
        via Spark's Hadoop UserGroupInformation.
        """
        hdfs_user = self.credentials.get("hdfs_user")
        if hdfs_user:
            os.environ["HADOOP_USER_NAME"] = hdfs_user

        kerberos_principal = self.credentials.get("kerberos_principal")
        kerberos_keytab = self.credentials.get("kerberos_keytab")
        if kerberos_principal and kerberos_keytab:
            hadoop_conf = self.spark.sparkContext._jsc.hadoopConfiguration()
            hadoop_conf.set("hadoop.security.authentication", "kerberos")
            hadoop_conf.set("dfs.namenode.kerberos.principal", kerberos_principal)
            self.spark.sparkContext._jvm \
                .org.apache.hadoop.security.UserGroupInformation \
                .loginUserFromKeytab(kerberos_principal, kerberos_keytab)

    def post_write(self) -> None:
        """
        Log the successful write path after commit.
        Override to write a _SUCCESS marker or update an HDFS manifest file.
        """
        write_path = self.build_write_path()
        print(f"[SinkHadoopAdapter] Write complete → {write_path}")

    def on_error(self, exception: Exception) -> None:
        """
        Log the failure and the target path.
        Override to send an alert or write to a dead-letter location on HDFS.
        """
        write_path = self.build_write_path()
        print(f"[SinkHadoopAdapter] Write failed → {write_path} | error: {exception}")
