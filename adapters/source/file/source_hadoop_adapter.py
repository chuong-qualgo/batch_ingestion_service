from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from adapters.source.file_adapter import FileAdapter
from adapters.source.base_read_adapter import PathSourceConfig


class SourceHadoopAdapter(FileAdapter):
    """
    Reads files from Hadoop HDFS via Spark native reader.
    Expects a PathSourceConfig with an HDFS path (hdfs://namenode:9000/...).

    Required credentials keys (fetched from OpenBao by Airflow):
        hdfs_user          : str  — Hadoop user (sets HADOOP_USER_NAME)
        kerberos_principal : str, optional — for Kerberised clusters
        kerberos_keytab    : str, optional — path to keytab file

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

    def _configure_spark_for_filesystem(self) -> None:
        """
        Configure Hadoop user and optional Kerberos settings on the SparkContext.
        Sets HADOOP_USER_NAME for simple auth, or configures Kerberos principal
        and keytab for secure clusters.
        """
        import os
        hdfs_user = self.credentials.get("hdfs_user")
        if hdfs_user:
            os.environ["HADOOP_USER_NAME"] = hdfs_user

        kerberos_principal = self.credentials.get("kerberos_principal")
        kerberos_keytab = self.credentials.get("kerberos_keytab")
        if kerberos_principal and kerberos_keytab:
            hadoop_conf = self.spark.sparkContext._jsc.hadoopConfiguration()
            hadoop_conf.set("hadoop.security.authentication", "kerberos")
            hadoop_conf.set("dfs.namenode.kerberos.principal", kerberos_principal)
            self.spark.sparkContext._jvm.org.apache.hadoop.security.UserGroupInformation \
                .loginUserFromKeytab(kerberos_principal, kerberos_keytab)
