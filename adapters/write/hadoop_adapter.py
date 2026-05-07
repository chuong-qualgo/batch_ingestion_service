from abc import abstractmethod
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from adapters.write.base_write_adapter import BaseWriteAdapter, SinkConfig, WriteMode


class HadoopAdapter(BaseWriteAdapter):
    """
    Mid-tier adapter for writing Spark DataFrames to file-based sinks via
    Spark native writer. Shared logic for HDFS and S3.
    Subclasses provide filesystem-specific credential configuration.
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
            write_mode=write_mode,
            partition_by=partition_by,
            batch_size=batch_size,
            schema=schema,
        )
        self.credentials = credentials
        self.file_format = file_format

    @abstractmethod
    def _configure_spark_for_filesystem(self) -> None:
        """
        Apply filesystem-specific Spark/Hadoop config (credentials, endpoint, etc.).
        Called once at the start of write() and validate_connection().
        """
        pass

    def validate_schema(self, df: DataFrame) -> bool:
        """
        Compare DataFrame schema against self.schema field by field.
        Raises ValueError on mismatch to prevent schema drift at the sink.
        """
        if self.schema is None:
            return True
        df_fields = {f.name: f.dataType for f in df.schema.fields}
        expected_fields = {f.name: f.dataType for f in self.schema.fields}
        mismatches = [
            f"Column '{name}': expected {expected_fields[name]}, got {df_fields.get(name)}"
            for name in expected_fields
            if df_fields.get(name) != expected_fields[name]
        ]
        if mismatches:
            raise ValueError(f"Schema validation failed:\n" + "\n".join(mismatches))
        return True

    def pre_write(self, df: DataFrame) -> DataFrame:
        """
        Apply schema enforcement and column pruning before writing.
        Casts columns to expected types when self.schema is provided.
        """
        if self.schema is None:
            return df
        expected_col_names = [f.name for f in self.schema.fields]
        df = df.select(*[c for c in expected_col_names if c in df.columns])
        for field in self.schema.fields:
            if field.name in df.columns:
                df = df.withColumn(field.name, df[field.name].cast(field.dataType))
        return df

    def write(self, df: DataFrame) -> None:
        """
        Write the DataFrame to the filesystem path built from SinkConfig.
        Runs pre_write → validate_schema → write → post_write.
        Calls on_error on any exception.
        """
        try:
            self._configure_spark_for_filesystem()
            self.validate_schema(df)
            df = self.pre_write(df)
            write_path = self.build_write_path()

            writer = df.write.format(self.file_format).mode(self.write_mode.value)

            if self.partition_by:
                writer = writer.partitionBy(*self.partition_by)

            writer.save(write_path)
            self.post_write()
        except Exception as exc:
            self.on_error(exc)
            raise

    def validate_connection(self) -> bool:
        """
        Validate access to the sink root path using Spark's Hadoop FileSystem API.
        """
        self._configure_spark_for_filesystem()
        hadoop_conf = self.spark.sparkContext._jvm.org.apache.hadoop.conf.Configuration()
        fs_path = self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path(
            self.sink_config.endpoint
        )
        fs = fs_path.getFileSystem(hadoop_conf)
        return fs.exists(fs_path)

    def post_write(self) -> None:
        """
        Default post-write hook.
        Logs the write path. Override in subclasses for manifest updates
        or downstream notifications.
        """
        pass

    def on_error(self, exception: Exception) -> None:
        """
        Default error hook — re-raises after logging.
        Override in subclasses for dead-letter routing or alerting.
        """
        pass
