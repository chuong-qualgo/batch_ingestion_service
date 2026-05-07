from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import Optional, Union
from pathlib import PurePosixPath

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from adapters.source.base_read_adapter import PathSourceConfig, TableSourceConfig

DEFAULT_SCHEMA_NAME = "default"


class WriteMode(str, Enum):
    APPEND    = "append"
    OVERWRITE = "overwrite"
    UPSERT    = "upsert"
    MERGE     = "merge"


# ── Sink config ───────────────────────────────────────────────────────────

@dataclass
class SinkConfig:
    """
    Configuration for a write sink, including all fields needed to
    construct the output path at runtime.

    Output path structure
    ---------------------
    SQL / NoSQL source:
        {endpoint}/{source_system_name}/{database_name}/{schema_name}/{table_name}/
        ingestion_date={date}/ingestion_time={time}/run_id={run_id}/

    File / Cloud Storage source:
        {endpoint}/{source_system_name}/{path_to_read_from_source}/
        ingestion_date={date}/ingestion_time={time}/run_id={run_id}/

    Attributes
    ----------
    endpoint : str
        Root URI of the sink system.
        Examples: hdfs://namenode:9000, s3://my-bucket, gs://my-bucket
    credential_ref : str
        Key used to fetch sink credentials from OpenBao.
    source_system_name : str
        Free-form label identifying the origin system (defined by the user).
        Examples: "postgres-prod", "mongodb-orders", "sftp-partner"
    ingestion_date : date
        The date partition for this run. Typically the Airflow execution date.
    ingestion_time : datetime
        The time partition for this run. Typically the Airflow execution time.
    run_id : str
        Unique identifier for this pipeline run. Typically the Airflow run_id.
    source_config : TableSourceConfig | PathSourceConfig
        The source config from the reader, used to derive the base path
        segments (database, schema, table or file path).
    extra : dict
        Optional additional sink-specific parameters.
    """
    endpoint: str
    credential_ref: str
    source_system_name: str
    ingestion_date: date
    ingestion_time: datetime
    run_id: str
    source_config: Union[TableSourceConfig, PathSourceConfig]
    extra: dict = field(default_factory=dict)


# ── Base adapter ──────────────────────────────────────────────────────────

class BaseWriteAdapter(ABC):
    """
    Abstract base class for all sink (write) adapters.
    Implementations must run in PySpark.

    Attributes
    ----------
    spark : SparkSession
        Active Spark session injected by the engine.
    sink_config : SinkConfig
        Full sink configuration including path construction fields.
    write_mode : WriteMode
        Strategy for writing: append, overwrite, upsert, or merge.
    partition_by : list[str]
        Column names to partition the output data by.
    batch_size : int
        Maximum number of records per write batch.
    schema : StructType, optional
        Enforced output schema. The DataFrame is validated against this
        before writing to prevent schema drift at the sink.
    """

    def __init__(
        self,
        spark: SparkSession,
        sink_config: SinkConfig,
        write_mode: WriteMode = WriteMode.APPEND,
        partition_by: Optional[list[str]] = None,
        batch_size: int = 10_000,
        schema: Optional[StructType] = None,
    ) -> None:
        self.spark = spark
        self.sink_config = sink_config
        self.write_mode = write_mode
        self.partition_by = partition_by or []
        self.batch_size = batch_size
        self.schema = schema

    # ── Path construction ─────────────────────────────────────────────────

    def build_write_path(self) -> str:
        """
        Construct the fully qualified output path from SinkConfig fields.

        SQL / NoSQL:
            {endpoint}/{source_system_name}/{database}/{schema}/{table}/
            ingestion_date={date}/ingestion_time={time}/run_id={run_id}/

        File / Cloud Storage:
            {endpoint}/{source_system_name}/{path}/
            ingestion_date={date}/ingestion_time={time}/run_id={run_id}/

        Returns
        -------
        str
            The fully constructed write path.
        """
        cfg = self.sink_config

        if isinstance(cfg.source_config, TableSourceConfig):
            schema_name = cfg.source_config.database or DEFAULT_SCHEMA_NAME
            base = PurePosixPath(
                cfg.source_system_name,
                cfg.source_config.database,
                schema_name,
                cfg.source_config.table,
            )
        elif isinstance(cfg.source_config, PathSourceConfig):
            # Strip leading slash so PurePosixPath joins cleanly
            normalised_path = cfg.source_config.path.lstrip("/")
            base = PurePosixPath(cfg.source_system_name, normalised_path)
        else:
            raise TypeError(
                f"Unsupported source_config type: {type(cfg.source_config)}"
            )

        partitions = PurePosixPath(
            f"ingestion_date={cfg.ingestion_date.isoformat()}",
            f"ingestion_time={cfg.ingestion_time.strftime('%H-%M-%S')}",
            f"run_id={cfg.run_id}",
        )

        # PurePosixPath strips double slashes (s3a:// → s3a:/) so we
        # reconstruct the scheme prefix manually
        scheme = ""
        endpoint = cfg.endpoint
        if "://" in endpoint:
            scheme, endpoint = endpoint.split("://", 1)
            scheme += "://"
        full_path = PurePosixPath(endpoint) / base / partitions
        return scheme + str(full_path)

    # ── Abstract actions ──────────────────────────────────────────────────

    @abstractmethod
    def write(self, df: DataFrame) -> None:
        """
        Write the given Spark DataFrame to the target sink.
        Should call `build_write_path()` to resolve the destination.
        Should respect `write_mode` and `partition_by`.
        """
        pass

    @abstractmethod
    def validate_connection(self) -> bool:
        """
        Test connectivity to the sink before writing.
        Returns True if the connection is healthy, raises on failure.
        """
        pass

    @abstractmethod
    def validate_schema(self, df: DataFrame) -> bool:
        """
        Confirm the DataFrame schema matches `self.schema`.
        Raises on mismatch to prevent schema drift at the sink.
        """
        pass

    @abstractmethod
    def pre_write(self, df: DataFrame) -> DataFrame:
        """
        Hook called just before writing.
        Use for last-mile transformations such as deduplication,
        type casting, or data quality checks. Returns the processed DataFrame.
        """
        pass

    @abstractmethod
    def post_write(self) -> None:
        """
        Hook called after a successful write.
        Use for updating manifests, committing transactions,
        or sending downstream notifications.
        """
        pass

    @abstractmethod
    def on_error(self, exception: Exception) -> None:
        """
        Error handling hook invoked when `write()` fails.
        Implement sink-specific strategies such as retry,
        dead-letter routing, alerting, or rollback.
        """
        pass
