from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


# ── Source config hierarchy ───────────────────────────────────────────────

@dataclass
class SourceConfig:
    """
    Base config shared by all source types.
    Do not instantiate directly — use TableSourceConfig or PathSourceConfig.
    """
    credential_ref: str          # key used to fetch credentials from OpenBao
    extra: dict = field(default_factory=dict)


@dataclass
class TableSourceConfig(SourceConfig):
    """
    Config for table-based sources: SQL databases and NoSQL stores.

    Attributes
    ----------
    host : str
        Hostname or IP of the database server.
    port : int
        Port the database listens on.
    database : str
        Database or keyspace name.
    table : str
        Table or collection name to read from.
    schema : str
        Schema name. Defaults to 'default' when not applicable (e.g. NoSQL).
    query : str, optional
        Custom query or filter override. When provided, takes precedence
        over `table` so the adapter executes this query directly.
    checkpoint_column : str, optional
        Column used to track read progress (e.g. updated_at, id, _id).
        When omitted, the adapter reads the full table on every run.
    """
    host: str = ""
    port: int = 0
    database: str = ""
    schema: str = "default"
    table: str = ""
    query: Optional[str] = None
    checkpoint_column: Optional[str] = None


@dataclass
class PathSourceConfig(SourceConfig):
    """
    Config for path-based sources: files on Cloud Storage, HDFS, or local FS.

    Attributes
    ----------
    path : str
        File or directory path to read from.
        Examples: s3://bucket/prefix/, gs://bucket/dir/, hdfs://nn:9000/data/
    file_format : FileFormat
        Format of the files at the given path.
    checkpoint_column : str, optional
        Column used to track read progress within file content (e.g. event_time).
        When omitted, all files under the path are read on every run.
    """

    class FileFormat(str, Enum):
        PARQUET = "parquet"
        CSV     = "csv"
        JSON    = "json"
        AVRO    = "avro"
        ORC     = "orc"
        DELTA   = "delta"
        TEXT    = "text"

    path: str = ""
    file_format: FileFormat = FileFormat.PARQUET
    checkpoint_column: Optional[str] = None


# ── Base adapter ──────────────────────────────────────────────────────────

class BaseReadAdapter(ABC):
    """
    Abstract base class for all source (read) adapters.
    Implementations must run in PySpark.

    Attributes
    ----------
    spark : SparkSession
        Active Spark session injected by the engine.
    source_config : TableSourceConfig | PathSourceConfig
        Typed config for the specific source kind.
    credentials : dict
        Already-fetched credentials passed in from Airflow via openbao_hook.
        Keys depend on the source type (e.g. username/password, access_key).
    schema : StructType, optional
        Expected schema of the source data. When provided, enforced on read.
    batch_size : int
        Maximum number of records to pull per batch.
    filters : dict
        Predicate pushdown filters to limit data read from the source.
        Keys are column names, values are filter expressions.
    checkpoint_from : str, optional
        Serialised read position to restore from (offset, timestamp, or
        primary key value). Tells the adapter where to start reading.
        When None, the adapter reads the full table/path.
    checkpoint_to : str, optional
        Serialised read position after the last successful read. Tells the
        engine what to persist so the next run can resume from here.
    """

    def __init__(
        self,
        spark: SparkSession,
        source_config: Union[TableSourceConfig, PathSourceConfig],
        credentials: dict,
        batch_size: int = 10_000,
        schema: Optional[StructType] = None,
        filters: Optional[dict] = None,
        checkpoint_from: Optional[str] = None,
        checkpoint_to: Optional[str] = None,
    ) -> None:
        self.spark = spark
        self.source_config = source_config
        self.credentials = credentials
        self.batch_size = batch_size
        self.schema = schema
        self.filters = filters or {}
        self.checkpoint_from = checkpoint_from
        self.checkpoint_to = checkpoint_to

    # ── Abstract actions ──────────────────────────────────────────────────

    @abstractmethod
    def read(self) -> DataFrame:
        """
        Pull data from the source and return a Spark DataFrame.
        Respects checkpoint_from to resume from last position when set.
        Updates checkpoint_to with the latest position after reading.
        When checkpoint_column is None, reads the full table/path.
        """
        pass

    @abstractmethod
    def validate_connection(self) -> bool:
        """
        Test connectivity to the source before reading.
        Returns True if the connection is healthy, raises on failure.
        """
        pass

    @abstractmethod
    def infer_schema(self) -> StructType:
        """
        Sample the source and return its detected schema as a StructType.
        Used when `schema` is not explicitly provided.
        """
        pass

    @abstractmethod
    def get_record_count(self) -> int:
        """
        Return the number of records available in the source for logging.
        Uses source-native count where possible (SELECT COUNT(*), estimatedDocumentCount, etc.).
        For file-based sources, lists and returns the number of files to read.
        """
        pass

    @abstractmethod
    def apply_filters(self) -> None:
        """
        Push predicate filters down to the source to reduce data transfer.
        Applies the expressions in `self.filters` before `read()` is called.
        """
        pass
