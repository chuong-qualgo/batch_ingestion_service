from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional, Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

CheckpointValue = Union[int, datetime]


# ── Source config hierarchy ───────────────────────────────────────────────

@dataclass
class SourceConfig:
    """
    Base config shared by all source types.
    Do not instantiate directly — use TableSourceConfig or PathSourceConfig.
    """
    credential_ref: str          # OpenBao key — fetches host, port, and credentials
    extra: dict = field(default_factory=dict)


@dataclass
class TableSourceConfig(SourceConfig):
    """
    Config for table-based sources: SQL databases and NoSQL stores.

    Connection details (host, port) are intentionally absent here —
    they are stored in OpenBao alongside the credentials and injected
    into this config by InitOperator after the secret is fetched.

    Attributes
    ----------
    host : str
        Hostname or IP — populated from OpenBao secret at runtime.
    port : int
        Port — populated from OpenBao secret at runtime.
    database : str
        Database or keyspace name (non-sensitive, kept in YAML).
    schema : str
        Schema name. Defaults to 'default' when not applicable (e.g. NoSQL).
    table : str
        Table or collection name to read from.
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

    The `path` is intentionally absent here — it is stored in OpenBao
    and injected into this config by InitOperator after the secret is fetched.

    Attributes
    ----------
    path : str
        File or directory path — populated from OpenBao secret at runtime.
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
        host/port (table) and path (file) are populated from OpenBao
        by InitOperator before this adapter is instantiated.
    credentials : dict
        Credentials fetched from OpenBao by Airflow.
        For table sources: username, password (+ host, port injected into config).
        For file sources: access keys (+ path injected into config).
    schema : StructType, optional
        Expected schema of the source data. When provided, enforced on read.
    batch_size : int
        Maximum number of records to pull per batch.
    filters : dict
        Predicate pushdown filters to limit data read from the source.
        Keys are column names, values are filter expressions.
    checkpoint_from : int | datetime, optional
        Last checkpoint value — integer ID or timestamp. Tells the adapter
        where to start reading. When None, the adapter reads the full table/path.
    checkpoint_to : int | datetime, optional
        Upper-bound checkpoint value for this run — integer ID or timestamp.
        Tells the engine what to persist so the next run can resume from here.
    """

    def __init__(
        self,
        spark: SparkSession,
        source_config: Union[TableSourceConfig, PathSourceConfig],
        credentials: dict,
        batch_size: int = 10_000,
        schema: Optional[StructType] = None,
        filters: Optional[dict] = None,
        checkpoint_from: Optional[CheckpointValue] = None,
        checkpoint_to: Optional[CheckpointValue] = None,
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
        pass

    @abstractmethod
    def validate_connection(self) -> bool:
        pass

    @abstractmethod
    def infer_schema(self) -> StructType:
        pass

    @abstractmethod
    def get_record_count(self) -> int:
        pass

    @abstractmethod
    def apply_filters(self) -> None:
        pass
