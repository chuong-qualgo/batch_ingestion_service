from abc import abstractmethod
from typing import Optional, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from adapters.source.base_read_adapter import BaseReadAdapter, PathSourceConfig


class FileAdapter(BaseReadAdapter):
    """
    Mid-tier adapter for file-based sources via Spark native readers.
    Subclasses provide filesystem-specific connection setup.
    """

    source_config: PathSourceConfig

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
        self._read_options: dict = {}

    @abstractmethod
    def _configure_spark_for_filesystem(self) -> None:
        """
        Apply filesystem-specific Spark/Hadoop config (credentials, endpoint, etc.).
        Called once before any read or validation operation.
        """
        pass

    def _list_files(self) -> List[str]:
        """
        List all files under source_config.path using Spark's Hadoop FileSystem API.
        Used by get_record_count() for logging and by read() for checkpoint filtering.
        """
        hadoop_conf = self.spark.sparkContext._jvm.org.apache.hadoop.conf.Configuration()
        fs_path = self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path(self.source_config.path)
        fs = fs_path.getFileSystem(hadoop_conf)
        statuses = fs.listStatus(fs_path)
        return [str(s.getPath()) for s in statuses if not s.isDirectory()]

    def apply_filters(self) -> None:
        """
        Store filter expressions to apply as DataFrame.filter() after read.
        File sources do not support predicate pushdown before load.
        """
        if self.filters:
            self._read_options["post_filters"] = self.filters

    def read(self) -> DataFrame:
        """
        Read files from the filesystem path via Spark native reader.
        Applies checkpoint_column filter after load when checkpoint_from is set.
        Applies post-load filters from self.filters if present.
        Updates checkpoint_to with the max checkpoint_column value after read.
        """
        self._configure_spark_for_filesystem()

        fmt = self.source_config.file_format.value
        reader = self.spark.read.format(fmt)

        if self.schema:
            reader = reader.schema(self.schema)

        df = reader.load(self.source_config.path)

        cfg = self.source_config
        if cfg.checkpoint_column and self.checkpoint_from:
            df = df.filter(f"{cfg.checkpoint_column} > '{self.checkpoint_from}'")

        post_filters = self._read_options.get("post_filters", {})
        for col, expr in post_filters.items():
            df = df.filter(f"{col} {expr}")

        if cfg.checkpoint_column:
            max_row = df.agg({cfg.checkpoint_column: "max"}).collect()
            if max_row and max_row[0][0] is not None:
                self.checkpoint_to = str(max_row[0][0])

        return df

    def validate_connection(self) -> bool:
        """
        Validate access by checking the path exists on the filesystem.
        Uses Spark's Hadoop FileSystem API.
        """
        self._configure_spark_for_filesystem()
        hadoop_conf = self.spark.sparkContext._jvm.org.apache.hadoop.conf.Configuration()
        fs_path = self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path(self.source_config.path)
        fs = fs_path.getFileSystem(hadoop_conf)
        return fs.exists(fs_path)

    def infer_schema(self) -> StructType:
        """Sample one file from the path to infer schema."""
        self._configure_spark_for_filesystem()
        fmt = self.source_config.file_format.value
        return self.spark.read.format(fmt).load(self.source_config.path).schema

    def get_record_count(self) -> int:
        """
        List all files under the path for logging.
        Returns the file count rather than row count to avoid expensive full scans.
        """
        self._configure_spark_for_filesystem()
        files = self._list_files()
        return len(files)
