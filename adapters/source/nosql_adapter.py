from abc import abstractmethod
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from adapters.source.base_read_adapter import BaseReadAdapter, TableSourceConfig


class NoSQLAdapter(BaseReadAdapter):
    """
    Mid-tier adapter for NoSQL stores via Spark connectors.
    Subclasses provide the Spark format string and connection options.
    """

    source_config: TableSourceConfig

    def __init__(
        self,
        spark: SparkSession,
        source_config: TableSourceConfig,
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

    @property
    @abstractmethod
    def spark_format(self) -> str:
        """Spark data source format string. Provided by each concrete subclass."""
        pass

    @abstractmethod
    def _build_connection_options(self) -> dict:
        """
        Build the format-specific connection options dict.
        Provided by each concrete subclass.
        """
        pass

    def apply_filters(self) -> None:
        """
        Store filter expressions in _read_options for pushdown.
        Concrete subclasses translate these into format-specific filter syntax.
        """
        if self.filters:
            self._read_options["filters"] = self.filters

    def read(self) -> DataFrame:
        """
        Read from the NoSQL source via Spark connector.
        Applies checkpoint and filters via _read_options.
        Updates checkpoint_to with max checkpoint_column value after read.
        """
        options = self._build_connection_options()
        options.update(self._read_options)

        reader = self.spark.read.format(self.spark_format).options(**options)

        if self.schema:
            reader = reader.schema(self.schema)

        df = reader.load()

        cfg = self.source_config
        if cfg.checkpoint_column and self.checkpoint_from:
            df = df.filter(f"{cfg.checkpoint_column} > '{self.checkpoint_from}'")

        if cfg.checkpoint_column:
            max_row = df.agg({cfg.checkpoint_column: "max"}).collect()
            if max_row and max_row[0][0] is not None:
                self.checkpoint_to = str(max_row[0][0])

        return df

    def infer_schema(self) -> StructType:
        """Sample records from the source to infer schema."""
        options = self._build_connection_options()
        options["sampleSize"] = "1"
        return self.spark.read.format(self.spark_format).options(**options).load().schema

    def validate_connection(self) -> bool:
        pass

    def get_record_count(self) -> int:
        pass
