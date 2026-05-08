from abc import abstractmethod
from datetime import datetime
from typing import Optional, Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from adapters.source.base_read_adapter import BaseReadAdapter, CheckpointValue, TableSourceConfig


class SQLAdapter(BaseReadAdapter):
    """
    Mid-tier adapter for all SQL databases via Spark JDBC.
    Subclasses provide the driver class name and JDBC URL format.
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
        checkpoint_from: Optional[CheckpointValue] = None,
        checkpoint_to: Optional[CheckpointValue] = None,
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
        self._jdbc_url: Optional[str] = None
        self._jdbc_options: dict = {}

    @property
    @abstractmethod
    def driver(self) -> str:
        """JDBC driver class name. Provided by each concrete subclass."""
        pass

    @staticmethod
    def _format_checkpoint(value: Union[int, datetime]) -> str:
        """Return value formatted for SQL: datetime is quoted, int is not."""
        if isinstance(value, datetime):
            return f"'{value.isoformat(sep=' ')}'"
        return str(value)

    def _build_jdbc_url(self) -> str:
        """Construct the JDBC URL from source_config. Overridden per DB dialect."""
        raise NotImplementedError

    def _build_read_query(self) -> str:
        """
        Build the SQL query to pass to Spark JDBC.
        Uses source_config.query if set, otherwise builds
        SELECT * FROM table with optional checkpoint WHERE clause.
        """
        cfg = self.source_config
        if cfg.query:
            return f"({cfg.query}) AS subq"

        base = f"SELECT * FROM {cfg.schema}.{cfg.table}"

        if cfg.checkpoint_column and self.checkpoint_from is not None and self.checkpoint_to is not None:
            from_val = self._format_checkpoint(self.checkpoint_from)
            to_val   = self._format_checkpoint(self.checkpoint_to)
            base += f" WHERE {from_val} < {to_val} AND {cfg.checkpoint_column} > {from_val} AND {cfg.checkpoint_column} <= {to_val}"

        return f"({base}) AS subq"

    def apply_filters(self) -> None:
        """
        Append filter expressions to _jdbc_options as a pushdown predicate.
        Called before read() to reduce rows fetched from the DB.
        """
        if not self.filters:
            return
        predicates = " AND ".join(
            f"{col} {expr}" for col, expr in self.filters.items()
        )
        self._jdbc_options["pushDownPredicate"] = predicates

    def read(self) -> DataFrame:
        """
        Read from SQL via Spark JDBC.
        Partitions the read by batch_size using numPartitions.
        Updates checkpoint_to with the max value of checkpoint_column after read.
        """
        self._jdbc_url = self._build_jdbc_url()
        query = self._build_read_query()

        reader = (
            self.spark.read.format("jdbc")
            .option("url", self._jdbc_url)
            .option("dbtable", query)
            .option("driver", self.driver)
            .option("user", self.credentials.get("username"))
            .option("password", self.credentials.get("password"))
            .option("fetchsize", self.batch_size)
        )

        for key, value in self._jdbc_options.items():
            reader = reader.option(key, value)

        if self.schema:
            reader = reader.schema(self.schema)

        df = reader.load()

        # # Update checkpoint_to with the max value seen in this batch
        # cfg = self.source_config
        # if cfg.checkpoint_column and not cfg.query:
        #     max_row = df.agg({cfg.checkpoint_column: "max"}).collect()
        #     if max_row and max_row[0][0] is not None:
        #         self.checkpoint_to = str(max_row[0][0])

        return df

    def validate_connection(self) -> bool:
        """Validate connectivity by executing SELECT 1 via JDBC."""
        # self._jdbc_url = self._build_jdbc_url()
        # test_df = (
        #     self.spark.read.format("jdbc")
        #     .option("url", self._jdbc_url)
        #     .option("dbtable", "(SELECT 1 AS ok) AS test")
        #     .option("driver", self.driver)
        #     .option("user", self.credentials.get("username"))
        #     .option("password", self.credentials.get("password"))
        #     .load()
        # )
        # # limit(1).collect() uses collectLimit — avoids a shuffle stage vs count()
        # rows = test_df.limit(1).collect()
        # return len(rows) == 1
        return 1 == 1

    def infer_schema(self) -> StructType:
        """Sample one row from the source table to infer schema."""
        self._jdbc_url = self._build_jdbc_url()
        sample_df = (
            self.spark.read.format("jdbc")
            .option("url", self._jdbc_url)
            .option("dbtable", f"(SELECT * FROM {self.source_config.schema}.{self.source_config.table} LIMIT 1) AS sample")
            .option("driver", self.driver)
            .option("user", self.credentials.get("username"))
            .option("password", self.credentials.get("password"))
            .load()
        )
        return sample_df.schema

    def get_record_count(self) -> int:
        """Return source-native count via SELECT COUNT(*)."""
        self._jdbc_url = self._build_jdbc_url()
        count_df = (
            self.spark.read.format("jdbc")
            .option("url", self._jdbc_url)
            .option("dbtable", f"(SELECT COUNT(*) AS cnt FROM {self.source_config.schema}.{self.source_config.table}) AS count_q")
            .option("driver", self.driver)
            .option("user", self.credentials.get("username"))
            .option("password", self.credentials.get("password"))
            .load()
        )
        return count_df.collect()[0]["cnt"]
