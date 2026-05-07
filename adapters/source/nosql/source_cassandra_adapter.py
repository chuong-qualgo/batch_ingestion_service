from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from adapters.source.nosql_adapter import NoSQLAdapter
from adapters.source.base_read_adapter import TableSourceConfig


class SourceCassandraAdapter(NoSQLAdapter):
    """
    Reads data from an Apache Cassandra table via the Spark Cassandra connector.
    Uses `database` as the Cassandra keyspace and `table` as the table name.

    Required credentials keys (fetched from OpenBao by Airflow):
        username : str  — Cassandra user
        password : str  — Cassandra password

    Required package:
        com.datastax.spark:spark-cassandra-connector_2.12:<version>
    """

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

    @property
    def spark_format(self) -> str:
        return "org.apache.spark.sql.cassandra"

    def _build_connection_options(self) -> dict:
        cfg = self.source_config
        return {
            "keyspace": cfg.database,
            "table": cfg.table,
            "spark.cassandra.connection.host": cfg.host,
            "spark.cassandra.connection.port": str(cfg.port),
            "spark.cassandra.auth.username": self.credentials.get("username"),
            "spark.cassandra.auth.password": self.credentials.get("password"),
            "spark.cassandra.input.fetch.size_in_rows": str(self.batch_size),
        }

    def apply_filters(self) -> None:
        """
        Build a CQL WHERE clause from self.filters and store for pushdown.
        Cassandra only supports pushdown on partition and clustering columns.
        """
        if not self.filters:
            return
        where_clause = " AND ".join(
            f"{col} {expr}" for col, expr in self.filters.items()
        )
        self._read_options["pushdown"] = "true"
        self._read_options["where"] = where_clause

    def validate_connection(self) -> bool:
        """Validate by reading cluster metadata via the Cassandra Python driver."""
        from cassandra.cluster import Cluster
        from cassandra.auth import PlainTextAuthProvider

        auth = PlainTextAuthProvider(
            username=self.credentials.get("username"),
            password=self.credentials.get("password"),
        )
        cluster = Cluster(
            [self.source_config.host],
            port=self.source_config.port,
            auth_provider=auth,
        )
        session = cluster.connect()
        is_connected = session is not None
        cluster.shutdown()
        return is_connected

    def get_record_count(self) -> int:
        """Return row count via SELECT COUNT(*) on the Cassandra table."""
        options = self._build_connection_options()
        count_df = (
            self.spark.read
            .format(self.spark_format)
            .options(**options)
            .load()
            .selectExpr("count(*) as cnt")
        )
        return count_df.collect()[0]["cnt"]
