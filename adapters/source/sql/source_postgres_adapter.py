from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from adapters.source.sql_adapter import SQLAdapter
from adapters.source.base_read_adapter import TableSourceConfig


class SourcePostgresAdapter(SQLAdapter):
    """
    Reads data from a PostgreSQL database via Spark JDBC.

    Required credentials keys (fetched from OpenBao by Airflow):
        username : str  — PostgreSQL user
        password : str  — PostgreSQL password

    Required JAR:
        org.postgresql:postgresql:<version>
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
    def driver(self) -> str:
        return "org.postgresql.Driver"

    def _build_jdbc_url(self) -> str:
        cfg = self.source_config
        return f"jdbc:postgresql://{cfg.host}:{cfg.port}/{cfg.database}"
