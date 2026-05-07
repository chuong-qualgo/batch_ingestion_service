from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from adapters.source.sql_adapter import SQLAdapter
from adapters.source.base_read_adapter import TableSourceConfig


class SourceMySQLAdapter(SQLAdapter):
    """
    Reads data from a MySQL database via Spark JDBC.

    Required credentials keys (fetched from OpenBao by Airflow):
        username : str  — MySQL user
        password : str  — MySQL password

    Required JAR:
        com.mysql:mysql-connector-j:<version>
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
        return "com.mysql.cj.jdbc.Driver"

    def _build_jdbc_url(self) -> str:
        cfg = self.source_config
        return (
            f"jdbc:mysql://{cfg.host}:{cfg.port}/{cfg.database}"
            f"?useSSL=false&allowPublicKeyRetrieval=true"
        )
