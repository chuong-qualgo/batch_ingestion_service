from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from adapters.source.nosql_adapter import NoSQLAdapter
from adapters.source.base_read_adapter import TableSourceConfig


class SourceMongoDBAdapter(NoSQLAdapter):
    """
    Reads data from a MongoDB collection via the Spark MongoDB connector.

    Required credentials keys (fetched from OpenBao by Airflow):
        username : str  — MongoDB user
        password : str  — MongoDB password

    Required package:
        org.mongodb.spark:mongo-spark-connector_2.12:<version>
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
        return "mongodb"

    def _build_connection_options(self) -> dict:
        cfg = self.source_config
        username = self.credentials.get("username")
        password = self.credentials.get("password")
        connection_string = (
            f"mongodb://{username}:{password}@{cfg.host}:{cfg.port}"
            f"/{cfg.database}?authSource=admin"
        )
        options = {
            "spark.mongodb.read.connection.uri": connection_string,
            "spark.mongodb.read.database": cfg.database,
            "spark.mongodb.read.collection": cfg.table,
        }
        # Apply checkpoint filter as a MongoDB aggregation pipeline filter
        if cfg.checkpoint_column and self.checkpoint_from:
            options["spark.mongodb.read.aggregation.pipeline"] = (
                f'[{{"$match": {{"{cfg.checkpoint_column}": {{"$gt": "{self.checkpoint_from}"}}}}}}]'
            )
        # Apply query override as aggregation pipeline
        if cfg.query:
            options["spark.mongodb.read.aggregation.pipeline"] = cfg.query

        return options

    def validate_connection(self) -> bool:
        """Ping MongoDB by reading a single document."""
        options = self._build_connection_options()
        options["spark.mongodb.read.aggregation.pipeline"] = '[{"$limit": 1}]'
        df = self.spark.read.format(self.spark_format).options(**options).load()
        return df is not None

    def get_record_count(self) -> int:
        """
        Return estimated document count via MongoDB $count aggregation.
        Uses estimatedDocumentCount behaviour via $collStats stage.
        """
        options = self._build_connection_options()
        options["spark.mongodb.read.aggregation.pipeline"] = (
            '[{"$collStats": {"count": {}}}, {"$group": {"_id": null, "count": {"$sum": "$count"}}}]'
        )
        count_df = self.spark.read.format(self.spark_format).options(**options).load()
        rows = count_df.collect()
        return rows[0]["count"] if rows else 0
