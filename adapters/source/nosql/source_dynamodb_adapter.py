from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from adapters.source.nosql_adapter import NoSQLAdapter
from adapters.source.base_read_adapter import TableSourceConfig


class SourceDynamoDBAdapter(NoSQLAdapter):
    """
    Reads data from an AWS DynamoDB table via the Spark DynamoDB connector.

    Required credentials keys (fetched from OpenBao by Airflow):
        aws_access_key_id     : str
        aws_secret_access_key : str
        aws_region            : str

    Required package:
        com.audienceproject:spark-dynamodb_2.12:<version>
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
        return "dynamodb"

    def _build_connection_options(self) -> dict:
        cfg = self.source_config
        return {
            "tableName": cfg.table,
            "region": self.credentials.get("aws_region"),
            "accessKey": self.credentials.get("aws_access_key_id"),
            "secretKey": self.credentials.get("aws_secret_access_key"),
            "readThroughput": str(self.batch_size),
        }

    def validate_connection(self) -> bool:
        """Validate by describing the DynamoDB table via boto3."""
        import boto3
        client = boto3.client(
            "dynamodb",
            region_name=self.credentials.get("aws_region"),
            aws_access_key_id=self.credentials.get("aws_access_key_id"),
            aws_secret_access_key=self.credentials.get("aws_secret_access_key"),
        )
        response = client.describe_table(TableName=self.source_config.table)
        return response["Table"]["TableStatus"] == "ACTIVE"

    def get_record_count(self) -> int:
        """
        Return the approximate item count from DynamoDB table metadata via boto3.
        Note: DynamoDB updates this count every ~6 hours, so it is approximate.
        """
        import boto3
        client = boto3.client(
            "dynamodb",
            region_name=self.credentials.get("aws_region"),
            aws_access_key_id=self.credentials.get("aws_access_key_id"),
            aws_secret_access_key=self.credentials.get("aws_secret_access_key"),
        )
        response = client.describe_table(TableName=self.source_config.table)
        return response["Table"].get("ItemCount", 0)
