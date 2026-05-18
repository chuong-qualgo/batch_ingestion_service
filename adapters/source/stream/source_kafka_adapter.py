from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from adapters.source.base_read_adapter import BaseReadAdapter, KafkaSourceConfig


class SourceKafkaAdapter(BaseReadAdapter):
    """
    Reads a bounded batch from Apache Kafka using spark.read.format("kafka").

    Checkpointing uses per-partition Kafka offsets stored as a Spark JSON offset
    string, e.g. '{"orders-events": {"0": 1000, "1": 2000}}'.

    Flow per run:
      1. InitOperator reads checkpoint_from from PostgreSQL (None on first run).
      2. InitOperator calls _max_kafka() → fetches current end offsets from the
         Kafka broker → returns a JSON offset string as checkpoint_to.
      3. SourceKafkaAdapter.read() uses:
           startingOffsets = checkpoint_from  (JSON string, or "earliest" if None)
           endingOffsets   = checkpoint_to    (JSON string, or "latest" if None)
      4. MetricPushOperator stores checkpoint_to in PostgreSQL for the next run.

    This ensures each batch reads exactly the messages between two broker-side
    snapshots — deterministic, gapless, no duplicates.

    Required Spark package: org.apache.spark:spark-sql-kafka-0-10_2.12:<spark-version>

    Required credentials (from OpenBao):
        bootstrap_servers : str — injected into config by InitOperator
        sasl_username     : str, optional — SASL/PLAIN username
        sasl_password     : str, optional — SASL/PLAIN password

    Value decoding (controlled by source_config.value_format):
        "json"   — parses value bytes as JSON; expands schema columns when provided
        "string" — casts value bytes to UTF-8 string + Kafka metadata columns
        "binary" — returns raw Kafka DataFrame (key/value as BinaryType)
    """

    source_config: KafkaSourceConfig

    def read(self) -> DataFrame:
        cfg = self.source_config

        # checkpoint_from is a JSON offset string from the previous run, or None
        starting = str(self.checkpoint_from) if self.checkpoint_from is not None else cfg.starting_offsets
        # checkpoint_to is a JSON offset string fetched from Kafka by InitOperator, or None
        ending = str(self.checkpoint_to) if self.checkpoint_to is not None else "latest"

        reader = (
            self.spark.read.format("kafka")
            .option("kafka.bootstrap.servers", cfg.bootstrap_servers)
            .option("subscribe", cfg.topic)
            .option("startingOffsets", starting)
            .option("endingOffsets", ending)
            .option("failOnDataLoss", "false")
        )

        if cfg.group_id:
            reader = reader.option("kafka.group.id", cfg.group_id)

        sasl_username = self.credentials.get("sasl_username")
        sasl_password = self.credentials.get("sasl_password")
        if sasl_username and sasl_password:
            jaas = (
                "org.apache.kafka.common.security.plain.PlainLoginModule required "
                f'username="{sasl_username}" password="{sasl_password}";'
            )
            reader = (
                reader
                .option("kafka.security.protocol", "SASL_PLAINTEXT")
                .option("kafka.sasl.mechanism", "PLAIN")
                .option("kafka.sasl.jaas.config", jaas)
            )

        for key, val in cfg.read_options.items():
            reader = reader.option(key, val)

        raw_df = reader.load()
        return self._decode_value(raw_df, cfg.value_format)

    def _decode_value(self, df: DataFrame, value_format: str) -> DataFrame:
        if value_format == "binary":
            return df

        str_df = (
            df
            .withColumn("value", F.col("value").cast("string"))
            .withColumn("key", F.col("key").cast("string"))
        )

        if value_format == "string":
            return str_df

        # json — parse value column using injected schema or keep as string
        if value_format == "json" and self.schema:
            return (
                str_df
                .withColumn("data", F.from_json(F.col("value"), self.schema))
                .select("data.*", "key", "topic", "partition", "offset", "timestamp")
            )

        return str_df

    def validate_connection(self) -> bool:
        return True

    def infer_schema(self) -> StructType:
        return self.read().limit(1).schema

    def get_record_count(self) -> int:
        return self.read().count()

    def apply_filters(self) -> None:
        pass
