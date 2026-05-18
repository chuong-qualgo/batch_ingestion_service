"""Tests for SourceKafkaAdapter."""
import json
import pytest
from unittest.mock import MagicMock, patch

from pyspark.sql.types import StringType, StructField, StructType

from adapters.source.base_read_adapter import KafkaSourceConfig
from adapters.source.stream.source_kafka_adapter import SourceKafkaAdapter


# ── Fixtures ──────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def mock_F():
    """
    Patch pyspark.sql.functions so tests don't need an active SparkContext.
    autouse=True applies this to every test in this module automatically.
    """
    mock_functions = MagicMock()
    with patch("adapters.source.stream.source_kafka_adapter.F", mock_functions):
        yield mock_functions


@pytest.fixture
def kafka_config():
    return KafkaSourceConfig(
        credential_ref="data-platform/kafka",
        bootstrap_servers="broker1:9092,broker2:9092",
        topic="orders-events",
        group_id="batch-ingestion",
        starting_offsets="earliest",
        value_format="json",
    )


@pytest.fixture
def mock_raw_df():
    """Mock DataFrame returned by reader.load() — simulates raw Kafka columns."""
    df = MagicMock()
    df.withColumn.return_value = df
    df.select.return_value = df
    return df


@pytest.fixture
def mock_reader(mock_raw_df):
    """Fluent Spark reader mock — .option() always returns itself."""
    reader = MagicMock()
    reader.option.return_value = reader
    reader.load.return_value = mock_raw_df
    return reader


@pytest.fixture
def mock_spark(mock_reader):
    spark = MagicMock()
    spark.read.format.return_value = mock_reader
    return spark


@pytest.fixture
def adapter(mock_spark, kafka_config):
    return SourceKafkaAdapter(
        spark=mock_spark,
        source_config=kafka_config,
        credentials={},
    )


def _option_calls(mock_reader) -> dict:
    """Collapse all .option(key, val) calls into a flat dict for easy assertion."""
    return {args[0]: args[1] for args, _ in mock_reader.option.call_args_list}


# ── read() — format and core options ─────────────────────────────────────

def test_read_uses_kafka_format(adapter, mock_spark):
    adapter.read()
    mock_spark.read.format.assert_called_once_with("kafka")


def test_read_sets_bootstrap_servers(adapter, mock_reader):
    adapter.read()
    assert _option_calls(mock_reader)["kafka.bootstrap.servers"] == "broker1:9092,broker2:9092"


def test_read_sets_subscribe_topic(adapter, mock_reader):
    adapter.read()
    assert _option_calls(mock_reader)["subscribe"] == "orders-events"


def test_read_sets_failOnDataLoss_false(adapter, mock_reader):
    adapter.read()
    assert _option_calls(mock_reader)["failOnDataLoss"] == "false"


# ── read() — startingOffsets ──────────────────────────────────────────────

def test_read_uses_starting_offsets_from_config_when_no_checkpoint(adapter, mock_reader):
    """No checkpoint_from → falls back to source_config.starting_offsets ('earliest')."""
    adapter.read()
    assert _option_calls(mock_reader)["startingOffsets"] == "earliest"


def test_read_uses_checkpoint_from_as_starting_offsets(mock_spark, kafka_config, mock_reader):
    """checkpoint_from (per-partition JSON string) is passed directly to startingOffsets."""
    offset_json = json.dumps({"orders-events": {"0": 1000, "1": 2000}})
    a = SourceKafkaAdapter(
        spark=mock_spark,
        source_config=kafka_config,
        credentials={},
        checkpoint_from=offset_json,
    )
    a.read()
    assert _option_calls(mock_reader)["startingOffsets"] == offset_json


def test_read_uses_latest_as_starting_offset_when_configured(mock_spark, kafka_config, mock_reader):
    kafka_config.starting_offsets = "latest"
    a = SourceKafkaAdapter(spark=mock_spark, source_config=kafka_config, credentials={})
    a.read()
    assert _option_calls(mock_reader)["startingOffsets"] == "latest"


# ── read() — endingOffsets ────────────────────────────────────────────────

def test_read_sets_ending_offsets_latest_when_no_checkpoint_to(adapter, mock_reader):
    """No checkpoint_to → endingOffsets = 'latest'."""
    adapter.read()
    assert _option_calls(mock_reader)["endingOffsets"] == "latest"


def test_read_uses_checkpoint_to_as_ending_offsets(mock_spark, kafka_config, mock_reader):
    """checkpoint_to (per-partition JSON string from InitOperator) is passed to endingOffsets."""
    offset_json = json.dumps({"orders-events": {"0": 1500, "1": 2500}})
    a = SourceKafkaAdapter(
        spark=mock_spark,
        source_config=kafka_config,
        credentials={},
        checkpoint_to=offset_json,
    )
    a.read()
    assert _option_calls(mock_reader)["endingOffsets"] == offset_json


def test_read_uses_both_offset_jsons_when_both_checkpoints_set(mock_spark, kafka_config, mock_reader):
    """Both checkpoints present → startingOffsets and endingOffsets are the respective JSON strings."""
    from_json = json.dumps({"orders-events": {"0": 1000, "1": 2000}})
    to_json   = json.dumps({"orders-events": {"0": 1500, "1": 2500}})
    a = SourceKafkaAdapter(
        spark=mock_spark,
        source_config=kafka_config,
        credentials={},
        checkpoint_from=from_json,
        checkpoint_to=to_json,
    )
    a.read()
    opts = _option_calls(mock_reader)
    assert opts["startingOffsets"] == from_json
    assert opts["endingOffsets"]   == to_json


# ── read() — group_id ─────────────────────────────────────────────────────

def test_read_sets_group_id_when_provided(adapter, mock_reader):
    adapter.read()
    assert _option_calls(mock_reader)["kafka.group.id"] == "batch-ingestion"


def test_read_skips_group_id_when_empty(mock_spark, kafka_config, mock_reader):
    kafka_config.group_id = ""
    a = SourceKafkaAdapter(spark=mock_spark, source_config=kafka_config, credentials={})
    a.read()
    assert "kafka.group.id" not in _option_calls(mock_reader)


# ── read() — SASL auth ────────────────────────────────────────────────────

def test_read_adds_sasl_options_when_credentials_present(mock_spark, kafka_config, mock_reader):
    a = SourceKafkaAdapter(
        spark=mock_spark,
        source_config=kafka_config,
        credentials={"sasl_username": "user1", "sasl_password": "pass1"},
    )
    a.read()
    opts = _option_calls(mock_reader)
    assert opts["kafka.security.protocol"] == "SASL_PLAINTEXT"
    assert opts["kafka.sasl.mechanism"] == "PLAIN"
    assert 'username="user1"' in opts["kafka.sasl.jaas.config"]
    assert 'password="pass1"' in opts["kafka.sasl.jaas.config"]
    assert "PlainLoginModule" in opts["kafka.sasl.jaas.config"]


def test_read_skips_sasl_when_no_credentials(adapter, mock_reader):
    adapter.read()
    opts = _option_calls(mock_reader)
    assert "kafka.security.protocol" not in opts
    assert "kafka.sasl.mechanism" not in opts
    assert "kafka.sasl.jaas.config" not in opts


def test_read_skips_sasl_when_only_username_present(mock_spark, kafka_config, mock_reader):
    a = SourceKafkaAdapter(
        spark=mock_spark,
        source_config=kafka_config,
        credentials={"sasl_username": "user1"},
    )
    a.read()
    assert "kafka.security.protocol" not in _option_calls(mock_reader)


# ── read() — extra read_options passthrough ───────────────────────────────

def test_read_passes_through_read_options(mock_spark, kafka_config, mock_reader):
    kafka_config.read_options = {"maxOffsetsPerTrigger": "100000", "minPartitions": "4"}
    a = SourceKafkaAdapter(spark=mock_spark, source_config=kafka_config, credentials={})
    a.read()
    opts = _option_calls(mock_reader)
    assert opts["maxOffsetsPerTrigger"] == "100000"
    assert opts["minPartitions"] == "4"


def test_read_with_no_read_options_does_not_fail(adapter):
    adapter.read()  # no exception


# ── _decode_value() — binary ──────────────────────────────────────────────

def test_decode_binary_returns_df_unchanged(adapter, mock_raw_df):
    result = adapter._decode_value(mock_raw_df, "binary")
    assert result is mock_raw_df
    mock_raw_df.withColumn.assert_not_called()


# ── _decode_value() — string ──────────────────────────────────────────────

def test_decode_string_casts_value_and_key(adapter, mock_raw_df):
    adapter._decode_value(mock_raw_df, "string")
    col_names = [c[0][0] for c in mock_raw_df.withColumn.call_args_list]
    assert "value" in col_names
    assert "key" in col_names


def test_decode_string_does_not_call_select(adapter, mock_raw_df):
    adapter._decode_value(mock_raw_df, "string")
    mock_raw_df.select.assert_not_called()


# ── _decode_value() — json without schema ────────────────────────────────

def test_decode_json_without_schema_does_not_call_select(adapter, mock_raw_df):
    adapter._decode_value(mock_raw_df, "json")
    mock_raw_df.select.assert_not_called()


def test_decode_json_without_schema_does_not_call_from_json(adapter, mock_raw_df, mock_F):
    adapter._decode_value(mock_raw_df, "json")
    mock_F.from_json.assert_not_called()


# ── _decode_value() — json with schema ───────────────────────────────────

def test_decode_json_with_schema_calls_from_json(mock_spark, kafka_config, mock_raw_df):
    schema = StructType([StructField("order_id", StringType())])
    a = SourceKafkaAdapter(
        spark=mock_spark,
        source_config=kafka_config,
        credentials={},
        schema=schema,
    )
    a._decode_value(mock_raw_df, "json")
    col_names = [c[0][0] for c in mock_raw_df.withColumn.call_args_list]
    assert "data" in col_names


def test_decode_json_with_schema_calls_select(mock_spark, kafka_config, mock_raw_df):
    schema = StructType([StructField("order_id", StringType())])
    a = SourceKafkaAdapter(
        spark=mock_spark,
        source_config=kafka_config,
        credentials={},
        schema=schema,
    )
    a._decode_value(mock_raw_df, "json")
    mock_raw_df.select.assert_called_once()


def test_decode_json_with_schema_selects_kafka_metadata_columns(mock_spark, kafka_config, mock_raw_df):
    schema = StructType([StructField("order_id", StringType())])
    a = SourceKafkaAdapter(
        spark=mock_spark,
        source_config=kafka_config,
        credentials={},
        schema=schema,
    )
    a._decode_value(mock_raw_df, "json")
    selected = str(mock_raw_df.select.call_args)
    for col in ("key", "topic", "partition", "offset", "timestamp"):
        assert col in selected


# ── Helper methods ────────────────────────────────────────────────────────

def test_validate_connection_returns_true(adapter):
    assert adapter.validate_connection() is True


def test_get_record_count_delegates_to_read(adapter, mock_raw_df):
    mock_raw_df.count.return_value = 42
    assert adapter.get_record_count() == 42


def test_infer_schema_calls_limit_then_schema(adapter, mock_raw_df):
    mock_raw_df.limit.return_value = mock_raw_df
    adapter.infer_schema()
    mock_raw_df.limit.assert_called_once_with(1)


def test_apply_filters_is_noop(adapter):
    adapter.apply_filters()  # must not raise
