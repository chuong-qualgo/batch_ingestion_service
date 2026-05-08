from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional, Union

from adapters.factory.adapter_config import (
    AdapterConfig,
    MetricAdapterType,
    ReadAdapterType,
    WriteAdapterType,
)
from adapters.source.base_read_adapter import CheckpointValue, PathSourceConfig, TableSourceConfig


def _serialise_checkpoint(val: Optional[CheckpointValue]) -> Optional[dict]:
    if val is None:
        return None
    if isinstance(val, datetime):
        return {"t": "ts", "v": val.isoformat()}
    return {"t": "int", "v": val}


def _deserialise_checkpoint(raw) -> Optional[CheckpointValue]:
    if raw is None or not isinstance(raw, dict):
        return None
    if raw.get("t") == "ts":
        return datetime.fromisoformat(raw["v"])
    if raw.get("t") == "int":
        return int(raw["v"])
    return None
from adapters.write.base_write_adapter import SinkConfig


@dataclass
class RunContext:
    """
    Carries all resolved runtime state from InitOperator to SparkRunOperator.
    Serialised to / from Airflow XCom as a plain dict via `to_dict()` / `from_dict()`.

    Attributes
    ----------
    dag_id : str
        Airflow DAG id — used as the MongoDB checkpoint lookup key.
    run_id : str
        Airflow run_id — written into the sink path partition.
    ingestion_date : date
        Airflow execution date — written into the sink path partition.
    ingestion_time : datetime
        Airflow execution datetime — written into the sink path partition.
    read_type : ReadAdapterType
        Which source adapter to instantiate in SparkRunOperator.
    write_type : WriteAdapterType
        Which sink adapter to instantiate in SparkRunOperator.
    metric_type : MetricAdapterType
        Which metric adapter to instantiate in SparkRunOperator.
    source_config : TableSourceConfig | PathSourceConfig
        Fully built source config (constructed by InitOperator).
    sink_config : SinkConfig
        Fully built sink config with ingestion partitions (constructed by InitOperator).
    source_credentials : dict
        Credentials fetched from OpenBao for the source system.
    sink_credentials : dict
        Credentials fetched from OpenBao for the sink system.
    metric_credentials : dict
        Credentials fetched from OpenBao for the metric queue.
    metric_config_raw : dict
        Raw metric queue config dict from YAML (queue_url / host / stream_name etc.).
        Passed directly to MetricAdapterFactory and metric_operator helpers.
    checkpoint_from : str, optional
        Last successful checkpoint value fetched from MongoDB.
        None means full read — no incremental filter applied.
    checkpoint_to : str, optional
        MAX(checkpoint_column) fetched from the source by InitOperator.
        Passed to SparkRunOperator as the upper bound of the read query.
        Written to MongoDB by MetricPushOperator after a successful run.
    """

    dag_id: str
    run_id: str
    ingestion_date: date
    ingestion_time: datetime
    read_type: ReadAdapterType
    write_type: WriteAdapterType
    metric_type: MetricAdapterType
    source_config: Union[TableSourceConfig, PathSourceConfig]
    sink_config: SinkConfig
    source_credentials: dict
    sink_credentials: dict
    metric_credentials: dict
    metric_config_raw: dict
    checkpoint_from: Optional[CheckpointValue] = None
    checkpoint_to: Optional[CheckpointValue] = None

    # ── XCom serialisation ────────────────────────────────────────────────

    def to_dict(self) -> dict:
        """Serialise RunContext to a plain dict for XCom storage."""
        import dataclasses
        raw = dataclasses.asdict(self)
        # Enums → string values
        raw["read_type"] = self.read_type.value
        raw["write_type"] = self.write_type.value
        raw["metric_type"] = self.metric_type.value
        # date/datetime → ISO strings (top-level and inside nested sink_config)
        raw["ingestion_date"] = self.ingestion_date.isoformat()
        raw["ingestion_time"] = self.ingestion_time.isoformat()
        raw["sink_config"]["ingestion_date"] = self.sink_config.ingestion_date.isoformat()
        raw["sink_config"]["ingestion_time"] = self.sink_config.ingestion_time.isoformat()
        raw["checkpoint_from"] = _serialise_checkpoint(self.checkpoint_from)
        raw["checkpoint_to"]   = _serialise_checkpoint(self.checkpoint_to)
        return raw

    @classmethod
    def from_dict(cls, data: dict) -> RunContext:
        """Deserialise a RunContext from an XCom dict."""
        from adapters.factory.source_config_factory import SourceConfigFactory
        from adapters.factory.sink_config_factory import SinkConfigFactory

        read_type = ReadAdapterType(data["read_type"])
        write_type = WriteAdapterType(data["write_type"])
        metric_type = MetricAdapterType(data["metric_type"])
        ingestion_date = date.fromisoformat(data["ingestion_date"])
        ingestion_time = datetime.fromisoformat(data["ingestion_time"])

        source_config = SourceConfigFactory.create(
            adapter_type=read_type,
            **data["source_config"],
        )
        SourceConfigFactory.inject_connection(source_config, data["source_config"])
        # Reconstruct SinkConfig — endpoint is stored in the serialised dict
        # and re-injected directly (it was already resolved from OpenBao previously)
        sink_raw = {k: v for k, v in data["sink_config"].items()
                    if k not in ("source_config", "ingestion_date", "ingestion_time",
                                 "run_id", "endpoint", "extra")}
        sink_config = SinkConfigFactory.create(
            source_config=source_config,
            ingestion_date=ingestion_date,
            ingestion_time=ingestion_time,
            run_id=data["run_id"],
            **sink_raw,
        )
        # Re-inject endpoint that was resolved from OpenBao on original execute
        sink_config.endpoint = data["sink_config"].get("endpoint", "")

        return cls(
            dag_id=data["dag_id"],
            run_id=data["run_id"],
            ingestion_date=ingestion_date,
            ingestion_time=ingestion_time,
            read_type=read_type,
            write_type=write_type,
            metric_type=metric_type,
            source_config=source_config,
            sink_config=sink_config,
            source_credentials=data["source_credentials"],
            sink_credentials=data["sink_credentials"],
            metric_credentials=data.get("metric_credentials", {}),
            metric_config_raw=data.get("metric_config_raw", {}),
            checkpoint_from=_deserialise_checkpoint(data.get("checkpoint_from")),
            checkpoint_to=_deserialise_checkpoint(data.get("checkpoint_to")),
        )
