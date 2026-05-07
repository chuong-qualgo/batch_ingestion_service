from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Optional

import yaml
from airflow.models import BaseOperator
from airflow.utils.context import Context
from pymongo import MongoClient

from adapters.factory.adapter_config import (
    MetricAdapterType,
    ReadAdapterType,
    WriteAdapterType,
)
from adapters.factory.sink_config_factory import SinkConfigFactory
from adapters.factory.source_config_factory import SourceConfigFactory
from adapters.source.base_read_adapter import PathSourceConfig
from orchestration.operators.run_context import RunContext
from orchestration.plugins.openbao_hook import OpenBaoHook

log = logging.getLogger(__name__)


class InitOperator(BaseOperator):
    """
    Airflow operator that prepares all runtime context for the pipeline run.

    Execution steps
    ---------------
    1. Load and parse the YAML config file from `config_path`.
    2. Build SourceConfig from the YAML `source` section via SourceConfigFactory.
    3. Build SinkConfig from the YAML `sink` section via SinkConfigFactory,
       binding ingestion_date, ingestion_time, and run_id from the Airflow context.
    4. Fetch source credentials from OpenBao using source_config.credential_ref.
    5. Fetch sink credentials from OpenBao using sink_config.credential_ref.
    6. Fetch metric credentials from OpenBao using metric.credential_ref from YAML.
    7. Fetch checkpoint_from from MongoDB using DAG id as the lookup key.
       Returns None (full read) if no checkpoint record exists.
    8. Fetch source record count for logging.
    9. Assemble a RunContext and push it to XCom for SparkRunOperator.

    Parameters
    ----------
    config_path : str
        Absolute path to the YAML pipeline config file.
        The file must contain `source`, `sink`, `read_type`, `write_type`,
        and `metric_type` sections. See config/settings.py for schema.
    mongo_conn_id : str
        Airflow connection id for the MongoDB checkpoint store.
    openbao_conn_id : str
        Airflow connection id for OpenBao. Defaults to 'openbao_default'.
    xcom_key : str
        XCom key under which RunContext is pushed. Defaults to 'run_context'.

    YAML config structure
    ---------------------
    read_type: sql                      # ReadAdapterType value
    write_type: hadoop                  # WriteAdapterType value
    metric_type: cloud_queue            # MetricAdapterType value

    source:
      credential_ref: data-processor/postgres
      host: localhost
      port: 5432
      database: orders_db
      schema: public
      table: orders
      checkpoint_column: updated_at     # omit for full read

    sink:
      credential_ref: data-platform/hadoop
      endpoint: hdfs://namenode:9000
      source_system_name: postgres-prod

    metric:
      credential_ref: data-platform/redis   # OpenBao key for queue credentials
      host: redis.infra.svc.cluster.local
      port: 6379
      stream_name: pipeline-metrics
      max_len: 5000
    """

    template_fields = ("config_path",)

    def __init__(
        self,
        config_path: str,
        mongo_conn_id: str = "mongo_checkpoint",
        openbao_conn_id: str = OpenBaoHook.default_conn_name,
        xcom_key: str = "run_context",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.config_path = config_path
        self.mongo_conn_id = mongo_conn_id
        self.openbao_conn_id = openbao_conn_id
        self.xcom_key = xcom_key

    # ── Main execute ──────────────────────────────────────────────────────

    def execute(self, context: Context) -> dict:
        dag_id: str = context["dag"].dag_id
        run_id: str = context["run_id"]
        ingestion_date = context["data_interval_start"].date()
        ingestion_time: datetime = context["data_interval_start"]

        log.info("[InitOperator] Starting — dag_id=%s run_id=%s", dag_id, run_id)

        # ── 1. Load YAML config ───────────────────────────────────────────
        cfg = self._load_config(self.config_path)
        log.info("[InitOperator] Config loaded from %s", self.config_path)

        read_type = ReadAdapterType(cfg["read_type"])
        write_type = WriteAdapterType(cfg["write_type"])
        metric_type = MetricAdapterType(cfg.get("metric_type", "cloud_queue"))

        # ── 2. Build SourceConfig ─────────────────────────────────────────
        source_cfg_raw: dict = cfg["source"]
        source_config = SourceConfigFactory.create(
            adapter_type=read_type,
            **source_cfg_raw,
        )
        log.info("[InitOperator] SourceConfig built — type=%s", read_type.value)

        # ── 3. Build SinkConfig ───────────────────────────────────────────
        sink_cfg_raw: dict = cfg["sink"]
        sink_config = SinkConfigFactory.create(
            endpoint=sink_cfg_raw["endpoint"],
            credential_ref=sink_cfg_raw["credential_ref"],
            source_system_name=sink_cfg_raw["source_system_name"],
            source_config=source_config,
            ingestion_date=ingestion_date,
            ingestion_time=ingestion_time,
            run_id=run_id,
            extra=sink_cfg_raw.get("extra", {}),
        )
        log.info("[InitOperator] SinkConfig built — endpoint=%s", sink_config.endpoint)

        # ── 4. Fetch source credentials from OpenBao ──────────────────────
        openbao = OpenBaoHook(openbao_conn_id=self.openbao_conn_id)
        source_credentials = openbao.get_secret(source_config.credential_ref)
        log.info(
            "[InitOperator] Source credentials fetched — ref=%s",
            source_config.credential_ref,
        )

        # ── 5. Fetch sink credentials from OpenBao ────────────────────────
        sink_credentials = openbao.get_secret(sink_config.credential_ref)
        log.info(
            "[InitOperator] Sink credentials fetched — ref=%s",
            sink_config.credential_ref,
        )

        # ── 6. Fetch metric credentials from OpenBao ─────────────────────
        metric_cfg_raw: dict = cfg.get("metric", {})
        metric_credential_ref = metric_cfg_raw.get("credential_ref", "")
        metric_credentials: dict = {}
        if metric_credential_ref:
            metric_credentials = openbao.get_secret(metric_credential_ref)
            log.info(
                "[InitOperator] Metric credentials fetched — ref=%s",
                metric_credential_ref,
            )
        else:
            log.info("[InitOperator] No metric credential_ref configured — skipping")

        # ── 7. Fetch checkpoint_from from MongoDB ─────────────────────────
        checkpoint_from: Optional[str] = self._fetch_checkpoint_from(dag_id)
        log.info(
            "[InitOperator] checkpoint_from=%s (None = full read)",
            checkpoint_from,
        )

        # ── 8. Log source record count (checkpoint_to comes from reader) ──
        record_count = self._fetch_source_record_count(
            read_type=read_type,
            source_config=source_config,
            source_credentials=source_credentials,
        )
        log.info("[InitOperator] Source record/file count = %s", record_count)

        # ── 9. Assemble RunContext and push to XCom ───────────────────────
        run_context = RunContext(
            dag_id=dag_id,
            run_id=run_id,
            ingestion_date=ingestion_date,
            ingestion_time=ingestion_time,
            read_type=read_type,
            write_type=write_type,
            metric_type=metric_type,
            source_config=source_config,
            sink_config=sink_config,
            source_credentials=source_credentials,
            sink_credentials=sink_credentials,
            metric_credentials=metric_credentials,
            metric_config_raw=metric_cfg_raw,
            checkpoint_from=checkpoint_from,
        )

        serialised = run_context.to_dict()
        context["ti"].xcom_push(key=self.xcom_key, value=serialised)
        log.info("[InitOperator] RunContext pushed to XCom key='%s'", self.xcom_key)
        return serialised

    # ── Helpers ───────────────────────────────────────────────────────────

    @staticmethod
    def _load_config(config_path: str) -> dict:
        """Load and parse the YAML pipeline config file."""
        with open(config_path, "r") as f:
            return yaml.safe_load(f)

    def _fetch_checkpoint_from(self, dag_id: str) -> Optional[str]:
        """
        Fetch the last successful checkpoint value from MongoDB.
        The checkpoint document is keyed by dag_id in the
        'checkpoints' collection of the config database.
        Returns None if no record exists (triggers full read).
        """
        mongo_conn = self.get_connection(self.mongo_conn_id)
        client = MongoClient(
            host=mongo_conn.host,
            port=mongo_conn.port,
            username=mongo_conn.login,
            password=mongo_conn.password,
        )
        try:
            db = client[mongo_conn.schema or "config"]
            doc = db["checkpoints"].find_one({"dag_id": dag_id})
            return doc["checkpoint_from"] if doc else None
        finally:
            client.close()

    @staticmethod
    def _fetch_source_record_count(
        read_type: ReadAdapterType,
        source_config,
        source_credentials: dict,
    ) -> int:
        """
        Instantiate a lightweight source adapter (no Spark) to call
        get_record_count() for logging purposes only.
        Uses a minimal SparkSession scoped to this check.
        """
        from pyspark.sql import SparkSession
        from adapters.factory.read_adapter_factory import ReadAdapterFactory
        from adapters.factory.adapter_config import AdapterConfig

        spark = (
            SparkSession.builder
            .appName("init-record-count")
            .getOrCreate()
        )
        adapter_config = AdapterConfig(read_type=read_type)
        # Temporarily attach source_config so factory can resolve it
        adapter_config.source_config = source_config

        adapter = ReadAdapterFactory.create(
            config=adapter_config,
            spark=spark,
        )
        adapter.credentials = source_credentials
        return adapter.get_record_count()
