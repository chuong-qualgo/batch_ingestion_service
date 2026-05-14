from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Optional

import yaml
import psycopg2
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.utils.context import Context

from adapters.factory.adapter_config import (
    MetricAdapterType,
    ReadAdapterType,
    WriteAdapterType,
)
from adapters.factory.sink_config_factory import SinkConfigFactory
from adapters.factory.source_config_factory import SourceConfigFactory
from adapters.source.base_read_adapter import CheckpointValue, PathSourceConfig
from orchestration.operators.run_context import RunContext
from orchestration.plugins.openbao_hook import OpenBaoHook

log = logging.getLogger(__name__)


def _parse_checkpoint(val) -> Optional["CheckpointValue"]:
    """
    Coerce a raw DB value into a CheckpointValue (int or datetime).
    Accepts int, float/Decimal (→ int), datetime, or an ISO/numeric string.
    Returns None for null inputs.
    """
    if val is None:
        return None
    if isinstance(val, datetime):
        return val
    if isinstance(val, bool):
        raise TypeError(f"Boolean is not a valid checkpoint value: {val!r}")
    if isinstance(val, int):
        return val
    try:
        from decimal import Decimal
        if isinstance(val, (float, Decimal)):
            return int(val)
    except ImportError:
        pass
    # String fallback — try int, then ISO datetime
    s = str(val).strip()
    try:
        return int(s)
    except ValueError:
        pass
    return datetime.fromisoformat(s)


class InitOperator(BaseOperator):
    """
    Airflow operator that prepares all runtime context for the pipeline run.

    Execution steps
    ---------------
    1. Load and parse the YAML config file from `config_path`.
    2. Build SourceConfig from YAML (without host/port/path — those are in OpenBao).
    3. Build SinkConfig from YAML (without endpoint — that is in OpenBao).
    4. Fetch source credentials from OpenBao; inject host/port (or path) into SourceConfig.
    5. Fetch sink credentials from OpenBao; inject endpoint into SinkConfig.
    6. Fetch metric credentials from OpenBao; host/port/queue_url injected at adapter build time.
    7. Fetch checkpoint_from from MongoDB using DAG id as the lookup key.
       Returns None (full read) if no checkpoint record exists.
    8. Fetch checkpoint_to from source via MAX(checkpoint_column).
       None when no checkpoint_column is configured (full read mode).
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
        checkpoint_conn_id: str = "postgres_checkpoint",
        openbao_conn_id: str = OpenBaoHook.default_conn_name,
        xcom_key: str = "run_context",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.config_path = config_path
        self.checkpoint_conn_id = checkpoint_conn_id
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
            credential_ref=sink_cfg_raw["credential_ref"],
            source_system_name=sink_cfg_raw["source_system_name"],
            source_config=source_config,
            ingestion_date=ingestion_date,
            ingestion_time=ingestion_time,
            run_id=run_id,
            extra=sink_cfg_raw.get("extra", {}),
            file_format=sink_cfg_raw.get("file_format", "parquet"),
        )
        log.info("[InitOperator] SinkConfig built — endpoint will be injected from OpenBao")

        # ── 4. Fetch source credentials from OpenBao; inject host/port/path ─
        openbao = OpenBaoHook(openbao_conn_id=self.openbao_conn_id)
        source_credentials = openbao.get_secret(source_config.credential_ref)
        SourceConfigFactory.inject_connection(source_config, source_credentials)
        log.info(
            "[InitOperator] Source credentials fetched and connection injected — ref=%s",
            source_config.credential_ref,
        )

        # ── 5. Fetch sink credentials from OpenBao; inject endpoint ──────────
        sink_credentials = openbao.get_secret(sink_config.credential_ref)
        SinkConfigFactory.inject_connection(sink_config, sink_credentials)
        log.info(
            "[InitOperator] Sink credentials fetched and endpoint injected — ref=%s  endpoint=%s",
            sink_config.credential_ref,
            sink_config.endpoint,
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
        checkpoint_from: Optional[CheckpointValue] = self._fetch_checkpoint_from(dag_id, self.checkpoint_conn_id)
        log.info(
            "[InitOperator] checkpoint_from=%s (None = full read)",
            checkpoint_from,
        )

        # ── 8. Fetch checkpoint_to = MAX(checkpoint_column) from source ──────
        checkpoint_to: Optional[CheckpointValue] = self._fetch_checkpoint_to(
            read_type=read_type,
            source_config=source_config,
            source_credentials=source_credentials,
        )
        log.info(
            "[InitOperator] checkpoint_to=%s (None = no checkpoint_column configured)",
            checkpoint_to,
        )

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
            checkpoint_to=checkpoint_to,
            count_records=bool(cfg.get("count_records", False)),
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

    @staticmethod
    def _fetch_checkpoint_from(dag_id: str, checkpoint_conn_id: str) -> Optional[CheckpointValue]:
        """
        Fetch the last successful checkpoint value from the PostgreSQL checkpoints table.
        Keyed by dag_id. Returns None if no record exists (triggers full read).
        """
        conn_cfg = BaseHook.get_connection(checkpoint_conn_id)
        conn = psycopg2.connect(
            host=conn_cfg.host,
            port=conn_cfg.port or 5432,
            dbname=conn_cfg.schema,
            user=conn_cfg.login,
            password=conn_cfg.password,
        )
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT checkpoint_to FROM public.checkpoints WHERE dag_id = %s",
                    (dag_id,),
                )
                row = cur.fetchone()
                return _parse_checkpoint(row[0]) if row else None
        finally:
            conn.close()

    @staticmethod
    def _fetch_checkpoint_to(
        read_type: ReadAdapterType,
        source_config,
        source_credentials: dict,
    ) -> Optional[CheckpointValue]:
        """
        Query MAX(checkpoint_column) from the source using a native
        lightweight client — no Spark session required.
        Returns None when no checkpoint_column is configured (full read).

        Dispatch by source type:
          SQL / MySQL   → SELECT MAX() via psycopg2 / pymysql
          MongoDB       → $group $max aggregation via pymongo
          DynamoDB      → scan with ProjectionExpression via boto3
          Cassandra     → SELECT MAX() via cassandra-driver
          HDFS / S3     → latest file modification time via Hadoop FileSystem API
                          (returns file count as string when no checkpoint_column
                           applies to file content)
        """
        from adapters.factory.adapter_config import ReadAdapterType as RAT

        checkpoint_col = getattr(source_config, "checkpoint_column", None)
        if not checkpoint_col:
            return None

        try:
            if read_type in (RAT.SQL, RAT.MYSQL):
                return InitOperator._max_sql(read_type, source_config, source_credentials, checkpoint_col)
            elif read_type == RAT.NOSQL:
                return InitOperator._max_mongodb(source_config, source_credentials, checkpoint_col)
            elif read_type == RAT.DYNAMODB:
                return InitOperator._max_dynamodb(source_config, source_credentials, checkpoint_col)
            elif read_type == RAT.CASSANDRA:
                return InitOperator._max_cassandra(source_config, source_credentials, checkpoint_col)
            elif read_type in (RAT.FILE, RAT.S3):
                return InitOperator._max_file(read_type, source_config, source_credentials)
            else:
                log.warning("[InitOperator] No native MAX query for read_type=%s — skipping checkpoint_to", read_type)
                return None
        except Exception as exc:
            log.warning("[InitOperator] Could not fetch checkpoint_to: %s — proceeding without upper bound", exc)
            return None

    # ── Native MAX helpers (no Spark) ─────────────────────────────────────

    @staticmethod
    def _max_sql(
        read_type: ReadAdapterType,
        source_config,
        credentials: dict,
        checkpoint_col: str,
    ) -> Optional[CheckpointValue]:
        """SELECT MAX(checkpoint_col) via psycopg2 (PostgreSQL) or pymysql (MySQL)."""
        schema = source_config.schema or "public"
        table  = source_config.table
        host   = source_config.host
        port   = source_config.port
        db     = source_config.database
        user   = credentials.get("username")
        pw     = credentials.get("password")
        sql    = f"SELECT MAX({checkpoint_col}) FROM {schema}.{table}"

        from adapters.factory.adapter_config import ReadAdapterType as RAT
        if read_type == RAT.SQL:
            import psycopg2
            conn = psycopg2.connect(host=host, port=port, dbname=db, user=user, password=pw)
        else:
            import pymysql
            conn = pymysql.connect(host=host, port=port, database=db, user=user, password=pw)

        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                row = cur.fetchone()
                return _parse_checkpoint(row[0]) if row else None
        finally:
            conn.close()

    @staticmethod
    def _max_mongodb(source_config, credentials: dict, checkpoint_col: str) -> Optional[CheckpointValue]:
        """$group $max aggregation via pymongo."""
        from pymongo import MongoClient
        host     = source_config.host
        port     = source_config.port
        db_name  = source_config.database
        coll     = source_config.table
        user     = credentials.get("username")
        pw       = credentials.get("password")
        uri      = f"mongodb://{user}:{pw}@{host}:{port}/{db_name}?authSource=admin"
        client   = MongoClient(uri)
        try:
            result = list(client[db_name][coll].aggregate([
                {"$group": {"_id": None, "max_val": {"$max": f"${checkpoint_col}"}}}
            ]))
            return _parse_checkpoint(result[0]["max_val"]) if result and result[0]["max_val"] is not None else None
        finally:
            client.close()

    @staticmethod
    def _max_dynamodb(source_config, credentials: dict, checkpoint_col: str) -> Optional[CheckpointValue]:
        """Scan DynamoDB table for MAX via boto3 (approximate — use with care on large tables)."""
        import boto3
        client = boto3.client(
            "dynamodb",
            region_name=credentials.get("aws_region", "us-east-1"),
            aws_access_key_id=credentials.get("aws_access_key_id"),
            aws_secret_access_key=credentials.get("aws_secret_access_key"),
        )
        max_val: Optional[CheckpointValue] = None
        paginator = client.get_paginator("scan")
        for page in paginator.paginate(
            TableName=source_config.table,
            ProjectionExpression=checkpoint_col,
        ):
            for item in page.get("Items", []):
                attr = item.get(checkpoint_col, {})
                raw = attr.get("N") or attr.get("S")
                if raw is None:
                    continue
                candidate = _parse_checkpoint(raw)
                if max_val is None or candidate > max_val:
                    max_val = candidate
        return max_val

    @staticmethod
    def _max_cassandra(source_config, credentials: dict, checkpoint_col: str) -> Optional[CheckpointValue]:
        """SELECT MAX() via cassandra-driver."""
        from cassandra.cluster import Cluster
        from cassandra.auth import PlainTextAuthProvider
        auth    = PlainTextAuthProvider(
            username=credentials.get("username"),
            password=credentials.get("password"),
        )
        cluster = Cluster([source_config.host], port=source_config.port, auth_provider=auth)
        session = cluster.connect()
        try:
            keyspace = source_config.database
            table    = source_config.table
            row = session.execute(
                f"SELECT MAX({checkpoint_col}) FROM {keyspace}.{table}"
            ).one()
            return _parse_checkpoint(row[0]) if row and row[0] is not None else None
        finally:
            cluster.shutdown()

    @staticmethod
    def _max_file(read_type: ReadAdapterType, source_config, credentials: dict) -> Optional[CheckpointValue]:
        """
        Return the latest file modification time under the source path.
        Uses boto3 for S3 and the Hadoop FileSystem API for HDFS.
        This acts as a proxy checkpoint_to for file-based sources.
        """
        from adapters.factory.adapter_config import ReadAdapterType as RAT

        if read_type == RAT.S3:
            import boto3
            from urllib.parse import urlparse
            parsed = urlparse(source_config.path.replace("s3a://", "s3://"))
            bucket = parsed.netloc
            prefix = parsed.path.lstrip("/")
            s3 = boto3.client(
                "s3",
                region_name=credentials.get("aws_region", "us-east-1"),
                aws_access_key_id=credentials.get("aws_access_key_id"),
                aws_secret_access_key=credentials.get("aws_secret_access_key"),
                endpoint_url=credentials.get("aws_endpoint"),
            )
            latest = None
            paginator = s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    ts = obj["LastModified"]
                    if latest is None or ts > latest:
                        latest = ts
            return latest

        # HDFS — use subprocess hdfs dfs -stat to avoid needing a JVM gateway
        import subprocess
        path = source_config.path
        result = subprocess.run(
            ["hdfs", "dfs", "-stat", "%y", path],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode == 0 and result.stdout.strip():
            raw = result.stdout.strip().split("\n")[-1]
            return _parse_checkpoint(raw)
        return None
