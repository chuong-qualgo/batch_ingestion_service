from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Optional

import psycopg2
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.utils.context import Context

from adapters.factory.adapter_config import AdapterConfig, MetricAdapterType
from adapters.factory.metric_adapter_factory import MetricAdapterFactory
from adapters.metric.base_metric_adapter import (
    BaseMetricAdapter,
    MetricConfig,
    KafkaMetricConfig,
    RedisMetricConfig,
    SQSMetricConfig,
)
from adapters.metric.redis_queue_adapter import RedisQueueAdapter
from orchestration.plugins.openbao_hook import OpenBaoHook

log = logging.getLogger(__name__)


def _build_metric_adapter(
    metric_type: MetricAdapterType,
    metric_config_raw: dict,
    openbao_conn_id: str,
) -> BaseMetricAdapter:
    """
    Shared helper — builds the metric adapter from raw config dict.
    Fetches credentials from OpenBao using the credential_ref in config.
    host/port (Redis) and queue_url/aws_region (SQS) come from credentials.
    """
    hook = OpenBaoHook(openbao_conn_id=openbao_conn_id)
    credential_ref = metric_config_raw.get("credential_ref", "")
    credentials = hook.get_secret(credential_ref) if credential_ref else {}

    if metric_type == MetricAdapterType.CLOUD_QUEUE:
        metric_config = SQSMetricConfig(
            credential_ref=credential_ref,
            queue_url=credentials.get("queue_url", metric_config_raw.get("queue_url", "")),
            aws_region=credentials.get("aws_region", metric_config_raw.get("aws_region", "")),
            extra=metric_config_raw.get("extra", {}),
        )
    elif metric_type == MetricAdapterType.ONPREM_QUEUE:
        metric_config = RedisMetricConfig(
            credential_ref=credential_ref,
            host=credentials.get("host", metric_config_raw.get("host", "")),
            port=int(credentials.get("port", metric_config_raw.get("port", 6379))),
            stream_name=metric_config_raw.get("stream_name", ""),
            max_len=metric_config_raw.get("max_len", 1000),
            extra=metric_config_raw.get("extra", {}),
        )
    elif metric_type == MetricAdapterType.KAFKA_QUEUE:
        metric_config = KafkaMetricConfig(
            credential_ref=credential_ref,
            bootstrap_servers=credentials.get(
                "bootstrap_servers", metric_config_raw.get("bootstrap_servers", "")
            ),
            topic=metric_config_raw.get("topic", ""),
            key=metric_config_raw.get("key"),
        )
    else:
        raise ValueError(f"Unsupported metric_type: {metric_type}")

    adapter_config = AdapterConfig(metric_type=metric_type)
    return MetricAdapterFactory.create(
        config=adapter_config,
        metric_config=metric_config,
        credentials=credentials,
        message_attributes=metric_config_raw.get("message_attributes", {}),
    )


def _push_redis_metric(
    payload: dict,
    redis_metric_config_raw: dict,
    redis_metric_credentials: dict,
) -> None:
    """
    Secondary Redis Streams push — publishes the pipeline event alongside the
    MongoDB checkpoint save. Fire-and-forget: logs errors but does not re-raise.

    Credentials (host, port, password) come from redis_metric_credentials,
    which InitOperator fetches from OpenBao under redis_metric.credential_ref.
    """
    try:
        config = RedisMetricConfig(
            credential_ref=redis_metric_config_raw.get("credential_ref", ""),
            host=redis_metric_credentials.get("host", redis_metric_config_raw.get("host", "")),
            port=int(redis_metric_credentials.get("port", redis_metric_config_raw.get("port", 6379))),
            stream_name=redis_metric_config_raw.get("stream_name", ""),
            max_len=redis_metric_config_raw.get("max_len", 1000),
        )
        adapter = RedisQueueAdapter(
            config=config,
            credentials=redis_metric_credentials,
            message_attributes=redis_metric_config_raw.get("message_attributes", {}),
        )
        adapter.validate_connection()
        adapter.publish(payload)
        log.info("[_push_redis_metric] Event published to Redis stream '%s'", config.stream_name)
    except Exception as exc:
        log.error("[_push_redis_metric] Failed to publish to Redis (non-fatal): %s", exc)


def push_metric_inline(
    payload: dict,
    metric_type: MetricAdapterType,
    metric_config_raw: dict,
    openbao_conn_id: str = OpenBaoHook.default_conn_name,
) -> None:
    """
    Inline metric push — call directly inside SparkRunOperator for
    failure callbacks. Fire-and-forget: logs errors but does not re-raise.

    Usage inside SparkRunOperator.execute() on failure:
        push_metric_inline(
            payload={"status": "failed", "dag_id": dag_id, "error": str(exc)},
            metric_type=run_ctx.metric_type,
            metric_config_raw=run_ctx.metric_config_raw,
        )
    """
    try:
        adapter = _build_metric_adapter(metric_type, metric_config_raw, openbao_conn_id)
        adapter.validate_connection()
        adapter.publish(payload)
        log.info("[push_metric_inline] Metric published — payload=%s", payload)
    except Exception as exc:
        log.error(
            "[push_metric_inline] Failed to publish metric (non-fatal): %s", exc
        )


class MetricPushOperator(BaseOperator):
    """
    Standalone Airflow operator — publishes a pipeline completion metric
    and saves checkpoint_to to MongoDB.

    Execution steps
    ---------------
    1. Pull RunContext from XCom (written by InitOperator).
    2. Publish metric message to the configured queue (Redis or SQS).
    3. Upsert checkpoint_to into MongoDB keyed by dag_id.
       Step 3 is skipped if checkpoint_to is None (no checkpoint_column
       configured) or if mongo_conn_id is not set.

    MongoDB document structure (collection: checkpoints)
    ----------------------------------------------------
    {
        "dag_id":          "<dag_id>",          # lookup key
        "checkpoint_from": "<checkpoint_to>",   # becomes next run's checkpoint_from
        "updated_at":      "<ISO timestamp>"
    }

    Parameters
    ----------
    init_task_id : str
        Task id of the upstream InitOperator to pull XCom from.
    metric_type : MetricAdapterType
        Queue backend (CLOUD_QUEUE for SQS, ONPREM_QUEUE for Redis).
    metric_config_raw : dict
        Raw queue config from YAML.
    status : str
        Pipeline status written into the metric payload. Defaults to 'success'.
    extra_payload : dict, optional
        Additional fields merged into the published metric payload.
    mongo_conn_id : str, optional
        Airflow connection id for the MongoDB checkpoint store.
        Matches the mongo_conn_id used by InitOperator to read checkpoints.
        When None, checkpoint save is skipped.
    xcom_key : str
        XCom key to pull RunContext from. Defaults to 'run_context'.
    spark_task_id : str, optional
        Task id of the upstream SparkRunOperator. When set, pulls
        'record_count' from that task's XCom and adds it as an int
        to the metric payload.
    openbao_conn_id : str
        Airflow connection id for OpenBao.
    """

    template_fields = ("extra_payload",)

    def __init__(
        self,
        init_task_id: str,
        metric_type: MetricAdapterType,
        metric_config_raw: dict,
        status: str = "success",
        extra_payload: Optional[dict] = None,
        checkpoint_conn_id: Optional[str] = None,
        xcom_key: str = "run_context",
        spark_task_id: Optional[str] = None,
        openbao_conn_id: str = OpenBaoHook.default_conn_name,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.init_task_id        = init_task_id
        self.metric_type         = metric_type
        self.metric_config_raw   = metric_config_raw
        self.status              = status
        self.extra_payload       = extra_payload or {}
        self.checkpoint_conn_id  = checkpoint_conn_id
        self.xcom_key            = xcom_key
        self.spark_task_id       = spark_task_id
        self.openbao_conn_id     = openbao_conn_id

    def execute(self, context: Context) -> None:
        dag_id: str = context["dag"].dag_id
        stop_time: str = datetime.now(tz=timezone.utc).isoformat()

        # ── 1. Pull RunContext from XCom ──────────────────────────────────
        raw_context: dict = context["ti"].xcom_pull(
            task_ids=self.init_task_id,
            key=self.xcom_key,
        )

        checkpoint_from: Optional[str] = None
        checkpoint_to: Optional[str] = None

        read_type: Optional[str] = None
        ingestion_date: Optional[str] = None
        redis_metric_config_raw: Optional[dict] = None
        redis_metric_credentials: Optional[dict] = None

        if raw_context:
            raw_ckpt_from = raw_context.get("checkpoint_from")
            if isinstance(raw_ckpt_from, dict):
                checkpoint_from = str(raw_ckpt_from.get("v", "")) or None

            raw_ckpt_to = raw_context.get("checkpoint_to")
            if isinstance(raw_ckpt_to, dict):
                checkpoint_to = str(raw_ckpt_to.get("v", "")) or None

            read_type = raw_context.get("read_type")
            ingestion_date = raw_context.get("ingestion_date")
            redis_metric_config_raw = raw_context.get("redis_metric_config_raw")
            redis_metric_credentials = raw_context.get("redis_metric_credentials")

        record_count: Optional[int] = None
        if self.spark_task_id:
            raw_count = context["ti"].xcom_pull(
                task_ids=self.spark_task_id,
                key="record_count",
            )
            if raw_count is not None:
                record_count = int(raw_count)

        payload = {
            "dag_id":          dag_id,
            "run_id":          context["run_id"],
            "status":          self.status,
            "start_time":      context["dag_run"].start_date.isoformat(),
            "stop_time":       stop_time,
            "checkpoint_from": checkpoint_from,
            "checkpoint_to":   checkpoint_to,
        }

        if read_type is not None:
            payload["read_type"] = read_type
        if ingestion_date is not None:
            payload["ingestion_date"] = ingestion_date
        if record_count is not None:
            payload["record_count"] = record_count

        payload.update(self.extra_payload)

        # ── 2. Publish metric ─────────────────────────────────────────────
        log.info("[MetricPushOperator] Publishing metric — payload=%s", payload)
        adapter = _build_metric_adapter(
            metric_type=self.metric_type,
            metric_config_raw=self.metric_config_raw,
            openbao_conn_id=self.openbao_conn_id,
        )
        adapter.validate_connection()
        adapter.publish(payload)
        log.info("[MetricPushOperator] Metric published successfully")

        # ── 2b. Secondary Redis push (optional) ───────────────────────────
        if redis_metric_config_raw and redis_metric_credentials:
            _push_redis_metric(payload, redis_metric_config_raw, redis_metric_credentials)

        # ── 3. Upsert checkpoint_to to PostgreSQL ─────────────────────────
        if checkpoint_to and self.checkpoint_conn_id:
            self._save_checkpoint(dag_id, checkpoint_to)
            log.info(
                "[MetricPushOperator] Checkpoint saved — "
                "dag_id=%s checkpoint_to=%s",
                dag_id, checkpoint_to,
            )
        else:
            log.info(
                "[MetricPushOperator] Checkpoint save skipped — "
                "checkpoint_to=%s checkpoint_conn_id=%s",
                checkpoint_to, self.checkpoint_conn_id,
            )

    # ── Helpers ───────────────────────────────────────────────────────────

    def _save_checkpoint(self, dag_id: str, checkpoint_to: str) -> None:
        """Upsert the checkpoint row into PostgreSQL."""
        conn_cfg = BaseHook.get_connection(self.checkpoint_conn_id)
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
                    """
                    INSERT INTO public.checkpoints (dag_id, checkpoint_to, updated_at)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (dag_id) DO UPDATE
                        SET checkpoint_to = EXCLUDED.checkpoint_to,
                            updated_at    = NOW()
                    """,
                    (dag_id, checkpoint_to),
                )
            conn.commit()
        finally:
            conn.close()
