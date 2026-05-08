from __future__ import annotations

import logging
from typing import Any, Optional

import boto3
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.utils.context import Context

from adapters.factory.adapter_config import AdapterConfig, MetricAdapterType
from adapters.factory.metric_adapter_factory import MetricAdapterFactory
from adapters.metric.base_metric_adapter import (
    BaseMetricAdapter,
    MetricConfig,
    RedisMetricConfig,
    SQSMetricConfig,
)
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
    else:
        raise ValueError(f"Unsupported metric_type: {metric_type}")

    adapter_config = AdapterConfig(metric_type=metric_type)
    return MetricAdapterFactory.create(
        config=adapter_config,
        metric_config=metric_config,
        credentials=credentials,
        message_attributes=metric_config_raw.get("message_attributes", {}),
    )


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
    and saves checkpoint_to to DynamoDB.

    Execution steps
    ---------------
    1. Pull RunContext from XCom (written by InitOperator).
    2. Publish metric message to the configured queue (Redis or SQS).
    3. Upsert checkpoint_to into DynamoDB keyed by dag_id.
       Step 3 is skipped if checkpoint_to is None (no checkpoint_column
       configured) or if dynamodb_conn_id / dynamodb_table are not set.

    DynamoDB document structure
    ---------------------------
    {
        "dag_id":          "<dag_id>",          # partition key
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
    dynamodb_table : str, optional
        DynamoDB table name for checkpoint storage.
        When None, checkpoint save is skipped.
    dynamodb_conn_id : str, optional
        Airflow connection id that supplies AWS region and credentials
        for DynamoDB. Uses keys: aws_access_key_id, aws_secret_access_key,
        region_name (from conn.extra_dejson or conn.host as region fallback).
        When None, checkpoint save is skipped.
    xcom_key : str
        XCom key to pull RunContext from. Defaults to 'run_context'.
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
        dynamodb_table: Optional[str] = None,
        dynamodb_conn_id: Optional[str] = None,
        xcom_key: str = "run_context",
        openbao_conn_id: str = OpenBaoHook.default_conn_name,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.init_task_id     = init_task_id
        self.metric_type      = metric_type
        self.metric_config_raw = metric_config_raw
        self.status           = status
        self.extra_payload    = extra_payload or {}
        self.dynamodb_table   = dynamodb_table
        self.dynamodb_conn_id = dynamodb_conn_id
        self.xcom_key         = xcom_key
        self.openbao_conn_id  = openbao_conn_id

    def execute(self, context: Context) -> None:
        dag_id: str = context["dag"].dag_id

        # ── 1. Pull RunContext from XCom ──────────────────────────────────
        raw_context: dict = context["ti"].xcom_pull(
            task_ids=self.init_task_id,
            key=self.xcom_key,
        )

        checkpoint_to: Optional[str] = None  # serialised as string for DynamoDB / metric payload
        payload = {
            "status":         self.status,
            "dag_id":         dag_id,
            "run_id":         context["run_id"],
            "ingestion_date": context["data_interval_start"].date().isoformat(),
        }

        if raw_context:
            raw_ckpt = raw_context.get("checkpoint_to")
            # checkpoint_to is stored as {"t": "int"|"ts", "v": ...} — extract the value
            if isinstance(raw_ckpt, dict):
                checkpoint_to = str(raw_ckpt.get("v", "")) or None
            checkpoint_to = checkpoint_to or None
            payload["checkpoint_to"] = checkpoint_to
            payload["read_type"]     = raw_context.get("read_type")
            payload["write_type"]    = raw_context.get("write_type")

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

        # ── 3. Upsert checkpoint_to to DynamoDB ───────────────────────────
        if checkpoint_to and self.dynamodb_table and self.dynamodb_conn_id:
            self._save_checkpoint_dynamo(dag_id, checkpoint_to)
            log.info(
                "[MetricPushOperator] Checkpoint saved to DynamoDB — "
                "dag_id=%s checkpoint_to=%s table=%s",
                dag_id, checkpoint_to, self.dynamodb_table,
            )
        else:
            log.info(
                "[MetricPushOperator] Checkpoint save skipped — "
                "checkpoint_to=%s dynamodb_table=%s dynamodb_conn_id=%s",
                checkpoint_to, self.dynamodb_table, self.dynamodb_conn_id,
            )

    # ── Helpers ───────────────────────────────────────────────────────────

    def _save_checkpoint_dynamo(self, dag_id: str, checkpoint_to: str) -> None:
        """
        Upsert the checkpoint document into DynamoDB.
        The previous checkpoint_to becomes the next run's checkpoint_from.

        DynamoDB item:
            dag_id         (S) — partition key
            checkpoint_from (S) — value to use as checkpoint_from next run
            updated_at     (S) — ISO timestamp of this write
        """
        from datetime import datetime, timezone

        conn = BaseHook.get_connection(self.dynamodb_conn_id)
        extra = conn.extra_dejson if conn.extra_dejson else {}

        client = boto3.client(
            "dynamodb",
            region_name=extra.get("region_name") or conn.host or "us-east-1",
            aws_access_key_id=extra.get("aws_access_key_id") or conn.login,
            aws_secret_access_key=extra.get("aws_secret_access_key") or conn.password,
        )

        client.put_item(
            TableName=self.dynamodb_table,
            Item={
                "dag_id":          {"S": dag_id},
                "checkpoint_from": {"S": checkpoint_to},
                "updated_at":      {"S": datetime.now(tz=timezone.utc).isoformat()},
            },
        )
