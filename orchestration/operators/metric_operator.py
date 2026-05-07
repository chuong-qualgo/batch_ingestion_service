from __future__ import annotations

import logging
from typing import Any, Optional

from airflow.models import BaseOperator
from airflow.utils.context import Context

from adapters.factory.adapter_config import AdapterConfig, MetricAdapterType
from adapters.factory.metric_adapter_factory import MetricAdapterFactory
from adapters.metric.base_metric_adapter import (
    BaseMetricAdapter,
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
    Used by both the inline helper and the standalone operator.
    Fetches credentials from OpenBao using the credential_ref in config.
    """
    hook = OpenBaoHook(openbao_conn_id=openbao_conn_id)
    credential_ref = metric_config_raw.get("credential_ref", "")
    credentials = hook.get_secret(credential_ref) if credential_ref else {}

    if metric_type == MetricAdapterType.CLOUD_QUEUE:
        metric_config = SQSMetricConfig(
            credential_ref=credential_ref,
            queue_url=metric_config_raw["queue_url"],
            aws_region=metric_config_raw.get("aws_region", ""),
            extra=metric_config_raw.get("extra", {}),
        )
    elif metric_type == MetricAdapterType.ONPREM_QUEUE:
        metric_config = RedisMetricConfig(
            credential_ref=credential_ref,
            host=metric_config_raw["host"],
            port=metric_config_raw.get("port", 6379),
            stream_name=metric_config_raw["stream_name"],
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
    failure callbacks or mid-pipeline events. Fire-and-forget: logs
    errors but does not re-raise so the calling operator is not affected.

    Usage inside SparkRunOperator.execute():
        push_metric_inline(
            payload={"status": "failed", "dag_id": dag_id, "error": str(exc)},
            metric_type=run_ctx.metric_type,
            metric_config_raw=run_ctx.metric_config_raw,
        )

    Parameters
    ----------
    payload : dict
        Metric data to publish (status, dag_id, run_id, error message, etc.)
    metric_type : MetricAdapterType
        Which queue backend to use.
    metric_config_raw : dict
        Raw config dict from YAML (queue_url / host / stream_name etc.)
    openbao_conn_id : str
        Airflow connection id for OpenBao.
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
    Standalone Airflow operator — publishes a metric message to the
    configured queue after the pipeline completes.

    Reads the RunContext from XCom (written by InitOperator) to auto-populate
    dag_id, run_id, and checkpoint_to into the payload. Additional fields
    can be passed via `extra_payload`.

    Add this as the final task in the DAG for pipeline completion confirmation:

        init >> spark_run >> metric_push

    Parameters
    ----------
    init_task_id : str
        Task id of the upstream InitOperator to pull XCom from.
    metric_type : MetricAdapterType
        Which queue backend to use (CLOUD_QUEUE for SQS, ONPREM_QUEUE for Redis).
    metric_config_raw : dict
        Raw queue config dict. Keys depend on metric_type:
        SQS:   queue_url, aws_region, credential_ref
        Redis: host, port, stream_name, max_len, credential_ref
    status : str
        Pipeline status to include in the payload. Defaults to 'success'.
    extra_payload : dict, optional
        Additional fields merged into the published payload.
    xcom_key : str
        XCom key to pull RunContext from. Defaults to 'run_context'.
    openbao_conn_id : str
        Airflow connection id for OpenBao. Defaults to 'openbao_default'.
    """

    template_fields = ("extra_payload",)

    def __init__(
        self,
        init_task_id: str,
        metric_type: MetricAdapterType,
        metric_config_raw: dict,
        status: str = "success",
        extra_payload: Optional[dict] = None,
        xcom_key: str = "run_context",
        openbao_conn_id: str = OpenBaoHook.default_conn_name,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.init_task_id = init_task_id
        self.metric_type = metric_type
        self.metric_config_raw = metric_config_raw
        self.status = status
        self.extra_payload = extra_payload or {}
        self.xcom_key = xcom_key
        self.openbao_conn_id = openbao_conn_id

    def execute(self, context: Context) -> None:
        # Pull RunContext from XCom to enrich the payload
        raw_context: dict = context["ti"].xcom_pull(
            task_ids=self.init_task_id,
            key=self.xcom_key,
        )

        payload = {
            "status": self.status,
            "dag_id": context["dag"].dag_id,
            "run_id": context["run_id"],
            "ingestion_date": context["data_interval_start"].date().isoformat(),
        }

        # Enrich with checkpoint_to from RunContext if available
        if raw_context:
            payload["checkpoint_to"] = raw_context.get("checkpoint_from")
            payload["read_type"] = raw_context.get("read_type")
            payload["write_type"] = raw_context.get("write_type")

        payload.update(self.extra_payload)

        log.info("[MetricPushOperator] Publishing metric — payload=%s", payload)

        adapter = _build_metric_adapter(
            metric_type=self.metric_type,
            metric_config_raw=self.metric_config_raw,
            openbao_conn_id=self.openbao_conn_id,
        )
        adapter.validate_connection()
        adapter.publish(payload)

        log.info("[MetricPushOperator] Metric published successfully")
