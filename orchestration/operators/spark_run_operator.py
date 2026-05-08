from __future__ import annotations

import logging
from typing import Any, Optional

from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.utils.context import Context

from adapters.factory.adapter_config import AdapterConfig, MetricAdapterType
from orchestration.operators.metric_operator import push_metric_inline
from orchestration.operators.run_context import RunContext
from orchestration.plugins.openbao_hook import OpenBaoHook

log = logging.getLogger(__name__)


class SparkRunOperator(BaseOperator):
    """
    Airflow operator that submits and runs the Spark ingestion job.

    Reads RunContext from XCom (written by InitOperator), instantiates
    the correct source and sink adapters, and runs the full read → write
    pipeline. checkpoint_from and checkpoint_to are both resolved by
    InitOperator — Spark only uses them to bound the read query.
    Checkpoint persistence is handled by MetricPushOperator.

    On failure, pushes an inline failure metric via push_metric_inline()
    if metric_type and metric_config_raw are provided.

    Execution steps
    ---------------
    1. Pull RunContext from XCom and deserialise it.
    2. Build SparkSession.
    3. Instantiate source adapter via ReadAdapterFactory.
    4. Instantiate sink adapter via WriteAdapterFactory.
    5. Validate source connection.
    6. Validate sink connection.
    7. Apply source filters.
    8. Read data from source → DataFrame (bounded by checkpoint_from / checkpoint_to).
    9. Write DataFrame to sink.

    On any exception: push inline failure metric (if configured), then re-raise.

    Parameters
    ----------
    init_task_id : str
        Task id of the upstream InitOperator to pull XCom from.
    xcom_key : str
        XCom key to pull RunContext from. Defaults to 'run_context'.
    mongo_conn_id : str
        Airflow connection id for MongoDB checkpoint store (kept for
        future use but checkpoint writes now handled by MetricPushOperator).
    spark_app_name : str, optional
        SparkSession app name. Defaults to dag_id at runtime.
    spark_config : dict, optional
        Extra Spark config key-value pairs.
    metric_type : MetricAdapterType, optional
        Queue backend for inline failure metric push.
    metric_config_raw : dict, optional
        Raw queue config (queue_url / host / stream_name etc.).
    openbao_conn_id : str
        Airflow connection id for OpenBao.
    """

    template_fields = ("init_task_id",)

    def __init__(
        self,
        init_task_id: str,
        xcom_key: str = "run_context",
        mongo_conn_id: str = "mongo_checkpoint",
        spark_app_name: Optional[str] = None,
        spark_config: Optional[dict] = None,
        metric_type: Optional[MetricAdapterType] = None,
        metric_config_raw: Optional[dict] = None,
        openbao_conn_id: str = OpenBaoHook.default_conn_name,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.init_task_id = init_task_id
        self.xcom_key = xcom_key
        self.mongo_conn_id = mongo_conn_id
        self.spark_app_name = spark_app_name
        self.spark_config = spark_config or {}
        self.metric_type = metric_type
        self.metric_config_raw = metric_config_raw or {}
        self.openbao_conn_id = openbao_conn_id

    # ── Main execute ──────────────────────────────────────────────────────

    def execute(self, context: Context) -> None:
        dag_id: str = context["dag"].dag_id
        run_id: str = context["run_id"]
        spark = None

        try:
            # ── 1. Pull RunContext from XCom ──────────────────────────────
            raw_context: dict = context["ti"].xcom_pull(
                task_ids=self.init_task_id,
                key=self.xcom_key,
            )
            if not raw_context:
                raise ValueError(
                    f"No XCom value for task_id='{self.init_task_id}' "
                    f"key='{self.xcom_key}'"
                )
            run_ctx = RunContext.from_dict(raw_context)
            log.info(
                "[SparkRunOperator] RunContext loaded — dag=%s run_id=%s "
                "read=%s write=%s checkpoint_from=%s",
                dag_id, run_id,
                run_ctx.read_type.value, run_ctx.write_type.value,
                run_ctx.checkpoint_from,
            )

            # ── 2. Build SparkSession ─────────────────────────────────────
            spark = self._build_spark_session(
                app_name=self.spark_app_name or dag_id,
                extra_config=self.spark_config,
            )
            log.info("[SparkRunOperator] SparkSession ready — app=%s", spark.sparkContext.appName)

            # ── 3. Instantiate source adapter ─────────────────────────────
            from adapters.factory.read_adapter_factory import ReadAdapterFactory
            from adapters.factory.write_adapter_factory import WriteAdapterFactory

            adapter_config = AdapterConfig(
                read_type=run_ctx.read_type,
                write_type=run_ctx.write_type,
                metric_type=run_ctx.metric_type,
            )
            adapter_config.source_config = run_ctx.source_config

            source_adapter = ReadAdapterFactory.create(
                config=adapter_config, spark=spark,
            )
            source_adapter.credentials = run_ctx.source_credentials
            source_adapter.checkpoint_from = run_ctx.checkpoint_from
            source_adapter.checkpoint_to   = run_ctx.checkpoint_to
            log.info("[SparkRunOperator] Source adapter — %s", type(source_adapter).__name__)

            # ── 4. Instantiate sink adapter ───────────────────────────────
            sink_adapter = WriteAdapterFactory.create(
                config=adapter_config,
                spark=spark,
                sink_config=run_ctx.sink_config,
                credentials=run_ctx.sink_credentials,
            )
            log.info("[SparkRunOperator] Sink adapter — %s", type(sink_adapter).__name__)

            # ── 5. Validate source connection ─────────────────────────────
            source_adapter.validate_connection()
            log.info("[SparkRunOperator] Source connection OK")

            # ── 6. Validate sink connection ───────────────────────────────
            sink_adapter.validate_connection()
            log.info("[SparkRunOperator] Sink connection OK")

            # ── 7. Apply source filters ───────────────────────────────────
            source_adapter.apply_filters()

            # ── 8. Read from source ───────────────────────────────────────
            log.info("[SparkRunOperator] Reading from source...")
            df = source_adapter.read()
            log.info(
                "[SparkRunOperator] Read complete — checkpoint_from=%s checkpoint_to=%s",
                run_ctx.checkpoint_from,
                run_ctx.checkpoint_to,
            )

            # ── 9. Write to sink ──────────────────────────────────────────
            write_path = sink_adapter.build_write_path()
            log.info("[SparkRunOperator] Writing to sink — path=%s", write_path)
            sink_adapter.write(df)
            log.info("[SparkRunOperator] Write complete")

            # checkpoint_to persistence is handled by MetricPushOperator
            log.info("[SparkRunOperator] Pipeline complete — checkpoint will be saved by MetricPushOperator")

        except Exception as exc:
            log.error("[SparkRunOperator] Pipeline failed: %s", exc)
            # Inline failure metric — fire-and-forget, will not mask original error
            self._push_failure_metric(dag_id=dag_id, run_id=run_id, exc=exc)
            raise

        # finally:
        #     if spark:
        #         try:
        #             spark.stop()
        #             log.info("[SparkRunOperator] SparkSession stopped")
        #         except Exception:
        #             pass

    # ── Helpers ───────────────────────────────────────────────────────────

    @staticmethod
    def _build_spark_session(app_name: str, extra_config: dict):
        from pyspark.sql import SparkSession
        builder = SparkSession.builder.appName(app_name)
        defaults = {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        }
        for key, value in {**defaults, **extra_config}.items():
            builder = builder.config(key, value)
        return builder.getOrCreate()


    def _push_failure_metric(self, dag_id: str, run_id: str, exc: Exception) -> None:
        """Push inline failure metric if metric config is present. Non-fatal."""
        if not self.metric_type or not self.metric_config_raw:
            return
        push_metric_inline(
            payload={
                "status": "failed",
                "dag_id": dag_id,
                "run_id": run_id,
                "error": str(exc),
            },
            metric_type=self.metric_type,
            metric_config_raw=self.metric_config_raw,
            openbao_conn_id=self.openbao_conn_id,
        )
