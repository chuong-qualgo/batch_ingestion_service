from __future__ import annotations

import json
import logging
import random
import uuid
from datetime import datetime, timezone
from typing import Any, List, Optional

from airflow.utils.context import Context

from adapters.factory.adapter_config import ReadAdapterType
from orchestration.operators.init_operator import InitOperator
from orchestration.plugins.openbao_hook import OpenBaoHook

log = logging.getLogger(__name__)

_PRODUCTS = ["laptop", "headphones", "keyboard", "monitor", "mouse", "webcam", "docking-station"]
_STATUSES = ["pending", "confirmed", "shipped", "delivered", "cancelled"]


def _generate_order(ts: datetime) -> dict:
    return {
        "order_id": str(uuid.uuid4()),
        "customer_id": f"C{random.randint(1000, 9999)}",
        "product": random.choice(_PRODUCTS),
        "quantity": random.randint(1, 5),
        "price": round(random.uniform(9.99, 999.99), 2),
        "status": random.choice(_STATUSES),
        "created_at": ts.isoformat(),
    }


class DemoInitOperator(InitOperator):
    """
    InitOperator variant for demo pipelines.

    Before running the normal init steps, produces ``num_records`` sample
    order records (or a caller-supplied record list) into the configured
    Kafka topic.  The seeding step is skipped silently for non-Kafka sources
    so the same operator class can be used across pipeline types without
    having to branch in the DAG.

    Parameters
    ----------
    num_records : int
        Number of auto-generated order records to push. Ignored when
        ``demo_records`` is supplied explicitly.  Defaults to 100.
    demo_records : list[dict], optional
        If provided, these exact records are pushed verbatim instead of the
        auto-generated set.  Useful for deterministic integration tests.
    """

    def __init__(
        self,
        num_records: int = 100,
        demo_records: Optional[List[dict]] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.num_records = num_records
        self.demo_records = demo_records

    # ── Main execute ──────────────────────────────────────────────────────

    def execute(self, context: Context) -> dict:
        cfg = self._load_config(self.config_path)
        read_type = ReadAdapterType(cfg["read_type"])

        if read_type == ReadAdapterType.KAFKA:
            self._seed_kafka(cfg)
        else:
            log.info(
                "[DemoInitOperator] read_type=%s is not Kafka — skipping demo seed",
                read_type.value,
            )

        return super().execute(context)

    # ── Kafka seeding ─────────────────────────────────────────────────────

    def _seed_kafka(self, cfg: dict) -> None:
        from kafka import KafkaProducer

        source_cfg = cfg["source"]
        topic = source_cfg["topic"]
        credential_ref = source_cfg["credential_ref"]

        openbao = OpenBaoHook(openbao_conn_id=self.openbao_conn_id)
        credentials = openbao.get_secret(credential_ref)

        bootstrap_servers = credentials.get("bootstrap_servers", "").split(",")

        producer_kwargs: dict = {
            "bootstrap_servers": bootstrap_servers,
            "value_serializer": lambda v: json.dumps(v).encode("utf-8"),
            "acks": "all",
        }

        sasl_username = credentials.get("sasl_username")
        sasl_password = credentials.get("sasl_password")
        if sasl_username and sasl_password:
            producer_kwargs.update({
                "security_protocol": "SASL_PLAINTEXT",
                "sasl_mechanism": "PLAIN",
                "sasl_plain_username": sasl_username,
                "sasl_plain_password": sasl_password,
            })

        records = self.demo_records or [
            _generate_order(datetime.now(timezone.utc)) for _ in range(self.num_records)
        ]

        producer = KafkaProducer(**producer_kwargs)
        try:
            for record in records:
                producer.send(topic, value=record)
            producer.flush()
            log.info(
                "[DemoInitOperator] Seeded %d records into Kafka topic '%s'",
                len(records),
                topic,
            )
        finally:
            producer.close()
