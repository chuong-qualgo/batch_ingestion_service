from datetime import date, datetime

from adapters.factory.adapter_config import WriteAdapterType
from adapters.source.base_read_adapter import PathSourceConfig, TableSourceConfig
from adapters.write.base_write_adapter import SinkConfig


class SinkConfigFactory:
    """
    Factory that constructs a SinkConfig and provides inject_connection()
    to populate the endpoint from OpenBao credentials.

    endpoint is intentionally absent from the YAML config — it is stored
    in OpenBao alongside the sink credentials (hdfs_user, aws keys, etc.)
    and injected into SinkConfig after the secret is fetched.

    Usage (from InitOperator)
    -------------------------
    # Step 1 — build config from YAML (no endpoint yet)
    sink_config = SinkConfigFactory.create(
        credential_ref="data-platform/hadoop",
        source_system_name="postgres-prod",
        source_config=source_config,
        ingestion_date=date.today(),
        ingestion_time=datetime.now(),
        run_id=context["run_id"],
    )

    # Step 2 — fetch secret from OpenBao (includes endpoint)
    sink_credentials = openbao.get_secret("data-platform/hadoop")
    # {"endpoint": "hdfs://namenode:9000", "hdfs_user": "hadoop"}

    # Step 3 — inject endpoint from secret into config
    SinkConfigFactory.inject_connection(sink_config, sink_credentials)
    """

    @classmethod
    def create(
        cls,
        credential_ref: str,
        source_system_name: str,
        source_config: TableSourceConfig | PathSourceConfig,
        ingestion_date: date,
        ingestion_time: datetime,
        run_id: str,
        extra: dict = None,
    ) -> SinkConfig:
        """
        Build and return a SinkConfig without an endpoint.
        Call inject_connection() after fetching the OpenBao secret.
        """
        cls._validate(
            credential_ref=credential_ref,
            source_system_name=source_system_name,
            source_config=source_config,
            ingestion_date=ingestion_date,
            ingestion_time=ingestion_time,
            run_id=run_id,
        )

        return SinkConfig(
            endpoint="",                     # populated by inject_connection()
            credential_ref=credential_ref,
            source_system_name=source_system_name,
            source_config=source_config,
            ingestion_date=ingestion_date,
            ingestion_time=ingestion_time,
            run_id=run_id,
            extra=extra or {},
        )

    @staticmethod
    def inject_connection(config: SinkConfig, credentials: dict) -> None:
        """
        Inject the sink endpoint from the OpenBao secret into the config.

        Expects credentials to contain:
            endpoint : str  — root URI of the sink
                              e.g. hdfs://namenode:9000 or s3://bucket-name
        """
        if "endpoint" in credentials:
            config.endpoint = credentials["endpoint"]

    @staticmethod
    def _validate(
        credential_ref: str,
        source_system_name: str,
        source_config,
        ingestion_date: date,
        ingestion_time: datetime,
        run_id: str,
    ) -> None:
        missing = [
            name for name, value in {
                "credential_ref": credential_ref,
                "source_system_name": source_system_name,
                "run_id": run_id,
            }.items()
            if not value
        ]
        if missing:
            raise ValueError(f"Missing required SinkConfig field(s): {missing}")

        if ingestion_date is None:
            raise ValueError("SinkConfig requires a non-null ingestion_date.")
        if ingestion_time is None:
            raise ValueError("SinkConfig requires a non-null ingestion_time.")

        if not isinstance(source_config, (TableSourceConfig, PathSourceConfig)):
            raise TypeError(
                f"source_config must be TableSourceConfig or PathSourceConfig, "
                f"got: {type(source_config).__name__}"
            )
