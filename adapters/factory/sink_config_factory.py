from datetime import date, datetime

from adapters.factory.adapter_config import WriteAdapterType
from adapters.source.base_read_adapter import PathSourceConfig, TableSourceConfig
from adapters.write.base_write_adapter import SinkConfig


class SinkConfigFactory:
    """
    Factory that constructs a SinkConfig, binding the source config,
    runtime partitioning fields, and sink connection details together.

    The resolved write path is determined by combining:
      - The sink endpoint and source_system_name
      - The source config type (table vs path) for the base path segments
      - The ingestion_date, ingestion_time, and run_id for partitioning

    Usage
    -----
    # Using a table source config
    sink_cfg = SinkConfigFactory.create(
        endpoint="hdfs://namenode:9000",
        credential_ref="secret/data-platform/hadoop",
        source_system_name="postgres-prod",
        source_config=table_source_config,
        ingestion_date=date.today(),
        ingestion_time=datetime.now(),
        run_id="scheduled__2024-01-15T14:30:00",
    )

    # Using a path source config
    sink_cfg = SinkConfigFactory.create(
        endpoint="s3://my-bucket",
        credential_ref="secret/data-platform/s3",
        source_system_name="sftp-partner",
        source_config=path_source_config,
        ingestion_date=date.today(),
        ingestion_time=datetime.now(),
        run_id="manual__2024-01-15T14:30:00",
    )

    Raises
    ------
    ValueError
        If any required field is missing or blank.
    TypeError
        If source_config is not a TableSourceConfig or PathSourceConfig.
    """

    @classmethod
    def create(
        cls,
        endpoint: str,
        credential_ref: str,
        source_system_name: str,
        source_config: TableSourceConfig | PathSourceConfig,
        ingestion_date: date,
        ingestion_time: datetime,
        run_id: str,
        extra: dict = None,
    ) -> SinkConfig:
        """
        Build and return a fully populated SinkConfig.

        Parameters
        ----------
        endpoint : str
            Root URI of the sink (e.g. hdfs://namenode:9000, s3://bucket).
        credential_ref : str
            OpenBao key for fetching sink credentials.
        source_system_name : str
            Free-form label for the origin system defined by the user.
        source_config : TableSourceConfig | PathSourceConfig
            Source config from the reader — drives the base path segments.
        ingestion_date : date
            Date partition for this run (typically Airflow execution_date).
        ingestion_time : datetime
            Time partition for this run (typically Airflow execution_time).
        run_id : str
            Unique run identifier (typically Airflow run_id).
        extra : dict, optional
            Additional sink-specific parameters.

        Returns
        -------
        SinkConfig
        """
        cls._validate(
            endpoint=endpoint,
            credential_ref=credential_ref,
            source_system_name=source_system_name,
            source_config=source_config,
            ingestion_date=ingestion_date,
            ingestion_time=ingestion_time,
            run_id=run_id,
        )

        return SinkConfig(
            endpoint=endpoint,
            credential_ref=credential_ref,
            source_system_name=source_system_name,
            source_config=source_config,
            ingestion_date=ingestion_date,
            ingestion_time=ingestion_time,
            run_id=run_id,
            extra=extra or {},
        )

    @staticmethod
    def _validate(
        endpoint: str,
        credential_ref: str,
        source_system_name: str,
        source_config: TableSourceConfig | PathSourceConfig,
        ingestion_date: date,
        ingestion_time: datetime,
        run_id: str,
    ) -> None:
        """Validate all required fields before constructing SinkConfig."""

        # Check string fields are non-empty
        missing = [
            name for name, value in {
                "endpoint": endpoint,
                "credential_ref": credential_ref,
                "source_system_name": source_system_name,
                "run_id": run_id,
            }.items()
            if not value
        ]
        if missing:
            raise ValueError(
                f"Missing required SinkConfig field(s): {missing}"
            )

        # Check runtime fields are provided
        if ingestion_date is None:
            raise ValueError("SinkConfig requires a non-null ingestion_date.")
        if ingestion_time is None:
            raise ValueError("SinkConfig requires a non-null ingestion_time.")

        # Check source config is a known subclass
        if not isinstance(source_config, (TableSourceConfig, PathSourceConfig)):
            raise TypeError(
                f"source_config must be TableSourceConfig or PathSourceConfig, "
                f"got: {type(source_config).__name__}"
            )
