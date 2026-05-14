from adapters.factory.adapter_config import ReadAdapterType
from adapters.source.base_read_adapter import (
    PathSourceConfig,
    SourceConfig,
    TableSourceConfig,
)


class SourceConfigFactory:
    """
    Factory that constructs the correct SourceConfig subclass based on
    the ReadAdapterType.

    SQL and NoSQL sources → TableSourceConfig
    File sources          → PathSourceConfig

    host, port (table sources) and path (file sources) are NOT passed
    here — they come from OpenBao credentials and are injected into the
    config object by InitOperator after the secret is fetched.

    Usage (from InitOperator)
    -------------------------
    # Step 1 — build config from YAML (no host/port/path yet)
    config = SourceConfigFactory.create(
        adapter_type=ReadAdapterType.SQL,
        credential_ref="data-processor/postgres",
        database="orders_db",
        schema="public",
        table="orders",
        checkpoint_column="updated_at",
    )

    # Step 2 — fetch secret from OpenBao (includes host + port)
    credentials = openbao.get_secret("data-processor/postgres")
    # {"host": "pg-host", "port": "5432", "username": "...", "password": "..."}

    # Step 3 — inject connection details from secret into config
    SourceConfigFactory.inject_connection(config, credentials)

    Raises
    ------
    ValueError
        If a required field for the resolved config type is missing.
    TypeError
        If the adapter_type does not map to a known SourceConfig subclass.
    """

    _TABLE_TYPES = {
        ReadAdapterType.SQL,
        ReadAdapterType.MYSQL,
        ReadAdapterType.NOSQL,
        ReadAdapterType.DYNAMODB,
        ReadAdapterType.CASSANDRA,
    }

    _PATH_TYPES = {
        ReadAdapterType.FILE,
        ReadAdapterType.S3,
    }

    @classmethod
    def create(
        cls,
        adapter_type: ReadAdapterType,
        credential_ref: str,
        **kwargs,
    ) -> SourceConfig:
        """
        Build and return the appropriate SourceConfig subclass.
        Connection details (host/port/path) are expected to be injected
        later via inject_connection().
        """
        if adapter_type in cls._TABLE_TYPES:
            return cls._build_table_config(credential_ref, **kwargs)
        elif adapter_type in cls._PATH_TYPES:
            return cls._build_path_config(credential_ref, **kwargs)
        else:
            raise TypeError(
                f"No SourceConfig mapping for adapter type: '{adapter_type}'. "
                f"Table types: {[t.value for t in cls._TABLE_TYPES]}, "
                f"Path types: {[t.value for t in cls._PATH_TYPES]}"
            )

    @staticmethod
    def inject_connection(
        config: SourceConfig,
        credentials: dict,
    ) -> None:
        """
        Inject connection details fetched from OpenBao into the config object.

        For TableSourceConfig — expects credentials to contain:
            host : str   — database hostname
            port : int   — database port

        For PathSourceConfig — expects credentials to contain:
            path : str   — file or directory URI
                           e.g. s3://bucket/prefix/ or hdfs://nn:9000/data/

        Any key not present in credentials is silently skipped (keeps
        any existing value set in the config).
        """
        if isinstance(config, TableSourceConfig):
            if "host" in credentials:
                config.host = credentials["host"]
            if "port" in credentials:
                config.port = int(credentials["port"])
            if "jars" in credentials:
                config.extra["jars"] = credentials["jars"]

        elif isinstance(config, PathSourceConfig):
            if "path" in credentials:
                config.path = credentials["path"]

    @classmethod
    def _build_table_config(
        cls,
        credential_ref: str,
        database: str = "",
        schema: str = "default",
        table: str = "",
        query: str = None,
        checkpoint_column: str = None,
        extra: dict = None,
        read_options: dict = None,
        **_ignored,
    ) -> TableSourceConfig:
        cls._require(database=database, table=table)
        return TableSourceConfig(
            credential_ref=credential_ref,
            database=database,
            schema=schema,
            table=table,
            query=query,
            checkpoint_column=checkpoint_column,
            extra=extra or {},
            read_options=read_options or {},
        )

    @classmethod
    def _build_path_config(
        cls,
        credential_ref: str,
        file_format: PathSourceConfig.FileFormat = PathSourceConfig.FileFormat.PARQUET,
        checkpoint_column: str = None,
        extra: dict = None,
        read_options: dict = None,
        **_ignored,
    ) -> PathSourceConfig:
        # path is intentionally not required here — comes from OpenBao
        return PathSourceConfig(
            credential_ref=credential_ref,
            file_format=file_format,
            checkpoint_column=checkpoint_column,
            extra=extra or {},
            read_options=read_options or {},
        )

    @staticmethod
    def _require(**fields) -> None:
        missing = [name for name, value in fields.items() if not value]
        if missing:
            raise ValueError(
                f"Missing required SourceConfig field(s): {missing}"
            )
