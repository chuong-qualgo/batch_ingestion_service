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

    Usage
    -----
    # Table-based source (SQL / NoSQL)
    config = SourceConfigFactory.create(
        adapter_type=ReadAdapterType.SQL,
        credential_ref="secret/data-processor/postgres",
        host="localhost",
        port=5432,
        database="orders_db",
        schema="public",
        table="orders",
    )

    # Path-based source (File / Cloud Storage)
    config = SourceConfigFactory.create(
        adapter_type=ReadAdapterType.FILE,
        credential_ref="secret/data-processor/s3",
        path="s3://raw-bucket/exports/transactions/",
        file_format=PathSourceConfig.FileFormat.PARQUET,
    )

    Raises
    ------
    ValueError
        If a required field for the resolved config type is missing.
    TypeError
        If the adapter_type does not map to a known SourceConfig subclass.
    """

    # Adapter types that produce a TableSourceConfig
    _TABLE_TYPES = {
        ReadAdapterType.SQL,
        ReadAdapterType.MYSQL,
        ReadAdapterType.NOSQL,
        ReadAdapterType.DYNAMODB,
        ReadAdapterType.CASSANDRA,
    }

    # Adapter types that produce a PathSourceConfig
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
        Create and return the appropriate SourceConfig subclass.

        Parameters
        ----------
        adapter_type : ReadAdapterType
            Determines which SourceConfig subclass to build.
        credential_ref : str
            OpenBao key for fetching source credentials.
        **kwargs
            Fields forwarded to the resolved config dataclass.
            TableSourceConfig: host, port, database, schema, table, query (optional)
            PathSourceConfig:  path, file_format (optional, defaults to PARQUET)

        Returns
        -------
        TableSourceConfig | PathSourceConfig
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

    @classmethod
    def _build_table_config(
        cls,
        credential_ref: str,
        host: str = "",
        port: int = 0,
        database: str = "",
        schema: str = "",
        table: str = "",
        query: str = None,
        checkpoint_column: str = None,
        extra: dict = None,
        **_ignored,
    ) -> TableSourceConfig:
        cls._require(host=host, port=port, database=database, table=table)
        return TableSourceConfig(
            credential_ref=credential_ref,
            host=host,
            port=port,
            database=database,
            schema=schema,
            table=table,
            query=query,
            checkpoint_column=checkpoint_column,
            extra=extra or {},
        )

    @classmethod
    def _build_path_config(
        cls,
        credential_ref: str,
        path: str = "",
        file_format: PathSourceConfig.FileFormat = PathSourceConfig.FileFormat.PARQUET,
        checkpoint_column: str = None,
        extra: dict = None,
        **_ignored,
    ) -> PathSourceConfig:
        cls._require(path=path)
        return PathSourceConfig(
            credential_ref=credential_ref,
            path=path,
            file_format=file_format,
            checkpoint_column=checkpoint_column,
            extra=extra or {},
        )

    @staticmethod
    def _require(**fields) -> None:
        """Raise ValueError for any blank or zero-value required field."""
        missing = [name for name, value in fields.items() if not value]
        if missing:
            raise ValueError(
                f"Missing required SourceConfig field(s): {missing}"
            )
