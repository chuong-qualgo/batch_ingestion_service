from adapters.write.hadoop_adapter import HadoopAdapter


class CloudStorageAdapter(HadoopAdapter):
    """
    Mid-tier adapter for cloud object storage sinks (S3, GCS, ADLS).
    Inherits all write logic from HadoopAdapter.
    Subclasses provide cloud-provider-specific credential configuration
    via _configure_spark_for_filesystem().
    """
    pass
