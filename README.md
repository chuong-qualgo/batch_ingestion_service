# File Landing Service

A batch ingestion data framework built on **Apache Spark** and **Apache Airflow**. It reads data from various sources, lands it to a configured sink, and publishes a metric event on completion. All secrets are managed by **OpenBao** (open-source Vault fork).

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Project Structure](#project-structure)
- [How It Works](#how-it-works)
- [Supported Sources and Sinks](#supported-sources-and-sinks)
- [Pipeline Configuration](#pipeline-configuration)
- [Output Path Structure](#output-path-structure)
- [Checkpointing](#checkpointing)
- [Secret Management](#secret-management)
- [Adding a New Pipeline](#adding-a-new-pipeline)
- [Extending the Framework](#extending-the-framework)
- [Running Tests](#running-tests)
- [Dependencies](#dependencies)

---

## Architecture Overview

```
Airflow DAG
  │
  ├── InitOperator          → loads YAML config, fetches credentials from OpenBao,
  │                           reads checkpoint from MongoDB, pushes RunContext to XCom
  │
  ├── SparkRunOperator      → reads RunContext from XCom, runs Spark read → write,
  │                           saves checkpoint to MongoDB
  │                           (on failure: publishes inline metric via push_metric_inline)
  │
  └── MetricPushOperator    → publishes pipeline completion metric to Redis or SQS
```

Data flows from **source → Spark engine → sink** on every DAG run. Incremental ingestion is driven by a **checkpoint column** — on the first run the full table is read; subsequent runs read only rows newer than the last saved checkpoint.

---

## Project Structure

```
file_landing_service/
│
├── adapters/
│   ├── source/                         # PySpark source adapters
│   │   ├── base_read_adapter.py        # Abstract base + SourceConfig hierarchy
│   │   ├── sql_adapter.py              # Mid-tier: shared JDBC read logic
│   │   ├── nosql_adapter.py            # Mid-tier: shared NoSQL connector logic
│   │   ├── file_adapter.py             # Mid-tier: shared Hadoop FileSystem read logic
│   │   ├── sql/
│   │   │   ├── source_postgres_adapter.py
│   │   │   └── source_mysql_adapter.py
│   │   ├── nosql/
│   │   │   ├── source_mongodb_adapter.py
│   │   │   ├── source_dynamodb_adapter.py
│   │   │   └── source_cassandra_adapter.py
│   │   └── file/
│   │       ├── source_hadoop_adapter.py
│   │       └── source_s3_adapter.py
│   │
│   ├── write/                          # PySpark sink adapters
│   │   ├── base_write_adapter.py       # Abstract base + SinkConfig + build_write_path()
│   │   ├── hadoop_adapter.py           # Mid-tier: shared write/validate/schema logic
│   │   ├── cloud_storage_adapter.py    # Mid-tier: passthrough for cloud object stores
│   │   └── sink/
│   │       ├── sink_hadoop_adapter.py
│   │       └── sink_s3_adapter.py
│   │
│   ├── metric/                         # Plain Python metric publishers (no Spark)
│   │   ├── base_metric_adapter.py      # Abstract base + MetricConfig hierarchy
│   │   ├── sqs_queue_adapter.py        # AWS SQS publisher
│   │   └── redis_queue_adapter.py      # Redis Streams publisher
│   │
│   └── factory/                        # Factories — config and adapter creation
│       ├── adapter_config.py           # Enums: ReadAdapterType, WriteAdapterType, MetricAdapterType
│       ├── source_config_factory.py    # Builds TableSourceConfig or PathSourceConfig
│       ├── sink_config_factory.py      # Builds SinkConfig with full validation
│       ├── read_adapter_factory.py     # Registry of all 7 source adapters
│       ├── write_adapter_factory.py    # Registry of 2 sink adapters
│       ├── metric_adapter_factory.py   # Registry of 2 metric adapters
│       └── adapter_factory.py          # Top-level facade
│
├── orchestration/
│   ├── operators/
│   │   ├── init_operator.py            # Step 1: load config, fetch creds, build RunContext
│   │   ├── spark_run_operator.py       # Step 2: run Spark pipeline
│   │   ├── metric_operator.py          # Step 3: publish metric + inline failure helper
│   │   └── run_context.py              # XCom envelope passed between operators
│   ├── plugins/
│   │   └── openbao_hook.py             # Airflow hook → OpenBao via Kubernetes auth
│   └── template_dags/
│       └── file_landing_dag.py         # Template DAG: init >> spark_run >> metric_push
│
├── config/                             # Global settings (placeholder)
│
├── tests/
│   ├── conftest.py                     # Shared pytest fixtures
│   ├── unit/
│   │   ├── adapters/source/            # SQL, NoSQL, File adapter unit tests
│   │   ├── adapters/write/             # Hadoop, S3 sink adapter unit tests
│   │   ├── adapters/metric/            # SQS (moto), Redis (fakeredis) unit tests
│   │   └── orchestration/              # InitOperator + RunContext unit tests
│   └── integration/
│       └── test_full_pipeline.py       # End-to-end operator wiring tests
│
├── requirements-test.txt
├── pytest.ini
└── README.md
```

---

## How It Works

### DAG execution flow

```
init >> spark_run >> metric_push
              ↘ (on any exception)
         push_metric_inline   ← non-fatal, will not mask the original error
```

### InitOperator — 9 steps

| Step | Action |
|------|--------|
| 1 | Load and parse the YAML pipeline config file |
| 2 | Build `SourceConfig` from the `source:` section via `SourceConfigFactory` |
| 3 | Build `SinkConfig` from the `sink:` section, binding `ingestion_date`, `ingestion_time`, `run_id` from the Airflow context |
| 4 | Fetch **source** credentials from OpenBao |
| 5 | Fetch **sink** credentials from OpenBao |
| 6 | Fetch **metric** credentials from OpenBao |
| 7 | Fetch `checkpoint_from` from MongoDB keyed by `dag_id` — `None` triggers a full read |
| 8 | Log source record/file count for observability |
| 9 | Assemble `RunContext` and push to XCom |

### SparkRunOperator — 10 steps

| Step | Action |
|------|--------|
| 1 | Pull `RunContext` from XCom |
| 2 | Build `SparkSession` with adaptive query execution enabled |
| 3 | Instantiate source adapter via `ReadAdapterFactory` |
| 4 | Instantiate sink adapter via `WriteAdapterFactory` |
| 5 | `validate_connection()` on source |
| 6 | `validate_connection()` on sink |
| 7 | `apply_filters()` — pushdown predicates to source |
| 8 | `read()` → `DataFrame` (respects `checkpoint_from`) |
| 9 | `write(df)` → `pre_write → write → post_write` |
| 10 | Upsert `checkpoint_to` to MongoDB |

On any exception: `push_metric_inline()` fires with `status=failed` (non-fatal), then re-raises.

### MetricPushOperator

Pulls `RunContext` from XCom, enriches the payload with pipeline metadata, then publishes to the configured queue.

---

## Supported Sources and Sinks

### Source adapters (`read_type`)

| `read_type`  | Concrete class              | Protocol                          | Config type        |
|--------------|-----------------------------|-----------------------------------|--------------------|
| `sql`        | `SourcePostgresAdapter`     | JDBC — PostgreSQL driver          | `TableSourceConfig` |
| `mysql`      | `SourceMySQLAdapter`        | JDBC — MySQL Connector/J          | `TableSourceConfig` |
| `nosql`      | `SourceMongoDBAdapter`      | Spark MongoDB connector           | `TableSourceConfig` |
| `dynamodb`   | `SourceDynamoDBAdapter`     | Spark DynamoDB connector + boto3  | `TableSourceConfig` |
| `cassandra`  | `SourceCassandraAdapter`    | Spark Cassandra connector         | `TableSourceConfig` |
| `file`       | `SourceHadoopAdapter`       | HDFS via Spark native reader      | `PathSourceConfig`  |
| `s3`         | `SourceS3Adapter`           | S3A via Spark (hadoop-aws)        | `PathSourceConfig`  |

### Sink adapters (`write_type`)

| `write_type` | Concrete class         | Protocol                    |
|--------------|------------------------|-----------------------------|
| `hadoop`     | `SinkHadoopAdapter`    | HDFS via Spark native writer |
| `s3`         | `SinkS3Adapter`        | S3A via Spark (hadoop-aws)  |

### Metric adapters (`metric_type`)

| `metric_type`   | Concrete class        | Protocol                    |
|-----------------|-----------------------|-----------------------------|
| `cloud_queue`   | `SQSQueueAdapter`     | AWS SQS standard queue      |
| `onprem_queue`  | `RedisQueueAdapter`   | Redis Streams (`XADD`)      |

---

## Pipeline Configuration

Each pipeline is defined by a YAML config file. The `InitOperator` loads this file at runtime.

### SQL / NoSQL source → HDFS sink → Redis metric

```yaml
read_type: sql
write_type: hadoop
metric_type: onprem_queue

source:
  credential_ref: data-processor/postgres   # OpenBao secret path
  host: pg-host.internal
  port: 5432
  database: orders_db
  schema: public
  table: orders
  checkpoint_column: updated_at             # omit for full read every run

sink:
  credential_ref: data-platform/hadoop
  endpoint: hdfs://namenode:9000
  source_system_name: postgres-prod         # appears in the output path

metric:
  credential_ref: data-platform/redis
  host: redis.infra.svc.cluster.local
  port: 6379
  stream_name: pipeline-metrics
  max_len: 5000                             # trim stream; omit for unbounded
```

### S3 source → S3 sink → SQS metric

```yaml
read_type: s3
write_type: s3
metric_type: cloud_queue

source:
  credential_ref: data-processor/s3
  path: s3://raw-bucket/exports/transactions/
  file_format: parquet                      # parquet | csv | json | avro | orc | delta | text
  checkpoint_column: event_time             # optional

sink:
  credential_ref: data-platform/s3
  endpoint: s3://landing-bucket
  source_system_name: sftp-partner

metric:
  credential_ref: data-platform/sqs
  queue_url: https://sqs.ap-southeast-1.amazonaws.com/123456789/pipeline-metrics
  aws_region: ap-southeast-1
```

### MySQL source with a custom query (no checkpoint)

```yaml
read_type: mysql
write_type: hadoop
metric_type: onprem_queue

source:
  credential_ref: data-processor/mysql
  host: mysql-host.internal
  port: 3306
  database: crm
  schema: dbo
  table: customers
  query: "SELECT id, name, email FROM dbo.customers WHERE active = 1"
  # checkpoint_column omitted → full read every run

sink:
  credential_ref: data-platform/hadoop
  endpoint: hdfs://namenode:9000
  source_system_name: mysql-crm

metric:
  credential_ref: data-platform/redis
  host: redis.infra.svc.cluster.local
  port: 6379
  stream_name: pipeline-metrics
```

---

## Output Path Structure

The sink path is constructed automatically by `SinkConfig.build_write_path()`:

**SQL / NoSQL source:**
```
{endpoint}/{source_system_name}/{database}/{schema}/{table}/
ingestion_date={YYYY-MM-DD}/ingestion_time={HH-MM-SS}/run_id={airflow_run_id}/
```

**File / Cloud Storage source:**
```
{endpoint}/{source_system_name}/{source_path}/
ingestion_date={YYYY-MM-DD}/ingestion_time={HH-MM-SS}/run_id={airflow_run_id}/
```

**Examples:**

```
# PostgreSQL orders table → HDFS
hdfs://namenode:9000/postgres-prod/orders_db/public/orders/
  ingestion_date=2024-01-15/
  ingestion_time=14-30-00/
  run_id=scheduled__2024-01-15T14:30:00/

# S3 exports folder → S3 landing bucket
s3a://landing-bucket/sftp-partner/exports/transactions/
  ingestion_date=2024-01-15/
  ingestion_time=14-30-00/
  run_id=scheduled__2024-01-15T14:30:00/
```

---

## Checkpointing

Checkpointing enables **incremental ingestion** — only rows newer than the last successful run are read.

### Checkpoint lifecycle

```
Run 1:  checkpoint_from = None              → full table read
        checkpoint_to   = "2024-01-15 14:30:00"  → saved to MongoDB

Run 2:  checkpoint_from = "2024-01-15 14:30:00"  → incremental read
        checkpoint_to   = "2024-01-16 09:00:00"  → saved to MongoDB

Run 3:  checkpoint_from = "2024-01-16 09:00:00"  → incremental read
        ...
```

### MongoDB checkpoint document

```json
{
  "dag_id": "file_landing_pipeline",
  "checkpoint_from": "2024-01-15 14:30:00",
  "updated_at": "2024-01-15T14:31:05.123Z"
}
```

One document per `dag_id`, upserted after every successful run.

### Disabling checkpoints

Simply omit `checkpoint_column` from the YAML source section. The full table or path is read every run and nothing is written to MongoDB.

### How filters are applied per source type

| Source     | Filter mechanism                                                        |
|------------|-------------------------------------------------------------------------|
| SQL/MySQL  | `WHERE {checkpoint_column} > '{checkpoint_from}'` appended to JDBC query |
| MongoDB    | `$match` stage in aggregation pipeline                                  |
| DynamoDB   | Post-load `DataFrame.filter()` after Spark read                         |
| Cassandra  | CQL `WHERE` clause pushed down via connector                            |
| HDFS / S3  | Post-load `DataFrame.filter()` on the checkpoint column                 |

---

## Secret Management

All credentials are stored in **OpenBao** and fetched at runtime by Airflow via the `OpenBaoHook`. No passwords or keys appear in config files, environment variables, or DAG code.

### OpenBao one-time setup

```bash
# 1. Initialise and unseal all 3 nodes
kubectl exec -n infra openbao-0 -- bao operator init \
  -key-shares=5 -key-threshold=3 -format=json > init.json   # keep this file safe

for pod in openbao-0 openbao-1 openbao-2; do
  for key in <unseal-key-1> <unseal-key-2> <unseal-key-3>; do
    kubectl exec -n infra $pod -- bao operator unseal $key
  done
done

# 2. Login and enable KV v2
export OPENBAO_ADDR=http://localhost:8200
bao login <root-token>
bao secrets enable -path=secret kv-v2

# 3. Enable Kubernetes auth and create Airflow policy
bao auth enable kubernetes
bao write auth/kubernetes/config kubernetes_host=https://$KUBERNETES_SERVICE_HOST

bao policy write airflow-policy - <<POL
path "secret/data/data-processor/*" { capabilities = ["read"] }
path "secret/data/data-platform/*"  { capabilities = ["read"] }
POL

bao write auth/kubernetes/role/airflow \
  bound_service_account_names=processor-sa \
  bound_service_account_namespaces=data-processor \
  policies=airflow-policy \
  ttl=1h

# 4. Write secrets
bao kv put secret/data-processor/postgres \
  username=myuser password=mypassword

bao kv put secret/data-platform/hadoop \
  hdfs_user=hadoop

bao kv put secret/data-platform/redis \
  password=redispassword

bao kv put secret/data-processor/s3 \
  aws_access_key_id=AKID \
  aws_secret_access_key=SECRET \
  aws_region=ap-southeast-1
```

### Required Airflow connection (OpenBao)

Create an Airflow connection with ID `openbao_default`:

| Field     | Value                                         |
|-----------|-----------------------------------------------|
| Conn Type | HTTP                                          |
| Host      | `http://openbao.infra.svc.cluster.local`      |
| Port      | `8200`                                        |
| Schema    | `airflow` (the Kubernetes auth role name)     |

### Required Airflow connection (MongoDB checkpoint store)

Create an Airflow connection with ID `mongo_checkpoint`:

| Field    | Value                          |
|----------|--------------------------------|
| Conn Type | MongoDB                       |
| Host     | MongoDB hostname               |
| Port     | `27017`                        |
| Login    | MongoDB username               |
| Password | MongoDB password               |
| Schema   | Database name (e.g. `config`)  |

---

## Adding a New Pipeline

1. Copy `orchestration/template_dags/file_landing_dag.py` to a new file, e.g. `mysql_crm_dag.py`
2. Change `dag_id` and `schedule_interval`
3. Create a YAML config file under `config/` (e.g. `mysql_crm.yaml`)
4. Write the secret to OpenBao for the new source
5. Deploy the DAG to your Airflow instance

No code changes are needed for any source/sink combination already listed in [Supported Sources and Sinks](#supported-sources-and-sinks).

---

## Extending the Framework

### Adding a new source adapter

1. Choose the right parent class:
   - SQL database → inherit from `SQLAdapter`
   - NoSQL store → inherit from `NoSQLAdapter`
   - File system → inherit from `FileAdapter`

2. Create the file under `adapters/source/{sql|nosql|file}/`

3. Implement the required methods:

   | Parent class   | Required methods                                      |
   |----------------|-------------------------------------------------------|
   | `SQLAdapter`   | `driver` (property), `_build_jdbc_url()`              |
   | `NoSQLAdapter` | `spark_format` (property), `_build_connection_options()` |
   | `FileAdapter`  | `_configure_spark_for_filesystem()`                   |

4. Add a new value to `ReadAdapterType` in `adapter_config.py`

5. Register the class in `ReadAdapterFactory._registry`

6. Add the new type to `_TABLE_TYPES` or `_PATH_TYPES` in `SourceConfigFactory`

7. Write unit tests in `tests/unit/adapters/source/`

**Example — adding a Google BigQuery adapter:**

```python
# adapters/source/sql/source_bigquery_adapter.py
from adapters.source.sql_adapter import SQLAdapter
from adapters.source.base_read_adapter import TableSourceConfig

class SourceBigQueryAdapter(SQLAdapter):
    """Reads from BigQuery via Spark BigQuery connector."""

    @property
    def driver(self) -> str:
        return "com.simba.googlebigquery.jdbc.Driver"

    def _build_jdbc_url(self) -> str:
        cfg = self.source_config
        return f"jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;ProjectId={cfg.database}"
```

```python
# adapter_config.py — add:
class ReadAdapterType(str, Enum):
    ...
    BIGQUERY = "bigquery"

# read_adapter_factory.py — add:
from adapters.source.sql.source_bigquery_adapter import SourceBigQueryAdapter
_registry = {
    ...
    ReadAdapterType.BIGQUERY: SourceBigQueryAdapter,
}

# source_config_factory.py — add BIGQUERY to _TABLE_TYPES
```

### Adding a new sink adapter

1. Inherit from `HadoopAdapter` (HDFS-like) or `CloudStorageAdapter` (object store)
2. Implement `_configure_spark_for_filesystem()`, `post_write()`, `on_error()`
3. Add a value to `WriteAdapterType` and register in `WriteAdapterFactory._registry`

### Adding a new metric adapter

1. Add a config dataclass inheriting from `MetricConfig` in `base_metric_adapter.py`
2. Create a class inheriting from `BaseMetricAdapter`
3. Implement `publish()`, `validate_connection()`, `build_message()`, `on_error()`
4. Add a value to `MetricAdapterType`, register in `MetricAdapterFactory._registry`
5. Handle the new config type in `metric_operator._build_metric_adapter()`

---

## Running Tests

```bash
# Install test dependencies
pip install -r requirements-test.txt

# Run the full suite (133 tests)
pytest

# Unit tests only
pytest tests/unit/ -v

# Integration tests only
pytest tests/integration/ -v

# A specific adapter family
pytest tests/unit/adapters/source/ -v
pytest tests/unit/adapters/metric/ -v

# With coverage
pip install pytest-cov
pytest --cov=adapters --cov=orchestration --cov-report=term-missing
```

### Test breakdown

| Suite       | File                              | Tests | Key mocks                    |
|-------------|-----------------------------------|-------|------------------------------|
| Unit        | `test_sql_adapter.py`             | 12    | MagicMock (Spark JDBC)       |
| Unit        | `test_nosql_adapter.py`           | 17    | MagicMock, boto3 patch       |
| Unit        | `test_file_adapter.py`            | 10    | MagicMock (Hadoop FS API)    |
| Unit        | `test_factory.py`                 | 25    | MagicMock                    |
| Unit        | `test_hadoop_adapter.py`          | 12    | MagicMock (Spark writer)     |
| Unit        | `test_cloud_storage_adapter.py`   | 8     | MagicMock, moto (S3)         |
| Unit        | `test_sqs_queue_adapter.py`       | 8     | moto `mock_aws` (real SQS)   |
| Unit        | `test_redis_queue_adapter.py`     | 11    | fakeredis (real XADD)        |
| Unit        | `test_init_operator.py`           | 14    | MagicMock (Airflow, OpenBao) |
| Integration | `test_full_pipeline.py`           | 21    | All of the above             |
| **Total**   |                                   | **133** |                            |

All external I/O (Spark, MongoDB, OpenBao, SQS, Redis) is mocked at the boundary — no real infrastructure is needed to run the test suite.

---

## Dependencies

### Runtime

| Package            | Purpose                                     |
|--------------------|---------------------------------------------|
| `apache-airflow`   | DAG orchestration and operator base classes |
| `pyspark`          | Spark engine for read and write             |
| `hvac`             | OpenBao / Vault Python client               |
| `pymongo`          | MongoDB checkpoint read/write               |
| `boto3`            | AWS SQS, S3, and DynamoDB                   |
| `redis`            | Redis Streams metric publisher              |
| `cassandra-driver` | Cassandra connection validation             |
| `PyYAML`           | Pipeline config file parsing                |

### Test only

| Package       | Purpose                                        |
|---------------|------------------------------------------------|
| `pytest`      | Test runner                                    |
| `pytest-mock` | MagicMock integration                          |
| `moto`        | AWS service mocks — SQS, S3, DynamoDB          |
| `fakeredis`   | In-memory Redis mock (full XADD/XRANGE support)|
| `mongomock`   | In-memory MongoDB mock                         |
