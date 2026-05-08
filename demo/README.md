# File Landing Service — Demo

End-to-end local demo of the pipeline:

```
PostgreSQL (orders table)
    → Apache Spark (JDBC read → Parquet write)
        → Hadoop HDFS (landing zone)
            → Redis Streams (metric event)
```

Orchestrated by **Apache Airflow** with secrets from **OpenBao**.

---

## Prerequisites

- Docker Desktop ≥ 4.x with Compose v2 (`docker compose`)
- At least **6 GB RAM** allocated to Docker
- Ports free: `5432`, `6379`, `7077`, `8080`, `8090`, `8200`, `9000`, `9864`, `9870`, `27017`

---

## Quick start

```bash
# 1. From the project root (file_landing_service/)
cd file_landing_service/

# 2. Start all services
docker compose up -d

# 3. Wait ~2 minutes for all health checks to pass
docker compose ps

# 4. Open Airflow UI
open http://localhost:8080
# Login: admin / admin

# 5. Trigger the demo DAG
#    Go to DAGs → demo_postgres_to_hadoop → ▶ Trigger DAG

# 6. Watch the run
#    Click the DAG run → Graph view → click each task to see logs
```

---

## Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | admin / admin |
| Hadoop NameNode | http://localhost:9870 | — |
| Spark Master | http://localhost:8090 | — |
| OpenBao | http://localhost:8200 | root token in `openbao-init` logs |
| PostgreSQL | localhost:5432 | orders_user / orders_pass / orders_db |
| MongoDB | localhost:27017 | mongo_user / mongo_pass |
| Redis | localhost:6379 | password: redis_pass |

---

## What the DAG does

```
init task
  ├── reads pipeline_config.yaml
  ├── fetches credentials from OpenBao
  │     secret/data-processor/postgres → orders_user / orders_pass
  │     secret/data-platform/hadoop   → hdfs_user: hadoop
  │     secret/data-platform/redis    → password: redis_pass
  ├── checks MongoDB for existing checkpoint (None on first run → full read)
  └── pushes RunContext to XCom

spark_run task
  ├── pulls RunContext from XCom
  ├── reads orders table from PostgreSQL via Spark JDBC
  │     SELECT * FROM public.orders
  │     (WHERE updated_at > <checkpoint> on subsequent runs)
  ├── writes Parquet files to HDFS:
  │     hdfs://namenode:9000/postgres-demo/orders_db/public/orders/
  │       ingestion_date=<date>/ingestion_time=<time>/run_id=<run_id>/
  └── saves max(updated_at) as checkpoint_to to MongoDB

metric_push task
  └── publishes {"status": "success", "dag_id": ..., "run_id": ...}
        to Redis Stream: pipeline-metrics
```

---

## Verifying the output

### Check HDFS output

```bash
# List the landing path
docker exec fls-namenode hdfs dfs -ls -R /postgres-demo/

# Show file sizes
docker exec fls-namenode hdfs dfs -du -h /postgres-demo/

# Read a Parquet file (requires parquet-tools or Spark shell)
docker exec fls-spark-master /opt/spark/bin/spark-shell \
  --master local \
  -e "spark.read.parquet(\"hdfs://namenode:9000/postgres-demo/\").show(5)"
```

### Check MongoDB checkpoint

```bash
docker exec fls-mongo mongosh \
  --username mongo_user --password mongo_pass \
  --eval 'db.getSiblingDB("config").checkpoints.find().pretty()'
```

### Check Redis metric stream

```bash
docker exec fls-redis redis-cli -a redis_pass \
  XRANGE pipeline-metrics - +
```

### Check PostgreSQL source

```bash
docker exec fls-postgres psql -U orders_user -d orders_db \
  -c "SELECT COUNT(*), MIN(updated_at), MAX(updated_at) FROM public.orders;"
```

---

## Testing incremental ingestion

The pipeline uses `updated_at` as the checkpoint column. After the first run,
only rows newer than the last checkpoint are read.

```bash
# Insert new rows into PostgreSQL
docker exec fls-postgres psql -U orders_user -d orders_db -c "
  INSERT INTO public.orders (customer_id, product_code, quantity, unit_price, status, region)
  VALUES (9999, 'SKU-NEW1', 1, 49.99, 'pending', 'APAC'),
         (9998, 'SKU-NEW2', 2, 99.00, 'confirmed', 'EMEA');
"

# Trigger the DAG again — only the 2 new rows will be read
# (Airflow UI → demo_postgres_to_hadoop → ▶ Trigger DAG)
```

---

## Stopping the demo

```bash
# Run from file_landing_service/ — not from demo/

# Stop all services (keeps volumes)
docker compose down

# Stop and delete all data volumes
docker compose down -v
```

---

## Troubleshooting

**NameNode stays in safe mode after startup**
```bash
docker exec fls-namenode hdfs dfsadmin -safemode leave
```

**Airflow scheduler can't import project modules**
Make sure the project root is mounted at `/opt/airflow/app` (already set in `docker-compose.yml`) and `PYTHONPATH=/opt/airflow/app` is set on the scheduler service.

**OpenBao token error in Airflow**
The `openbao-init` container prints the root token to its logs. Use it to update the Airflow connection:
```bash
# Get the root token
docker logs fls-openbao-init | grep "Root token"

# Update via Airflow CLI
docker exec fls-airflow-scheduler airflow connections delete openbao_default
docker exec fls-airflow-scheduler airflow connections add openbao_default \
  --conn-type http \
  --conn-host http://openbao \
  --conn-port 8200 \
  --conn-schema demo \
  --conn-password <root-token>
```

**Spark can't reach HDFS**
Verify the NameNode is healthy and the `core-site.xml` is mounted correctly:
```bash
docker compose ps namenode
docker exec fls-spark-master cat /opt/hadoop/conf/core-site.xml
```

**Port conflict**
Change the host port in `docker-compose.yml` for the conflicting service. Only the host port (left side of `:`) needs to change.
