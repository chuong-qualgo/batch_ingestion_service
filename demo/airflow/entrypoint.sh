#!/bin/bash
set -e

# Only run initialization if this is the first start
if [ ! -f /tmp/airflow-init.done ]; then
    echo "[Airflow Init] Running database migration..."
    airflow db migrate

    echo "[Airflow Init] Creating admin user..."
    airflow users create \
      --username admin \
      --password admin \
      --firstname Admin \
      --lastname User \
      --role Admin \
      --email admin@demo.local || true

    echo "[Airflow Init] Registering OpenBao connection..."
    airflow connections add openbao_default \
      --conn-type http \
      --conn-host http://openbao \
      --conn-port 8200 \
      --conn-schema demo || true

    echo "[Airflow Init] Registering PostgreSQL checkpoint connection..."
    airflow connections add postgres_checkpoint \
      --conn-type postgres \
      --conn-host postgres \
      --conn-port 5432 \
      --conn-login orders_user \
      --conn-password orders_pass \
      --conn-schema orders_db || true

    echo "[Airflow Init] Initialization complete"
    touch /tmp/airflow-init.done
else
    echo "[Airflow Init] Already initialized, skipping setup"
fi

# Execute airflow command passed as arguments
exec airflow "$@"
