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

    echo "[Airflow Init] Registering MongoDB connection..."
    airflow connections add mongo_checkpoint \
      --conn-type generic \
      --conn-host mongo \
      --conn-port 27017 \
      --conn-login mongo_user \
      --conn-password mongo_pass \
      --conn-schema config || true

    echo "[Airflow Init] Initialization complete"
    touch /tmp/airflow-init.done
else
    echo "[Airflow Init] Already initialized, skipping setup"
fi

# Execute airflow command passed as arguments
exec airflow "$@"
