FROM apache/airflow:2.9.1-python3.11

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN ARCH=$(dpkg --print-architecture) \
    && ln -sfn /usr/lib/jvm/java-17-openjdk-${ARCH} /usr/lib/jvm/java-17-openjdk

# ── Copy project source code into the image ───────────────────────────────────
# Build context is the project root (file_landing_service/),
# so all adapters and orchestration code is available here.
COPY adapters/        /opt/airflow/app/adapters/
COPY orchestration/   /opt/airflow/app/orchestration/

# Copy demo pipeline config
COPY demo/airflow/config/ /opt/airflow/app/config/

# Copy entrypoint script
COPY demo/airflow/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

USER airflow

# ── Install Python dependencies ───────────────────────────────────────────────
RUN pip install --no-cache-dir \
    pyspark==3.5.1 \
    pymongo==4.7.0 \
    hvac==2.1.0 \
    PyYAML==6.0.1 \
    redis==5.0.4 \
    boto3==1.34.0

# ── Download PostgreSQL JDBC driver ───────────────────────────────────────────
RUN mkdir -p /home/airflow/jars && \
    curl -fsSL \
    https://jdbc.postgresql.org/download/postgresql-42.7.3.jar \
    -o /home/airflow/jars/postgresql-42.7.3.jar

USER root

COPY demo/airflow/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

USER airflow

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk
ENV SPARK_HOME=/home/airflow/.local/lib/python3.11/site-packages/pyspark
ENV PYSPARK_SUBMIT_ARGS="--jars /home/airflow/jars/postgresql-42.7.3.jar pyspark-shell"
ENV PYTHONPATH=/opt/airflow/app

ENTRYPOINT ["/entrypoint.sh"]
CMD ["scheduler"]

ENTRYPOINT ["/entrypoint.sh"]
