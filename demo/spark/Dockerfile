FROM apache/spark:3.5.1-python3

USER root

# Install Python packages needed by the framework
RUN pip install --no-cache-dir \
    pymongo==4.7.0 \
    hvac==2.1.0 \
    PyYAML==6.0.1 \
    redis==5.0.4 \
    boto3==1.34.0 \
    apache-airflow-client==2.9.0

# Download PostgreSQL JDBC driver
RUN curl -fsSL \
    https://jdbc.postgresql.org/download/postgresql-42.7.3.jar \
    -o /opt/spark/jars/postgresql-42.7.3.jar

# Hadoop client JARs for HDFS access (already bundled in Spark, but ensure version match)
ENV HADOOP_CONF_DIR=/opt/hadoop/conf
ENV HADOOP_USER_NAME=hadoop

# Copy Hadoop config so Spark can find the NameNode
RUN mkdir -p /opt/hadoop/conf

USER spark

CMD ["/bin/bash", "-c", "/opt/spark/sbin/start-master.sh && tail -f /opt/spark/logs/*.out"]
