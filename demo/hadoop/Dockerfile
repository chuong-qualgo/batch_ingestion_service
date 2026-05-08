# ─────────────────────────────────────────────────────────────────────────────
# Hadoop 3.3.6 — arm64 compatible image (macOS M-series)
# Built on top of eclipse-temurin:11-jre which publishes linux/arm64 natively.
# ─────────────────────────────────────────────────────────────────────────────
FROM eclipse-temurin:11-jre-jammy

ENV HADOOP_VERSION=3.3.6
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
ENV PATH=${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:${PATH}
ENV JAVA_HOME=/opt/java/openjdk

RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
        netcat-openbsd \
        procps \
    && rm -rf /var/lib/apt/lists/*

# Download and extract Hadoop
RUN curl -fsSL \
    "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" \
    | tar -xz -C /opt \
    && mv /opt/hadoop-${HADOOP_VERSION} ${HADOOP_HOME} \
    && rm -rf ${HADOOP_HOME}/share/doc

# Create hadoop user and data directories
RUN useradd -m -s /bin/bash hadoop \
    && mkdir -p /opt/hadoop/data/namenode \
                /opt/hadoop/data/datanode \
    && chown -R hadoop:hadoop ${HADOOP_HOME}

# Copy entrypoint
COPY --chown=hadoop:hadoop entrypoint.sh ${HADOOP_HOME}/entrypoint.sh
RUN chmod +x ${HADOOP_HOME}/entrypoint.sh

USER hadoop
WORKDIR ${HADOOP_HOME}
