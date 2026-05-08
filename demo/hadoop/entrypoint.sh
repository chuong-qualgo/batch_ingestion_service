#!/bin/bash
set -e

export JAVA_HOME=/opt/java/openjdk
export PATH=${JAVA_HOME}/bin:${PATH}

ROLE="${1:-namenode}"

# Format NameNode only on first start
if [ "$ROLE" = "namenode" ]; then
    if [ ! -d "${HADOOP_HOME}/data/namenode/current" ]; then
        echo "Formatting HDFS NameNode for the first time..."
        ${HADOOP_HOME}/bin/hdfs namenode -format -nonInteractive -force
    fi
    echo "Starting NameNode..."
    exec ${HADOOP_HOME}/bin/hdfs namenode

elif [ "$ROLE" = "datanode" ]; then
    echo "Starting DataNode..."
    exec ${HADOOP_HOME}/bin/hdfs datanode
fi
