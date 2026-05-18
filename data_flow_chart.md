flowchart TD
    Trigger(["Airflow DAG\nscheduled / manual trigger"])

    subgraph DAG["DAG  —  demo_postgres_to_hadoop"]
        direction TB
        IO["InitOperator\n① Read pipeline_config.yaml\n② Resolve credentials from OpenBao\n③ Compute checkpoint window\n④ Push RunContext → XCom"]
        SR["SparkRunOperator\n① Pull RunContext from XCom\n② Build SparkSession\n③ Validate source + sink\n④a FILE source → FileUtil.copy()\n④b DB source  → read() → write()\n⑤ Push record_count → XCom"]
        MP["MetricPushOperator\n① Persist checkpoint_to → Postgres\n② Publish pipeline metric → Kafka"]

        IO --> SR --> MP
    end

    subgraph Sources["Data Sources"]
        SRC_PG[("PostgreSQL\norders_db")]
        SRC_HDFS[("HDFS\nfile source")]
        SRC_S3[("S3 / GCS / ABFS\nfile source")]
        SRC_OTHER[("MySQL / MongoDB\nDynamoDB / Cassandra")]
    end

    subgraph Sinks["Data Sinks"]
        SINK_HDFS[("HDFS\nhdfs://namenode:9000\n/data/{system}/{date}/")]
        SINK_S3[("S3  (s3a://)\nor GCS / ABFS")]
    end

    subgraph CrossCutting["Cross-Cutting Services"]
        OB["OpenBao\nKV-v2\ncredential_ref → {user,pass,host,…}"]
        CP[("PostgreSQL\ncheckpoints table\ndag_id → checkpoint_to")]
        KF[("Kafka\npipeline-metrics topic\nstatus / record_count / run_id")]
    end

    Trigger --> DAG

    IO -->|"get_secret()"| OB
    OB -->|"credentials dict"| IO
    IO -->|"read checkpoint_from"| CP

    SR -->|"JDBC / connector"| SRC_PG
    SR -->|"JDBC / connector"| SRC_OTHER
    SR -->|"FileUtil.copy()\n(no Spark memory)"| SRC_HDFS
    SR -->|"FileUtil.copy()\n(no Spark memory)"| SRC_S3

    SR -->|"DataFrame.write\nor FileUtil.copy()"| SINK_HDFS
    SR -->|"DataFrame.write\nor FileUtil.copy()"| SINK_S3

    MP -->|"UPDATE checkpoint_to"| CP
    MP -->|"PRODUCE metric event"| KF

    SR -->|"failure metric\n(fire-and-forget)"| KF