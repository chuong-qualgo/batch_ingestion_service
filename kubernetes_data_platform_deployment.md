graph TB
    EXT(["External Clients / Ops"])

    subgraph SRC["External Data Sources"]
        SRCDB["Relational / NoSQL\nPostgreSQL · MySQL\nMongoDB · Cassandra · DynamoDB"]
        SRCFILE["File Sources\nS3  |  GCS  |  Azure Blob  |  HDFS"]
    end

    subgraph SINKS["External Sink Systems"]
        SINKSTORAGE[("Sink Storage\nS3  |  GCS  |  Azure Blob  |  HDFS")]
        SINKMETRIC[("Sink Metric\nKafka  |  SQS  |  Redis Streams")]
    end

    subgraph K8s["☸  Kubernetes Cluster"]

        subgraph INGRESS["── Ingress Layer ──"]
            ING["Ingress Controller  (nginx / traefik)\nTLS termination  :443"]
        end

        subgraph NS_ORCH["namespace: orchestration"]
            AW["Airflow Webserver\nDeployment · 2 replicas\nSvc :8080"]
            ASHED["Airflow Scheduler\nDeployment · 2 replicas\nHA active-passive"]
            AWORK["Airflow Celery Workers\nDeployment · N replicas\n⇅  HPA  (queue depth / CPU)"]
            RD["Redis\nStatefulSet · 1 replica\nCelery broker + result backend\nSvc :6379"]
            FL["Flower\nDeployment · 1 replica\nCelery monitor  Svc :5555"]
            DAGVOL[("DAGs Volume\ngit-sync sidecar\nor shared RWX PVC")]

            AW <-->|"UI / API"| ASHED
            ASHED -->|"enqueue task"| RD
            RD -->|"dequeue task"| AWORK
            FL -->|"monitor queue"| RD
        end

        subgraph NS_IE["namespace: ingest-engine"]
            SM["Spark Master\nDeployment · 1 replica\nSvc :7077 RPC | :8090 UI"]
            SW["Spark Workers\nStatefulSet · N replicas\n⇅  HPA  (CPU / memory)"]
            SM -->|"schedule executors"| SW
        end

        subgraph NS_SEC["namespace: security"]
            OB["OpenBao\nStatefulSet · 1 replica\nKV-v2 secrets engine\nK8s auth  (ServiceAccount JWT)\nSvc :8200"]
            CP[("PostgreSQL  —  Checkpoint Store\nStatefulSet · 1 replica\nSvc :5432")]
        end

        subgraph VOL_ORCH["Volumes — orchestration"]
            Vrd[(redis)]
            Vdags[(airflow-dags)]
        end

        subgraph VOL_IE["Volumes — ingest-engine"]
            Vsm[(spark-master-work)]
            Vsw[(spark-worker-scratch × N\nlocal shuffle / spill)]
        end

        subgraph VOL_SEC["Volumes — security"]
            Vob[(openbao)]
            Vcp[(postgres-checkpoint)]
        end

    end

    %% Ingress routing
    EXT -->|"HTTPS :443"| ING
    ING -->|"/airflow"| AW
    ING -->|"/spark"| SM
    ING -->|"/flower"| FL
    ING -->|"/openbao"| OB

    %% Worker → ingest-engine
    AWORK -->|"submit Spark job\nspark://spark-master:7077"| SM

    %% Spark Workers → sources
    SW -->|"JDBC / connector\n(incremental read)"| SRCDB
    SW -->|"FileUtil.copy\n(zero-memory copy)"| SRCFILE

    %% Spark Workers → sinks
    SW -->|"DataFrame.write\nor FileUtil.copy"| SINKSTORAGE
    AWORK -->|"publish pipeline metrics"| SINKMETRIC

    %% Worker → security
    AWORK -->|"fetch secrets\nK8s ServiceAccount"| OB
    AWORK -->|"R/W checkpoints"| CP

    %% DAG volume mounts
    DAGVOL -.-|"mounted"| AW
    DAGVOL -.-|"mounted"| ASHED
    DAGVOL -.-|"mounted"| AWORK

    %% Volume bindings
    RD -.- Vrd
    DAGVOL -.- Vdags
    SM -.- Vsm
    SW -.- Vsw
    OB -.- Vob
    CP -.- Vcp