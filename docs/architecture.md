# Architecture

## Component ownership

| Folder | Owns | Docker Compose |
|---|---|---|
| `/streaming` | Kafka (KRaft), topic creation, the `orders_stream` producer | `streaming/docker-compose.yml` |
| `/processing` | MinIO (data lake), Iceberg REST catalog, the Spark runtime image + all Spark job scripts | `processing/docker-compose.yml` |
| `/orchestration` | Airflow (scheduler/webserver/metadata DB), DAGs | `orchestration/docker-compose.yml` |

All three compose files join one external Docker network, `bigdata_net`, created
once up front (`docker network create bigdata_net`) so containers can address
each other by service name (`kafka`, `minio`, `iceberg-rest`, `spark-iceberg`)
regardless of which compose file started them.

## The three data sources

Per the team's own design (`McDonald's analysis.pdf`, Data Sources Overview),
each source plays a different role -- this shapes how each one enters the
system:

| Source | Role | Entry point |
|---|---|---|
| Food Delivery (orders) | real-time streaming | Kafka `orders_stream` topic (`/streaming`) |
| Store Reviews | **late-arrival** (review_time trails ingestion_time by hours/days) | batch file -> MinIO `landing` bucket (`/processing/seed_data`) |
| Stores | static/batch reference data (feeds `dim_store`, SCD2) | batch file -> MinIO `landing` bucket (`/processing/seed_data`) |

Reviews and Stores are real Kaggle datasets Tama sources directly; the infra
side only provides the landing zone they get uploaded into (see
`processing/seed_data/README.md`) and the Spark environment to read them
(`s3a://landing/...`). The 48-hour late-arrival requirement is primarily
about Reviews, not orders -- the orders producer backdating some events is
just a secondary bonus demonstration on the streaming side.

## Service topology

```mermaid
flowchart LR
    subgraph streaming ["/streaming"]
        producer["orders-producer"] --> kafka[("Kafka\n(KRaft, topic: orders_stream)")]
        kafkaui["kafka-ui"] -.inspect.-> kafka
    end

    subgraph processing ["/processing"]
        seed["seed_data/\n(reviews.csv, stores.csv -- Tama)"] -->|mc-init uploads once| landing[("MinIO\nlanding bucket")]
        spark["spark-iceberg\n(Spark + Iceberg + hadoop-aws)"]
        rest["iceberg-rest\n(Iceberg REST catalog)"]
        minio[("MinIO\nwarehouse/{bronze,silver,gold}")]
        landing -->|batch_to_bronze.py reads| spark
        spark -->|reads/writes tables| rest
        rest -->|table data + metadata| minio
        spark -->|S3A| minio
    end

    subgraph orchestration ["/orchestration"]
        scheduler["airflow-scheduler"]
        webserver["airflow-webserver"]
        pg[("airflow-postgres")]
        scheduler --> pg
        webserver --> pg
    end

    kafka -->|structured streaming read| spark
    scheduler -->|DockerOperator: launches an ephemeral\nprocessing-spark container per task,\nvia the host Docker daemon| spark
```

## Why Spark only ever runs in `/processing` containers

The assignment disqualifies submissions where Spark executes outside a
`/processing` container. Airflow never runs `spark-submit` itself. Instead,
`airflow-scheduler` mounts the host's Docker socket (`/var/run/docker.sock`) and
uses `DockerOperator` to ask the **host's** Docker daemon to start a brand-new
container from the `processing-spark:latest` image for every task, run
`spark-submit` inside it, stream its logs back into the Airflow task log, and
remove the container on success. The Spark driver and all execution happen
inside that ephemeral `/processing`-built container -- the Airflow container
itself never imports or runs Spark.

The same `processing-spark:latest` image also backs the always-on
`spark-iceberg` service (idling on `tail -f /dev/null`), which exists purely so
a human can `docker exec` into it to run ad-hoc `spark-sql`/PySpark for demoing
table contents -- it is not otherwise part of the pipeline.

## Bronze / Silver / Gold in MinIO

A single MinIO bucket, `warehouse`, holds all three layers as Iceberg
namespaces: `lake.bronze`, `lake.silver`, `lake.gold` (catalog name `lake`).
Iceberg places each namespace's tables under `warehouse/<namespace>/<table>/`,
which is what gives the literal `bronze/`, `silver/`, `gold/` folder structure
the assignment asks for. A second bucket, `landing`, holds the raw,
pre-Iceberg batch files (Reviews, Stores) that `batch_to_bronze.py` reads to
produce the first bronze tables -- see `/processing/jobs/README.md` for the
exact catalog contract Spark jobs must follow, and `/docs/data_model.md` /
`/docs/bronze_silver_gold.md` for the table designs themselves.

## Orchestration: two DAGs

- `main_pipeline_dag` (daily): `load_batch_to_bronze` -> `run_bronze_to_silver`
  -> `run_silver_to_gold` -> `run_quality_checks`.
- `stream_ingestion_dag` (every 5 minutes): drains whatever has landed in the
  `orders_stream` Kafka topic since the last run into bronze, via Spark
  Structured Streaming with `trigger(availableNow=True)`.

Both DAGs use the same ephemeral-container execution model described above.
Retries (2x, with a short delay) and an `on_failure_callback` that logs a clear
failure message give basic error handling/alerting without needing an external
alerting service.
