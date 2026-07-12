# `/processing/jobs` contract

This folder is baked into the `processing-spark:latest` image at build time
(`COPY jobs /opt/processing/jobs` in `processing/Dockerfile`). **After adding or
editing a script here, rebuild the image** so both the long-lived `spark-iceberg`
container and Airflow's ephemeral task containers pick it up:

```bash
docker compose -f processing/docker-compose.yml build spark-iceberg
```

Every script is run as `spark-submit /opt/processing/jobs/<name>.py` with no CLI
arguments -- all connection config (Iceberg catalog, MinIO, Kafka) already lives in
`processing/conf/spark-defaults.conf` and is available to any `SparkSession` you
create with `SparkSession.builder.getOrCreate()`.

## Catalog / storage contract

- Iceberg catalog name: `lake` (REST catalog, also the default catalog -- you can
  write `bronze.my_table` instead of `lake.bronze.my_table`).
- Namespaces: `bronze`, `silver`, `gold`. They map to `s3://warehouse/bronze/...`
  etc. in MinIO. Jobs should defensively run
  `CREATE NAMESPACE IF NOT EXISTS lake.<namespace>` before writing, in case a job
  runs before `_smoke_test.py` has (Airflow tasks run standalone containers, so
  there's no guaranteed one-time setup step otherwise).
- Kafka: reachable at `kafka:9092` from any container on the `bigdata_net`
  network. Topic: `orders_stream` (see `/streaming/producer/orders_producer.py`
  for the exact JSON schema it emits, and the root README's data contract section).
- The Kafka structured-streaming connector (`spark-sql-kafka-0-10`) is **not**
  pre-baked into the image -- it's resolved via `spark.jars.packages` on first use
  and cached in the `ivy2_cache` volume (shared across all Spark containers, so
  it only downloads once). This needs internet access the very first time any
  Kafka-reading job runs.
- Batch inputs (reviews, traffic): read from MinIO's `landing` bucket, e.g.
  `spark.read.option("header", True).csv("s3a://landing/reviews.csv")`. See
  `../seed_data/README.md` for how raw files get there (drop them in
  `processing/seed_data/`, an init container uploads them to `s3://landing/`
  automatically).

## Late-arrival handling: which dataset owns it

Per the team's own architecture (`McDonald's analysis.pdf`, Data Sources
Overview table), **Reviews is the "Late Arrival Source"**, not orders --
`review_time` can be hours-to-days before the batch file lands
(`ingestion_time`). The 48-hour late-arrival requirement should be
implemented as a watermark on the Reviews path in `bronze_to_silver.py` (or
wherever the reviews transformation lives), comparing `review_time` to
`ingestion_time`/processing time.

The Kafka producer *also* occasionally backdates `event_time` on orders as a
secondary, bonus demonstration of the same capability on the streaming side
(see `/streaming/producer/orders_producer.py`) -- useful if
`stream_orders_to_bronze.py` wants to show a watermark too, but it's not the
primary/required late-arrival story for this project.

## Expected files (per the team's task split)

| File | Owner | Status |
|---|---|---|
| `batch_to_bronze.py` | Tama | **present** -- loads Reviews + Traffic from `landing` into `lake.bronze.reviews_raw` / `lake.bronze.traffic_raw` (idempotent per source file, partitioned by `ingestion_date`) |
| `bronze_to_silver.py` | Tama | not yet added -- wired into `main_pipeline_dag.py` |
| `silver_to_gold.py` | Tama | not yet added -- wired into `main_pipeline_dag.py` |
| `scd2_dim_store.py` | Tama | not yet added. `dim_store` is derived from the store IDs linking the three sources (Traffic replaced the external Stores file) |
| `data_quality_checks.py` | Tama | not yet added -- wired into `main_pipeline_dag.py` |
| `stream_orders_to_bronze.py` | **unassigned** | not yet added -- wired into `stream_ingestion_dag.py`. This wasn't in either person's original file list; someone needs to own the Kafka-consuming Structured Streaming job. Suggested shape: `readStream` from `orders_stream` with a watermark (`event_time`, `48 hours`) to satisfy the late-arrival requirement, `trigger(availableNow=True)` so it behaves like a bounded batch when triggered by Airflow every few minutes, `writeStream` (or a foreachBatch write) into `lake.bronze.<table>`. |
| `_smoke_test.py` | infra | present -- **not** part of any DAG, manual-only sanity check |

Until the real scripts land, `main_pipeline_dag.py` / `stream_ingestion_dag.py`
will fail at the "file not found" step inside the container -- that's expected
and confirms the DAG plumbing (DockerOperator, network, image) is working
correctly; only the missing job file is the blocker.
