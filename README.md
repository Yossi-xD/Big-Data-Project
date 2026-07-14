# McDonald's Branch Profitability Analytics Platform

An end-to-end data engineering pipeline (batch + streaming) built for the Big
Data Engineering final project: Kafka -> Spark -> Iceberg-on-MinIO
(bronze/silver/gold) -> Airflow, predicting underperforming McDonald's
branches from delivery, review, and traffic data. See `docs/` for the business
context, data model, and architecture, and the McDonald's analysis deck in the
repo root for the original design.

## Project layout

```
/orchestration   Airflow: DAGs, scheduling, error handling
/streaming       Kafka (KRaft) + the orders_stream producer
/processing      MinIO + Iceberg REST catalog + the Spark runtime + all job scripts
/docs            Architecture, data model, data quality docs
```

Each folder has its own `docker-compose.yml`; all three share one Docker
network so they can talk to each other.

## Quickstart

Requires Docker (Compose v2) and internet access for the initial image pulls.

```bash
# 1. one-time shared network
docker network create bigdata_net

# 2. bring up each component, in this order
docker compose -f processing/docker-compose.yml up -d --build
docker compose -f streaming/docker-compose.yml up -d --build
docker compose -f orchestration/docker-compose.yml up -d --build
```

Give it a minute for Airflow's Postgres + DB migration to finish on first boot.

## Where to look once it's up

| What | URL / command | Credentials |
|---|---|---|
| MinIO console (data lake) | http://localhost:9001 | `admin` / `password12345` |
| Kafka UI (topic/messages) | http://localhost:8085 | - |
| Airflow UI (DAGs) | http://localhost:8080 | `admin` / `admin` |
| Spark UI (while a job runs) | http://localhost:4040 | - |

Run the infrastructure smoke test to confirm Spark, the Iceberg REST catalog,
and MinIO are wired together correctly:

```bash
docker exec spark-iceberg spark-submit /opt/processing/jobs/_smoke_test.py
```

Inspect Iceberg tables from a Spark shell at any time:

```bash
docker exec -it spark-iceberg spark-sql
spark-sql> SHOW NAMESPACES IN lake;
spark-sql> SELECT * FROM lake.bronze.infra_smoke_test;
```

Full step-by-step verification (per-layer checks, rebuild instructions,
troubleshooting) is in [`docs/setup.md`](docs/setup.md); the service/network
diagram and the "why" behind the design choices are in
[`docs/architecture.md`](docs/architecture.md).

## Data contract

The `orders_stream` Kafka topic (produced by
`streaming/producer/orders_producer.py`) emits JSON matching:

```json
{
  "order_id": "ORD_1A2B3C4D5E",
  "store_id": "5021",
  "order_value": 42.5,
  "delivery_duration": 31,
  "traffic_condition": "Low",
  "event_time": "2026-06-24T12:30:00+00:00",
  "ingestion_time": "2026-06-24T14:05:00+00:00"
}
```

Events replay the real **Food Delivery** dataset committed at
`streaming/producer/data/food_delivery_dataset.csv` (20,000 orders across 100
stores): `order_id`, `store_id`, `order_value` and `traffic_condition` come
straight from the dataset, while `event_time`/`ingestion_time` are stamped at
emit time and `delivery_duration` is synthesized (the source's order/delivery
times are date-only, so no real duration exists). A configurable fraction of
events (`LATE_RATIO`, default 15%) carry an `event_time` backdated by up to
`MAX_LATE_HOURS` (default 48h) relative to `ingestion_time` -- a secondary
demonstration of late-arrival handling on the streaming side. The **primary** late-arrival source, per the team's own
architecture, is the batch **Reviews** dataset (`review_time` can trail
`ingestion_time` by hours/days) -- see
[`processing/seed_data/README.md`](processing/seed_data/README.md).

Two other batch sources (Reviews and Traffic, both sourced by Tama; Traffic
is committed as a reproducible 20,000-row sample of the full 571MB dataset --
see `processing/data_prep/create_traffic_sample.py`) land in MinIO's `landing`
bucket via `processing/seed_data/` for `batch_to_bronze.py` to read; see that
same file for the upload mechanism.

Note there is no separate Stores dataset: Traffic replaced it (per the
lecturer's feedback on the mid-term submission), and the `dim_store` dimension
is instead derived during processing from the store IDs that link the three
sources together.

Spark jobs read/write Iceberg tables through the `lake` REST catalog under
namespaces `bronze` / `silver` / `gold` -- see
[`processing/jobs/README.md`](processing/jobs/README.md) for the full
job/catalog contract, including which job files are still pending.

## Running the batch + streaming pipeline

In the Airflow UI (http://localhost:8080), two DAGs are pre-loaded and
unpaused:

- `main_pipeline_dag` (daily): `load_batch_to_bronze` -> `run_bronze_to_silver`
  -> `run_silver_to_gold` -> `run_quality_checks`.
- `stream_ingestion_dag` (every 5 min): drains `orders_stream` into bronze via
  Spark Structured Streaming.

Trigger either manually from the UI ("Trigger DAG") for a demo run. Each task
launches its own short-lived Spark container (see `docs/architecture.md` for
why), visible with `docker ps` while it runs.
