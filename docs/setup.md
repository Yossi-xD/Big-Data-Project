# Setup (detailed)

This expands on the root [`README.md`](../README.md) quickstart with more detail
and troubleshooting. Read the README first if you just want the fastest path to
a running system.

## Prerequisites

- Docker Desktop (or Docker Engine + Compose v2) with at least ~4GB RAM assigned.
- Ports free on the host: `9000`, `9001` (MinIO), `8181` (Iceberg REST), `4040`
  (Spark UI), `29092` (Kafka, host-reachable listener), `8085` (Kafka UI),
  `8080` (Airflow UI).
- Internet access on first run: Docker Hub pulls, plus Spark resolving the Kafka
  connector jar (`spark.jars.packages`) the first time any streaming job runs
  (cached afterwards in the `ivy2_cache` volume).

## Bring-up order

The three components are independent Compose projects that share one Docker
network. Bring them up in this order so dependents don't race ahead of what
they depend on:

```bash
docker network create bigdata_net

docker compose -f processing/docker-compose.yml up -d --build
docker compose -f streaming/docker-compose.yml up -d --build
docker compose -f orchestration/docker-compose.yml up -d --build
```

`--build` is only strictly required the first time (or after editing a
Dockerfile / anything under `processing/jobs`); subsequent `up -d` calls reuse
the built images.

## Verifying each layer

**MinIO** -- open http://localhost:9001 (user `admin`, password `password12345`).
You should see a `warehouse` bucket with `bronze/`, `silver/`, `gold/` prefixes
(created by the one-shot `mc-init` container).

**Iceberg + Spark** -- run the infra smoke test, which creates the three
namespaces and writes/reads back one throwaway table:

```bash
docker exec spark-iceberg spark-submit /opt/processing/jobs/_smoke_test.py
```

You should see the row printed back, and a new `infra_smoke_test` object appear
under `warehouse/bronze/` in the MinIO console.

**Kafka** -- confirm the topic exists and watch messages arrive:

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders_stream --from-beginning --max-messages 5
```

Or open Kafka UI at http://localhost:8085 and browse the `orders_stream` topic
visually -- this is the easiest way to see late-arriving messages (compare
`event_time` vs `ingestion_time` in the payload).

**Airflow** -- open http://localhost:8080 (user/password `admin`/`admin`). Both
`main_pipeline_dag` and `stream_ingestion_dag` should be visible and unpaused.
Until the real job scripts land in `/processing/jobs` (see that folder's
`README.md`), triggering a DAG run will fail at the "no such file" step inside
the container -- that confirms the DockerOperator/network/image plumbing is
correct and only the job logic is pending.

## Rebuilding after job changes

Job scripts under `processing/jobs/` are baked into the `processing-spark`
image at build time, not bind-mounted, so both the long-lived container and
every Airflow-launched ephemeral container run from the same, predictable
image. After adding/editing a job script:

```bash
docker compose -f processing/docker-compose.yml build spark-iceberg
```

No DAG changes are needed for this -- just re-trigger the run in Airflow.

## Troubleshooting

- **`docker: network bigdata_net not found`** -- run
  `docker network create bigdata_net` before bringing up any component.
- **Airflow tasks stuck / DockerOperator can't connect to the daemon** -- on
  Linux, ensure `airflow-scheduler` can reach `/var/run/docker.sock` (the
  compose file already runs that service as `root` to sidestep group
  permissions; if your daemon socket lives elsewhere, adjust the bind mount in
  `orchestration/docker-compose.yml`).
- **First streaming job run is slow / needs internet** -- expected: it's
  resolving `spark-sql-kafka-0-10` via Ivy. Subsequent runs reuse the
  `ivy2_cache` Docker volume.
- **Tearing everything down**: `docker compose -f <file> down` per component
  (`-v` additionally to drop volumes / start from a clean warehouse), then
  optionally `docker network rm bigdata_net`.
