"""Scheduled micro-batch ingestion of Kafka's orders_stream topic into bronze.

The assignment asks Airflow to "schedule both batch and streaming jobs". Rather
than running an unmanaged, always-on streaming container outside Airflow's
visibility, this DAG runs a Spark Structured Streaming job with
`trigger(availableNow=True)` every few minutes: each run drains whatever is
currently in the topic and exits, so it behaves like a batch task while the job
logic itself is genuine Structured Streaming (with a watermark handling
late-arriving events, per /processing/jobs/README.md).

Same execution model as main_pipeline_dag.py: one ephemeral processing-spark
container per run, removed on success.
"""

from __future__ import annotations

import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

PROCESSING_IMAGE = "processing-spark:latest"
NETWORK = "bigdata_net"
JOBS_DIR = "/opt/processing/jobs"
IVY_CACHE_MOUNT = Mount(source="ivy2_cache", target="/opt/processing/.ivy2", type="volume")

default_args = {
    "owner": "infrastructure",
    "retries": 2,
    "retry_delay": datetime.timedelta(minutes=1),
}

with DAG(
    dag_id="stream_ingestion_dag",
    description="Micro-batch: drain orders_stream (Kafka) into the bronze layer",
    default_args=default_args,
    schedule=datetime.timedelta(minutes=5),
    start_date=datetime.datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["streaming", "mcdonalds"],
) as dag:

    ingest_orders_stream = DockerOperator(
        task_id="ingest_orders_stream",
        image=PROCESSING_IMAGE,
        api_version="auto",
        auto_remove="success",
        command=f"spark-submit {JOBS_DIR}/stream_orders_to_bronze.py",
        docker_url="unix://var/run/docker.sock",
        network_mode=NETWORK,
        mounts=[IVY_CACHE_MOUNT],
        mount_tmp_dir=False,
    )
