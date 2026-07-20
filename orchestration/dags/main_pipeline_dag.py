"""Batch ETL pipeline: bronze -> silver -> gold -> data quality.

Every task launches a brand-new, short-lived container built from the
processing-spark image (see /processing/Dockerfile) via DockerOperator, runs
`spark-submit` inside it, then removes the container. This guarantees Spark code
only ever executes inside a /processing-owned container -- never inside the
Airflow container itself -- regardless of how the task is triggered.

After changing anything under /processing/jobs, rebuild the image so these tasks
pick up the change:
    docker compose -f processing/docker-compose.yml build spark-iceberg
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
    "retry_delay": datetime.timedelta(minutes=2),
}


def alert_on_failure(context) -> None:
    ti = context["task_instance"]
    print(
        f"[ALERT] task={ti.task_id} dag={ti.dag_id} run_id={context['run_id']} FAILED. "
        f"See the task log above for the Spark job's stack trace."
    )


def spark_task(task_id: str, script: str) -> DockerOperator:
    return DockerOperator(
        task_id=task_id,
        image=PROCESSING_IMAGE,
        api_version="auto",
        auto_remove="success",
        command=f"spark-submit {JOBS_DIR}/{script}",
        docker_url="unix://var/run/docker.sock",
        network_mode=NETWORK,
        mounts=[IVY_CACHE_MOUNT],
        mount_tmp_dir=False,
        # env parity with the long-lived spark-iceberg service in
        # processing/docker-compose.yml, which also sets AWS_REGION
        environment={"AWS_REGION": "us-east-1"},
        on_failure_callback=alert_on_failure,
    )


with DAG(
    dag_id="main_pipeline_dag",
    description="Batch ETL: bronze -> silver -> gold -> data quality checks",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime.datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["batch", "mcdonalds"],
) as dag:

    load_batch_to_bronze = spark_task(
        "load_batch_to_bronze",
        "batch_to_bronze.py",
    )

    run_bronze_to_silver = spark_task(
        "run_bronze_to_silver",
        "bronze_to_silver.py",
    )

    build_silver_conformed = spark_task(
        "build_silver_conformed",
        "build_silver_conformed.py",
    )

    build_gold_dimensions = spark_task(
        "build_gold_dimensions",
        "build_gold_dimensions.py",
    )

    update_scd2_dim_store = spark_task(
        "update_scd2_dim_store",
        "scd2_dim_store.py",
    )

    build_gold_facts = spark_task(
        "build_gold_facts",
        "build_gold_facts.py",
    )

    build_gold_aggregates = spark_task(
        "build_gold_aggregates",
        "build_gold_aggregates.py",
    )

    run_quality_checks = spark_task(
        "run_quality_checks",
        "data_quality_checks.py",
    )

    (
        load_batch_to_bronze
        >> run_bronze_to_silver
        >> build_silver_conformed
        >> build_gold_dimensions
        >> update_scd2_dim_store
        >> build_gold_facts
        >> build_gold_aggregates
        >> run_quality_checks
    )