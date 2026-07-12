"""Infra smoke test -- NOT part of the real pipeline.

Proves the Spark <-> Iceberg REST catalog <-> MinIO wiring works before any real
job logic exists: creates the bronze/silver/gold namespaces and writes/reads back
one throwaway table. Run manually with:

    docker exec spark-iceberg spark-submit /opt/processing/jobs/_smoke_test.py
"""

from pyspark.sql import SparkSession


def main() -> None:
    spark = SparkSession.builder.appName("infra-smoke-test").getOrCreate()

    for namespace in ("bronze", "silver", "gold"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS lake.{namespace}")

    df = spark.createDataFrame(
        [("smoke-1", "5021", 42.5, 31, "Low", "2026-06-24T12:30:00", "2026-06-24T12:31:00")],
        ["order_id", "store_id", "order_value", "delivery_duration", "traffic_condition", "event_time", "ingestion_time"],
    )
    df.writeTo("lake.bronze.infra_smoke_test").createOrReplace()

    print("=== infra smoke test read-back (lake.bronze.infra_smoke_test) ===")
    spark.table("lake.bronze.infra_smoke_test").show(truncate=False)
    print("=== namespaces visible in the catalog ===")
    spark.sql("SHOW NAMESPACES IN lake").show()

    spark.stop()


if __name__ == "__main__":
    main()
