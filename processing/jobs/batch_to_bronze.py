from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from typing import Optional


BRONZE_NAMESPACE = "lake.bronze"

BATCH_SOURCES = [
    {
        "source_path": "s3a://landing/reviews.csv",
        "source_file": "reviews.csv",
        "source_system": "mcdonalds_reviews",
        "table_name": "lake.bronze.reviews_raw",
        "multiline": True,
        "escape": '"',
    },
    {
        "source_path": "s3a://landing/traffic_sample.csv",
        "source_file": "traffic_sample.csv",
        "source_system": "traffic_tab23",
        "table_name": "lake.bronze.traffic_raw",
        "multiline": False,
        "escape": None,
    },
]


def source_already_loaded(
    spark: SparkSession,
    table_name: str,
    source_file: str,
) -> bool:
    if not spark.catalog.tableExists(table_name):
        return False

    return (
        spark.table(table_name)
        .filter(F.col("source_file") == source_file)
        .limit(1)
        .count()
        > 0
    )


def read_raw_csv(
    spark: SparkSession,
    source_path: str,
    multiline: bool,
    escape: Optional[str],
) -> DataFrame:
    reader = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .option("multiLine", multiline)
        .option("mode", "PERMISSIVE")
    )

    if escape is not None:
        reader = reader.option("escape", escape)

    return reader.csv(source_path)


def add_ingestion_metadata(
    dataframe: DataFrame,
    source_file: str,
    source_system: str,
) -> DataFrame:
    return (
        dataframe
        .withColumn("ingestion_time", F.current_timestamp())
        .withColumn("ingestion_date", F.current_date())
        .withColumn("source_file", F.lit(source_file))
        .withColumn("source_system", F.lit(source_system))
    )


def write_bronze_table(
    spark: SparkSession,
    dataframe: DataFrame,
    table_name: str,
) -> None:
    if spark.catalog.tableExists(table_name):
        dataframe.writeTo(table_name).append()
    else:
        (
            dataframe.writeTo(table_name)
            .using("iceberg")
            .partitionedBy(F.col("ingestion_date"))
            .create()
        )


def ingest_source(
    spark: SparkSession,
    source: dict,
) -> None:
    table_name = source["table_name"]
    source_file = source["source_file"]

    if source_already_loaded(spark, table_name, source_file):
        print(
            f"Skipping {source_file}: it is already loaded into "
            f"{table_name}."
        )
        return

    raw_dataframe = read_raw_csv(
        spark=spark,
        source_path=source["source_path"],
        multiline=source["multiline"],
        escape=source["escape"],
    )

    bronze_dataframe = add_ingestion_metadata(
        dataframe=raw_dataframe,
        source_file=source_file,
        source_system=source["source_system"],
    ).cache()

    record_count = bronze_dataframe.count()

    write_bronze_table(
        spark=spark,
        dataframe=bronze_dataframe,
        table_name=table_name,
    )

    bronze_dataframe.unpersist()

    print(
        f"Loaded {record_count} records from {source_file} "
        f"into {table_name}."
    )


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("batch-to-bronze")
        .getOrCreate()
    )

    spark.sql(
        f"CREATE NAMESPACE IF NOT EXISTS {BRONZE_NAMESPACE}"
    )

    try:
        for source in BATCH_SOURCES:
            ingest_source(spark, source)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()