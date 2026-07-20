from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


GOLD_NAMESPACE = "lake.gold"


def write_table(
    dataframe: DataFrame,
    table_name: str,
) -> None:
    (
        dataframe.writeTo(table_name)
        .using("iceberg")
        .createOrReplace()
    )

    print(f"{table_name}: created successfully.")


def build_dim_date(
    spark: SparkSession,
) -> DataFrame:
    source_dates = (
        spark.table("lake.silver.orders_clean")
        .select(F.col("order_date").alias("calendar_date"))
        .unionByName(
            spark.table("lake.silver.reviews_enriched")
            .select(
                F.col("ingestion_date").alias(
                    "calendar_date"
                )
            )
        )
        .unionByName(
            spark.table("lake.silver.traffic_enriched")
            .select(
                F.col("event_date").alias("calendar_date")
            )
        )
        .filter(F.col("calendar_date").isNotNull())
    )

    date_range = source_dates.agg(
        F.min("calendar_date").alias("start_date"),
        F.max("calendar_date").alias("end_date"),
    )

    return (
        date_range
        .select(
            F.explode(
                F.sequence("start_date", "end_date")
            ).alias("calendar_date")
        )
        .withColumn(
            "date_key",
            F.date_format(
                "calendar_date",
                "yyyyMMdd",
            ).cast("int"),
        )
        .withColumn("year", F.year("calendar_date"))
        .withColumn(
            "quarter",
            F.quarter("calendar_date"),
        )
        .withColumn("month", F.month("calendar_date"))
        .withColumn(
            "month_name",
            F.date_format("calendar_date", "MMMM"),
        )
        .withColumn(
            "day_of_month",
            F.dayofmonth("calendar_date"),
        )
        .withColumn(
            "day_of_week",
            F.dayofweek("calendar_date"),
        )
        .withColumn(
            "day_name",
            F.date_format("calendar_date", "EEEE"),
        )
        .withColumn(
            "is_weekend",
            F.dayofweek("calendar_date").isin(1, 7),
        )
        .select(
            "date_key",
            "calendar_date",
            "year",
            "quarter",
            "month",
            "month_name",
            "day_of_month",
            "day_of_week",
            "day_name",
            "is_weekend",
        )
    )


def build_dim_road_segment(
    spark: SparkSession,
) -> DataFrame:
    return (
        spark.table("lake.silver.traffic_enriched")
        .filter(F.col("road_segment_id").isNotNull())
        .groupBy("road_segment_id")
        .agg(
            F.avg("gps_latitude").alias("latitude"),
            F.avg("gps_longitude").alias("longitude"),
        )
        .withColumn(
            "road_segment_key",
            F.sha2(
                F.col("road_segment_id").cast("string"),
                256,
            ),
        )
        .select(
            "road_segment_key",
            "road_segment_id",
            "latitude",
            "longitude",
        )
    )


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("build-gold-dimensions")
        .getOrCreate()
    )

    spark.sql(
        f"CREATE NAMESPACE IF NOT EXISTS "
        f"{GOLD_NAMESPACE}"
    )

    try:
        write_table(
            build_dim_date(spark),
            "lake.gold.dim_date",
        )
        write_table(
            build_dim_road_segment(spark),
            "lake.gold.dim_road_segment",
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
    