from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


OUTPUT_TABLE = "lake.gold.agg_store_performance"


def build_order_metrics(
    spark: SparkSession,
) -> DataFrame:
    return (
        spark.table("lake.gold.fact_orders")
        .groupBy("store_id")
        .agg(
            F.count("*").alias("order_count"),
            F.countDistinct("customer_id").alias(
                "unique_customers"
            ),
            F.round(
                F.sum("order_value"),
                2,
            ).alias("total_revenue"),
            F.round(
                F.avg("order_value"),
                2,
            ).alias("average_order_value"),
            F.round(
                F.avg("delivery_delay"),
                2,
            ).alias("average_delivery_delay"),
            F.sum(
                F.when(
                    F.col("is_delayed_delivery"),
                    1,
                ).otherwise(0)
            ).alias("delayed_order_count"),
            F.round(
                F.avg("customer_rating"),
                2,
            ).alias("average_customer_rating"),
            F.round(
                F.avg("customer_satisfaction"),
                2,
            ).alias("average_customer_satisfaction"),
            F.sum(
                F.when(
                    F.col("is_late_arriving"),
                    1,
                ).otherwise(0)
            ).alias("late_arriving_order_count"),
        )
    )


def build_review_metrics(
    spark: SparkSession,
) -> DataFrame:
    return (
        spark.table(
            "lake.gold.fact_customer_reviews"
        )
        .groupBy("store_id")
        .agg(
            F.count("*").alias("review_count"),
            F.round(
                F.avg("rating"),
                2,
            ).alias("average_review_rating"),
            F.sum(
                F.when(
                    F.col("rating") <= 2,
                    1,
                ).otherwise(0)
            ).alias("negative_review_count"),
        )
    )


def build_traffic_metrics(
    spark: SparkSession,
) -> DataFrame:
    return (
        spark.table(
            "lake.gold.fact_traffic_observations"
        )
        .groupBy("store_id")
        .agg(
            F.count("*").alias(
                "traffic_observation_count"
            ),
            F.round(
                F.avg("vehicle_count"),
                2,
            ).alias("average_vehicle_count"),
            F.round(
                F.avg("average_speed"),
                2,
            ).alias("average_traffic_speed"),
            F.round(
                F.avg("jam_density_index"),
                4,
            ).alias("average_jam_density"),
            F.sum(
                F.when(
                    F.col("anomaly_label") == 1,
                    1,
                ).otherwise(0)
            ).alias("traffic_anomaly_count"),
        )
    )


def safe_rate(
    numerator,
    denominator,
):
    return F.when(
        denominator > 0,
        F.round(
            numerator / denominator,
            4,
        ),
    )


def build_store_performance(
    spark: SparkSession,
) -> DataFrame:
    current_stores = (
        spark.table("lake.gold.dim_store")
        .filter(F.col("is_current"))
        .select(
            "store_key",
            "store_id",
            "store_name",
            "store_address",
            "latitude",
            "longitude",
            "store_profile_source",
        )
    )

    result = (
        current_stores
        .join(
            build_order_metrics(spark),
            "store_id",
            "left",
        )
        .join(
            build_review_metrics(spark),
            "store_id",
            "left",
        )
        .join(
            build_traffic_metrics(spark),
            "store_id",
            "left",
        )
    )

    count_columns = [
        "order_count",
        "unique_customers",
        "delayed_order_count",
        "late_arriving_order_count",
        "review_count",
        "negative_review_count",
        "traffic_observation_count",
        "traffic_anomaly_count",
    ]

    for column_name in count_columns:
        result = result.withColumn(
            column_name,
            F.coalesce(
                F.col(column_name),
                F.lit(0),
            ),
        )

    return (
        result
        .withColumn(
            "delayed_order_rate",
            safe_rate(
                F.col("delayed_order_count"),
                F.col("order_count"),
            ),
        )
        .withColumn(
            "negative_review_rate",
            safe_rate(
                F.col("negative_review_count"),
                F.col("review_count"),
            ),
        )
        .withColumn(
            "traffic_anomaly_rate",
            safe_rate(
                F.col("traffic_anomaly_count"),
                F.col("traffic_observation_count"),
            ),
        )
        .withColumn(
            "gold_processed_time",
            F.current_timestamp(),
        )
    )


def validate_output(
    dataframe: DataFrame,
) -> None:
    missing_store_keys = (
        dataframe
        .filter(
            F.col("store_key").isNull()
            | F.col("store_id").isNull()
        )
        .limit(1)
        .count()
    )

    duplicate_stores = (
        dataframe
        .groupBy("store_id")
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )

    if missing_store_keys:
        raise ValueError(
            "Store performance contains missing keys."
        )

    if duplicate_stores:
        raise ValueError(
            "Store performance contains duplicate stores."
        )


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("build-gold-aggregates")
        .getOrCreate()
    )

    spark.sql(
        "CREATE NAMESPACE IF NOT EXISTS lake.gold"
    )

    try:
        store_performance = (
            build_store_performance(spark).cache()
        )

        validate_output(store_performance)

        (
            store_performance.writeTo(OUTPUT_TABLE)
            .using("iceberg")
            .createOrReplace()
        )

        row_count = store_performance.count()
        store_performance.unpersist()

        print(
            f"{OUTPUT_TABLE}: wrote {row_count} stores."
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
    