import re

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


SILVER_NAMESPACE = "lake.silver"


def normalize_column_names(dataframe: DataFrame) -> DataFrame:
    for old_name in dataframe.columns:
        new_name = re.sub(
            r"[^a-z0-9_]+",
            "_",
            old_name.strip().lower(),
        ).strip("_")

        if old_name != new_name:
            dataframe = dataframe.withColumnRenamed(
                old_name,
                new_name,
            )

    return dataframe


def write_table(
    dataframe: DataFrame,
    table_name: str,
) -> None:
    (
        dataframe.writeTo(table_name)
        .using("iceberg")
        .createOrReplace()
    )


def write_quality_results(
    dataframe: DataFrame,
    valid_condition,
    clean_table: str,
    rejected_table: str,
) -> None:
    resolved_condition = F.coalesce(
        valid_condition,
        F.lit(False),
    )

    prepared = dataframe.cache()

    clean = (
        prepared
        .filter(resolved_condition)
        .withColumn("dq_status", F.lit("valid"))
    )

    rejected = (
        prepared
        .filter(~resolved_condition)
        .withColumn("dq_status", F.lit("rejected"))
    )

    clean_count = clean.count()
    rejected_count = rejected.count()

    write_table(clean, clean_table)
    write_table(rejected, rejected_table)

    prepared.unpersist()

    print(
        f"{clean_table}: {clean_count} valid, "
        f"{rejected_count} rejected."
    )


def process_reviews(spark: SparkSession) -> None:
    reviews = normalize_column_names(
        spark.table("lake.bronze.reviews_raw")
    )

    reviews = (
        reviews
        .withColumnRenamed("rating", "rating_raw")
        .withColumn(
            "rating",
            F.regexp_extract(
                F.col("rating_raw"),
                r"([1-5])",
                1,
            ).cast("int"),
        )
        .withColumn(
            "rating_count",
            F.regexp_replace(
                F.col("rating_count"),
                r"[^0-9]",
                "",
            ).cast("long"),
        )
        .withColumn(
            "latitude",
            F.col("latitude").cast("double"),
        )
        .withColumn(
            "longitude",
            F.col("longitude").cast("double"),
        )
        .withColumn(
            "has_coordinates",
            F.col("latitude").isNotNull()
            & F.col("longitude").isNotNull(),
        )
        .withColumn(
            "silver_processed_time",
            F.current_timestamp(),
        )
    )

    valid = (
        F.col("reviewer_id").isNotNull()
        & (F.length(F.trim(F.col("reviewer_id"))) > 0)
        & F.col("store_address").isNotNull()
        & F.col("rating").between(1, 5)
        & (
            F.col("latitude").isNull()
            | F.col("latitude").between(-90.0, 90.0)
        )
        & (
            F.col("longitude").isNull()
            | F.col("longitude").between(-180.0, 180.0)
        )
    )

    write_quality_results(
        reviews,
        valid,
        "lake.silver.reviews_clean",
        "lake.silver.reviews_rejected",
    )


def process_traffic(spark: SparkSession) -> None:
    traffic = normalize_column_names(
        spark.table("lake.bronze.traffic_raw")
    )

    integer_columns = [
        "vehicle_count",
        "hard_braking_events",
        "rapid_acceleration_events",
        "anomaly_label",
    ]

    double_columns = [
        "gps_latitude",
        "gps_longitude",
        "distance_to_intersection",
        "average_speed",
        "lane_occupancy_rate",
        "jam_density_index",
        "lane_changes_per_minute",
        "stop_duration_avg",
        "visibility_range",
        "v2x_packet_loss_rate",
        "v2v_beacon_interval_avg",
        "v2x_message_delay_avg",
    ]

    for column_name in integer_columns:
        traffic = traffic.withColumn(
            column_name,
            F.col(column_name).cast("int"),
        )

    for column_name in double_columns:
        traffic = traffic.withColumn(
            column_name,
            F.col(column_name).cast("double"),
        )

    traffic = (
        traffic
        .withColumnRenamed("timestamp", "timestamp_raw")
        .withColumn(
            "event_time",
            F.to_timestamp(F.col("timestamp_raw")),
        )
        .withColumn(
            "event_date",
            F.to_date(F.col("event_time")),
        )
        .withColumn(
            "silver_processed_time",
            F.current_timestamp(),
        )
    )

    valid = (
        F.col("road_segment_id").isNotNull()
        & F.col("event_time").isNotNull()
        & (F.col("vehicle_count") >= 0)
        & (F.col("average_speed") >= 0)
        & F.col("gps_latitude").between(-90.0, 90.0)
        & F.col("gps_longitude").between(-180.0, 180.0)
    )

    write_quality_results(
        traffic,
        valid,
        "lake.silver.traffic_clean",
        "lake.silver.traffic_rejected",
    )


def json_value(column_name: str):
    return F.get_json_object(
        F.col("raw_payload"),
        f"$.{column_name}",
    )


def process_orders(spark: SparkSession) -> None:
    orders = spark.table(
        "lake.bronze.orders_stream_raw"
    )

    string_columns = [
        "order_id",
        "restaurant_id",
        "store_id",
        "food_item",
        "delivery_method",
        "traffic_condition",
        "weather_condition",
        "route_taken",
        "customer_id",
        "gender",
        "location",
        "preferred_cuisine",
        "order_frequency",
        "loyalty_program",
        "food_freshness",
        "packaging_quality",
        "food_condition",
        "small_route",
        "bike_friendly_route",
        "route_type",
        "traffic_avoidance",
        "source_dataset",
    ]

    double_columns = [
        "delivery_distance",
        "order_value",
        "delivery_delay",
        "food_temperature",
        "customer_satisfaction",
        "route_efficiency",
    ]

    integer_columns = [
        "age",
        "order_history",
        "customer_rating",
    ]

    for column_name in string_columns:
        orders = orders.withColumn(
            column_name,
            json_value(column_name),
        )

    for column_name in double_columns:
        orders = orders.withColumn(
            column_name,
            json_value(column_name).cast("double"),
        )

    for column_name in integer_columns:
        orders = orders.withColumn(
            column_name,
            json_value(column_name).cast("int"),
        )

    orders = (
        orders
        .withColumn(
            "order_date",
            F.to_date(json_value("order_time")),
        )
        .withColumn(
            "delivery_date",
            F.to_date(json_value("delivery_time")),
        )
        .withColumn(
            "event_time",
            F.to_timestamp(json_value("event_time")),
        )
        .withColumn(
            "event_ingestion_time",
            F.to_timestamp(json_value("ingestion_time")),
        )
        .withColumn(
            "lateness_hours",
            (
                F.unix_timestamp("event_ingestion_time")
                - F.unix_timestamp("event_time")
            ) / F.lit(3600.0),
        )
        .withColumn(
            "is_late",
            F.col("lateness_hours") > 0,
        )
        .withColumn(
            "silver_processed_time",
            F.current_timestamp(),
        )
    )

    valid = (
        F.col("order_id").isNotNull()
        & (F.length(F.trim(F.col("order_id"))) > 0)
        & F.col("store_id").isNotNull()
        & (F.length(F.trim(F.col("store_id"))) > 0)
        & (F.col("restaurant_id") == F.col("store_id"))
        & (F.col("order_value") > 0)
        & (F.col("delivery_distance") >= 0)
        & F.col("customer_rating").between(1, 5)
        & F.col("order_date").isNotNull()
        & F.col("event_time").isNotNull()
        & F.col("event_ingestion_time").isNotNull()
        & F.col("lateness_hours").between(0.0, 48.0)
    )

    write_quality_results(
        orders,
        valid,
        "lake.silver.orders_clean",
        "lake.silver.orders_rejected",
    )


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("bronze-to-silver")
        .getOrCreate()
    )

    spark.sql(
        f"CREATE NAMESPACE IF NOT EXISTS {SILVER_NAMESPACE}"
    )

    try:
        process_reviews(spark)
        process_traffic(spark)
        process_orders(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()