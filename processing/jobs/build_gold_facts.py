from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def stable_key(*columns):
    return F.sha2(
        F.concat_ws(
            "|",
            *[
                F.coalesce(
                    column.cast("string"),
                    F.lit(""),
                )
                for column in columns
            ],
        ),
        256,
    )


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


def add_store_key(
    dataframe: DataFrame,
    dim_store: DataFrame,
    event_time_column: str,
) -> DataFrame:
    return (
        dataframe.alias("fact")
        .join(
            dim_store.alias("store"),
            (
                F.col("fact.store_id")
                == F.col("store.store_id")
            )
            & (
                F.col(f"fact.{event_time_column}")
                >= F.col("store.valid_from")
            )
            & (
                F.col(f"fact.{event_time_column}")
                < F.col("store.valid_to")
            ),
            "left",
        )
        .select(
            "fact.*",
            F.col("store.store_key"),
        )
    )


def build_fact_orders(
    spark: SparkSession,
    dim_store: DataFrame,
) -> DataFrame:
    orders = (
        spark.table("lake.silver.orders_clean")
        .withColumn(
            "fact_event_time",
            F.col("order_date").cast("timestamp"),
        )
    )

    return (
        add_store_key(
            orders,
            dim_store,
            "fact_event_time",
        )
        .withColumn(
            "order_key",
            stable_key(F.col("order_id")),
        )
        .withColumn(
            "date_key",
            F.date_format(
                "order_date",
                "yyyyMMdd",
            ).cast("int"),
        )
        .withColumn(
            "is_delayed_delivery",
            F.col("delivery_delay") > 0,
        )
        .select(
            "order_key",
            "order_id",
            "store_key",
            "store_id",
            "date_key",
            "customer_id",
            "food_item",
            "order_value",
            "delivery_distance",
            "delivery_delay",
            "delivery_method",
            "customer_rating",
            "customer_satisfaction",
            "route_efficiency",
            "traffic_condition",
            "weather_condition",
            "is_delayed_delivery",
            F.col("is_late").alias(
                "is_late_arriving"
            ),
            "event_time",
            "event_ingestion_time",
        )
    )


def build_fact_reviews(
    spark: SparkSession,
    dim_store: DataFrame,
) -> DataFrame:
    reviews = (
        spark.table("lake.silver.reviews_enriched")
        .withColumn(
            "fact_event_time",
            F.col("ingestion_date").cast("timestamp"),
        )
    )

    return (
        add_store_key(
            reviews,
            dim_store,
            "fact_event_time",
        )
        .withColumn(
            "review_key",
            stable_key(F.col("reviewer_id")),
        )
        .withColumn(
            "date_key",
            F.date_format(
                "ingestion_date",
                "yyyyMMdd",
            ).cast("int"),
        )
        .select(
            "review_key",
            "reviewer_id",
            "store_key",
            "store_id",
            "date_key",
            "rating",
            "review",
            "review_time",
            "has_coordinates",
        )
    )


def build_fact_traffic(
    spark: SparkSession,
    dim_store: DataFrame,
    dim_road_segment: DataFrame,
) -> DataFrame:
    traffic = add_store_key(
        spark.table("lake.silver.traffic_enriched"),
        dim_store,
        "event_time",
    )

    return (
        traffic.alias("traffic")
        .join(
            dim_road_segment.alias("road"),
            "road_segment_id",
            "left",
        )
        .withColumn(
            "traffic_observation_key",
            stable_key(
                F.col("traffic.road_segment_id"),
                F.col("traffic.event_time"),
                F.col("traffic.gps_latitude"),
                F.col("traffic.gps_longitude"),
                F.col("traffic.vehicle_count"),
                F.col("traffic.average_speed"),
                F.col("traffic.lane_occupancy_rate"),
                F.col("traffic.jam_density_index"),
                F.col("traffic.hard_braking_events"),
                F.col(
                    "traffic.rapid_acceleration_events"
                ),
                F.col("traffic.anomaly_label"),
            ),
        )
        .withColumn(
            "date_key",
            F.date_format(
                "traffic.event_date",
                "yyyyMMdd",
            ).cast("int"),
        )
        .select(
            "traffic_observation_key",
            F.col("traffic.store_key"),
            F.col("traffic.store_id"),
            F.col("road.road_segment_key"),
            F.col("traffic.road_segment_id"),
            "date_key",
            F.col("traffic.event_time"),
            F.col("traffic.vehicle_count"),
            F.col("traffic.average_speed"),
            F.col("traffic.lane_occupancy_rate"),
            F.col("traffic.jam_density_index"),
            F.col("traffic.hard_braking_events"),
            F.col(
                "traffic.rapid_acceleration_events"
            ),
            F.col("traffic.weather_condition"),
            F.col("traffic.road_surface_status"),
            F.col("traffic.incident_type"),
            F.col("traffic.anomaly_label"),
        )
    )


def validate_fact(
    dataframe: DataFrame,
    primary_key: str,
    required_keys,
    fact_name: str,
) -> None:
    missing_condition = F.col(primary_key).isNull()

    for key_name in required_keys:
        missing_condition = (
            missing_condition
            | F.col(key_name).isNull()
        )

    missing_keys = (
        dataframe
        .filter(missing_condition)
        .limit(1)
        .count()
    )

    duplicate_keys = (
        dataframe
        .groupBy(primary_key)
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )

    if missing_keys:
        raise ValueError(
            f"{fact_name} contains missing keys."
        )

    if duplicate_keys:
        raise ValueError(
            f"{fact_name} contains duplicate primary keys."
        )


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("build-gold-facts")
        .getOrCreate()
    )

    spark.conf.set(
        "spark.sql.session.timeZone",
        "UTC",
    )

    spark.sql(
        "CREATE NAMESPACE IF NOT EXISTS lake.gold"
    )

    try:
        dim_store = spark.table(
            "lake.gold.dim_store"
        )
        dim_road_segment = spark.table(
            "lake.gold.dim_road_segment"
        )

        fact_orders = build_fact_orders(
            spark,
            dim_store,
        ).cache()

        fact_reviews = build_fact_reviews(
            spark,
            dim_store,
        ).cache()

        fact_traffic = build_fact_traffic(
            spark,
            dim_store,
            dim_road_segment,
        ).cache()

        validate_fact(
            fact_orders,
            "order_key",
            [
                "store_key",
                "date_key",
            ],
            "fact_orders",
        )

        validate_fact(
            fact_reviews,
            "review_key",
            [
                "store_key",
                "date_key",
            ],
            "fact_customer_reviews",
        )

        validate_fact(
            fact_traffic,
            "traffic_observation_key",
            [
                "store_key",
                "road_segment_key",
                "date_key",
            ],
            "fact_traffic_observations",
        )

        write_table(
            fact_orders,
            "lake.gold.fact_orders",
        )
        write_table(
            fact_reviews,
            "lake.gold.fact_customer_reviews",
        )
        write_table(
            fact_traffic,
            "lake.gold.fact_traffic_observations",
        )

        fact_orders.unpersist()
        fact_reviews.unpersist()
        fact_traffic.unpersist()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
    