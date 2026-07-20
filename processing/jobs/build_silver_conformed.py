from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


REVIEW_MAP_TABLE = "lake.silver.review_store_map"
ROAD_MAP_TABLE = "lake.silver.store_road_segment_map"


def table_exists(spark: SparkSession, table_name: str) -> bool:
    return spark.catalog.tableExists(table_name)


def replace_table(dataframe: DataFrame, table_name: str) -> None:
    prepared = dataframe.cache()
    record_count = prepared.count()
    (
        prepared.writeTo(table_name)
        .using("iceberg")
        .createOrReplace()
    )
    prepared.unpersist()
    print(f"{table_name}: wrote {record_count} records.")


def append_new_mappings(
    dataframe: DataFrame,
    table_name: str,
    already_exists: bool,
) -> None:
    prepared = dataframe.cache()
    record_count = prepared.count()

    if record_count > 0:
        writer = prepared.writeTo(table_name)
        if already_exists:
            writer.append()
        else:
            writer.using("iceberg").create()

    prepared.unpersist()
    print(f"{table_name}: added {record_count} new mappings.")


def load_store_ids(spark: SparkSession) -> DataFrame:
    return (
        spark.table("lake.silver.orders_clean")
        .select(F.trim(F.col("store_id")).alias("store_id"))
        .filter(
            F.col("store_id").isNotNull()
            & (F.length(F.col("store_id")) > 0)
        )
        .distinct()
    )


def load_review_locations(spark: SparkSession) -> DataFrame:
    return (
        spark.table("lake.silver.reviews_clean")
        .filter(
            F.col("store_address").isNotNull()
            & (F.length(F.trim(F.col("store_address"))) > 0)
        )
        .groupBy("store_address")
        .agg(
            F.min("store_name").alias("store_name"),
            F.min("latitude").alias("latitude"),
            F.min("longitude").alias("longitude"),
        )
    )


def update_review_store_map(
    spark: SparkSession,
    store_ids: DataFrame,
    review_locations: DataFrame,
) -> DataFrame:
    exists = table_exists(spark, REVIEW_MAP_TABLE)

    if exists:
        existing = spark.table(REVIEW_MAP_TABLE)
        new_locations = review_locations.join(
            existing.select("store_address"),
            "store_address",
            "left_anti",
        )
        available_stores = store_ids.join(
            existing.select("store_id"),
            "store_id",
            "left_anti",
        )
    else:
        new_locations = review_locations
        available_stores = store_ids

    new_count = new_locations.count()
    available_count = available_stores.count()

    if new_count > available_count:
        raise ValueError(
            "Not enough unmapped store IDs for new review locations."
        )

    location_window = Window.orderBy("store_address")
    store_window = Window.orderBy(
        F.col("store_id").cast("long").asc_nulls_last(),
        F.col("store_id"),
    )

    ranked_locations = new_locations.withColumn(
        "mapping_rank",
        F.row_number().over(location_window),
    )
    ranked_stores = available_stores.withColumn(
        "mapping_rank",
        F.row_number().over(store_window),
    )

    updates = (
        ranked_locations
        .join(ranked_stores, "mapping_rank", "inner")
        .select(
            "store_address",
            "store_id",
            F.lit("generated_persistent_rank").alias(
                "mapping_method"
            ),
            F.current_timestamp().alias("mapping_created_time"),
        )
    )

    append_new_mappings(updates, REVIEW_MAP_TABLE, exists)
    mapping = spark.table(REVIEW_MAP_TABLE).cache()

    total = mapping.count()
    if (
        mapping.select("store_address").distinct().count() != total
        or mapping.select("store_id").distinct().count() != total
    ):
        raise ValueError("Review-store mapping is not one-to-one.")

    return mapping


def build_store_reference(
    store_ids: DataFrame,
    review_locations: DataFrame,
    review_store_map: DataFrame,
) -> DataFrame:
    profiles = (
        review_store_map
        .join(review_locations, "store_address", "left")
        .select(
            "store_id",
            "store_address",
            "store_name",
            "latitude",
            "longitude",
            "mapping_method",
        )
    )

    return (
        store_ids
        .join(profiles, "store_id", "left")
        .select(
            "store_id",
            F.coalesce(
                F.col("store_name"),
                F.concat(
                    F.lit("McDonald's Store "),
                    F.col("store_id"),
                ),
            ).alias("store_name"),
            "store_address",
            "latitude",
            "longitude",
            F.when(
                F.col("mapping_method").isNotNull(),
                F.lit("reviews_enriched_profile"),
            ).otherwise(
                F.lit("orders_only_profile")
            ).alias("store_profile_source"),
            F.current_timestamp().alias("conformed_processed_time"),
        )
    )


def build_reviews_enriched(
    spark: SparkSession,
    review_store_map: DataFrame,
) -> DataFrame:
    return (
        spark.table("lake.silver.reviews_clean").alias("reviews")
        .join(
            review_store_map.alias("mapping"),
            F.col("reviews.store_address")
            == F.col("mapping.store_address"),
            "left",
        )
        .select(
            "reviews.*",
            F.col("mapping.store_id"),
            F.col("mapping.mapping_method").alias(
                "store_mapping_method"
            ),
            F.current_timestamp().alias("conformed_processed_time"),
        )
    )


def update_road_segment_map(
    spark: SparkSession,
    store_ids: DataFrame,
) -> DataFrame:
    segments = (
        spark.table("lake.silver.traffic_clean")
        .select("road_segment_id")
        .filter(F.col("road_segment_id").isNotNull())
        .distinct()
    )
    exists = table_exists(spark, ROAD_MAP_TABLE)

    if exists:
        existing = spark.table(ROAD_MAP_TABLE)
        new_segments = segments.join(
            existing.select("road_segment_id"),
            "road_segment_id",
            "left_anti",
        )
    else:
        new_segments = segments

    store_count = store_ids.count()
    if store_count == 0:
        raise ValueError("No store IDs found in orders_clean.")

    segment_window = Window.orderBy("road_segment_id")
    store_window = Window.orderBy(
        F.col("store_id").cast("long").asc_nulls_last(),
        F.col("store_id"),
    )

    ranked_segments = new_segments.withColumn(
        "segment_rank",
        F.row_number().over(segment_window),
    ).withColumn(
        "store_rank",
        F.pmod(F.col("segment_rank") - F.lit(1), F.lit(store_count))
        + F.lit(1),
    )
    ranked_stores = store_ids.withColumn(
        "store_rank",
        F.row_number().over(store_window),
    )

    updates = (
        ranked_segments
        .join(ranked_stores, "store_rank", "inner")
        .select(
            "store_id",
            "road_segment_id",
            F.lit("generated_persistent_round_robin").alias(
                "mapping_method"
            ),
            F.current_timestamp().alias("mapping_created_time"),
        )
    )

    append_new_mappings(updates, ROAD_MAP_TABLE, exists)
    mapping = spark.table(ROAD_MAP_TABLE).cache()

    total = mapping.count()
    if mapping.select("road_segment_id").distinct().count() != total:
        raise ValueError("A road segment has multiple store mappings.")

    return mapping


def build_traffic_enriched(
    spark: SparkSession,
    road_segment_map: DataFrame,
) -> DataFrame:
    return (
        spark.table("lake.silver.traffic_clean").alias("traffic")
        .join(
            road_segment_map.alias("mapping"),
            F.col("traffic.road_segment_id")
            == F.col("mapping.road_segment_id"),
            "left",
        )
        .select(
            "traffic.*",
            F.col("mapping.store_id"),
            F.col("mapping.mapping_method").alias(
                "store_mapping_method"
            ),
            F.current_timestamp().alias("conformed_processed_time"),
        )
    )


def require_complete_mapping(
    dataframe: DataFrame,
    source_table: str,
) -> None:
    unmapped_count = dataframe.filter(F.col("store_id").isNull()).count()
    if unmapped_count > 0:
        raise ValueError(
            f"{source_table} contains {unmapped_count} unmapped records."
        )


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("build-silver-conformed")
        .getOrCreate()
    )
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.silver")

    try:
        store_ids = load_store_ids(spark).cache()
        review_locations = load_review_locations(spark).cache()

        if store_ids.count() == 0:
            raise ValueError("No store IDs found in orders_clean.")

        review_store_map = update_review_store_map(
            spark,
            store_ids,
            review_locations,
        )
        store_reference = build_store_reference(
            store_ids,
            review_locations,
            review_store_map,
        )
        reviews_enriched = build_reviews_enriched(
            spark,
            review_store_map,
        ).cache()

        road_segment_map = update_road_segment_map(
            spark,
            store_ids,
        )
        traffic_enriched = build_traffic_enriched(
            spark,
            road_segment_map,
        ).cache()

        require_complete_mapping(reviews_enriched, "reviews_enriched")
        require_complete_mapping(traffic_enriched, "traffic_enriched")

        replace_table(
            store_reference,
            "lake.silver.store_reference",
        )
        replace_table(
            reviews_enriched,
            "lake.silver.reviews_enriched",
        )
        replace_table(
            traffic_enriched,
            "lake.silver.traffic_enriched",
        )

        traffic_enriched.unpersist()
        road_segment_map.unpersist()
        reviews_enriched.unpersist()
        review_store_map.unpersist()
        review_locations.unpersist()
        store_ids.unpersist()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
