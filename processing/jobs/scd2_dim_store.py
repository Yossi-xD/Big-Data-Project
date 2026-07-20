from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


DIM_STORE = "lake.gold.dim_store"
OPEN_END = datetime(9999, 12, 31)

TRACKED_COLUMNS = [
    "store_name",
    "store_address",
    "latitude",
    "longitude",
    "store_profile_source",
]


def stable_store_key(
    store_id,
    valid_from,
):
    return F.sha2(
        F.concat_ws(
            "|",
            store_id.cast("string"),
            valid_from.cast("string"),
        ),
        256,
    )


def build_first_activity(
    spark: SparkSession,
) -> DataFrame:
    return (
        spark.table("lake.silver.orders_clean")
        .select(
            "store_id",
            F.col("order_date").alias("activity_date"),
        )
        .unionByName(
            spark.table("lake.silver.reviews_enriched")
            .select(
                "store_id",
                F.col("ingestion_date").alias(
                    "activity_date"
                ),
            )
        )
        .unionByName(
            spark.table("lake.silver.traffic_enriched")
            .select(
                "store_id",
                F.col("event_date").alias(
                    "activity_date"
                ),
            )
        )
        .filter(
            F.col("store_id").isNotNull()
            & F.col("activity_date").isNotNull()
        )
        .groupBy("store_id")
        .agg(
            F.min("activity_date")
            .cast("timestamp")
            .alias("first_activity_time")
        )
    )


def build_source(
    spark: SparkSession,
) -> DataFrame:
    source = (
        spark.table("lake.silver.store_reference")
        .select(
            "store_id",
            *TRACKED_COLUMNS,
        )
        .filter(F.col("store_id").isNotNull())
        .join(
            build_first_activity(spark),
            "store_id",
            "left",
        )
    )

    has_duplicates = (
        source
        .groupBy("store_id")
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
        > 0
    )

    if has_duplicates:
        raise ValueError(
            "store_reference contains duplicate store IDs."
        )

    return source


def add_version_columns(
    dataframe: DataFrame,
    valid_from,
    processed_time,
) -> DataFrame:
    return (
        dataframe
        .withColumn(
            "valid_from",
            valid_from,
        )
        .withColumn(
            "valid_to",
            F.lit(OPEN_END).cast("timestamp"),
        )
        .withColumn("is_current", F.lit(True))
        .withColumn(
            "store_key",
            stable_store_key(
                F.col("store_id"),
                F.col("valid_from"),
            ),
        )
        .withColumn(
            "gold_processed_time",
            processed_time,
        )
        .select(
            "store_key",
            "store_id",
            *TRACKED_COLUMNS,
            "valid_from",
            "valid_to",
            "is_current",
            "gold_processed_time",
        )
    )


def build_initial_dimension(
    source: DataFrame,
    run_time: datetime,
) -> DataFrame:
    return add_version_columns(
        source,
        F.coalesce(
            F.col("first_activity_time"),
            F.lit(run_time).cast("timestamp"),
        ),
        F.lit(run_time).cast("timestamp"),
    )


def attributes_match():
    comparisons = []

    for column_name in TRACKED_COLUMNS:
        source_column = F.col(f"source.{column_name}")
        current_column = F.col(f"current.{column_name}")

        if column_name in {"latitude", "longitude"}:
            source_column = F.bround(source_column, 6)
            current_column = F.bround(current_column, 6)

        comparisons.append(
            source_column.eqNullSafe(current_column)
        )

    condition = comparisons[0]

    for comparison in comparisons[1:]:
        condition = condition & comparison

    return condition


def find_changes(
    source: DataFrame,
    current_dimension: DataFrame,
) -> DataFrame:
    return (
        source.alias("source")
        .join(
            current_dimension.alias("current"),
            F.col("source.store_id")
            == F.col("current.store_id"),
            "left",
        )
        .filter(
            F.col("current.store_id").isNull()
            | ~attributes_match()
        )
        .select(
            "source.*",
            F.col("current.store_id").alias(
                "existing_store_id"
            ),
        )
    )


def validate_existing_dimension(
    existing: DataFrame,
) -> None:
    duplicate_current_versions = (
        existing
        .filter(F.col("is_current"))
        .groupBy("store_id")
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )

    invalid_periods = (
        existing
        .filter(
            F.col("valid_from").isNull()
            | F.col("valid_to").isNull()
            | (
                F.col("valid_from")
                >= F.col("valid_to")
            )
        )
        .limit(1)
        .count()
    )

    if duplicate_current_versions:
        raise ValueError(
            "dim_store contains multiple current "
            "versions for one store."
        )

    if invalid_periods:
        raise ValueError(
            "dim_store contains an invalid SCD2 period."
        )


def close_changed_versions(
    existing: DataFrame,
    changes: DataFrame,
    run_time: datetime,
) -> DataFrame:
    changed_stores = (
        changes
        .filter(F.col("existing_store_id").isNotNull())
        .select("store_id")
        .distinct()
        .withColumn("should_close", F.lit(True))
    )

    joined = existing.join(
        changed_stores,
        "store_id",
        "left",
    )

    closing_current_version = (
        F.col("is_current")
        & F.coalesce(
            F.col("should_close"),
            F.lit(False),
        )
    )

    return (
        joined
        .withColumn(
            "valid_to",
            F.when(
                closing_current_version,
                F.lit(run_time).cast("timestamp"),
            ).otherwise(F.col("valid_to")),
        )
        .withColumn(
            "is_current",
            F.when(
                closing_current_version,
                F.lit(False),
            ).otherwise(F.col("is_current")),
        )
        .withColumn(
            "gold_processed_time",
            F.when(
                closing_current_version,
                F.lit(run_time).cast("timestamp"),
            ).otherwise(F.col("gold_processed_time")),
        )
        .drop("should_close")
    )


def build_new_versions(
    changes: DataFrame,
    run_time: datetime,
) -> DataFrame:
    valid_from = F.when(
        F.col("existing_store_id").isNotNull(),
        F.lit(run_time).cast("timestamp"),
    ).otherwise(
        F.coalesce(
            F.col("first_activity_time"),
            F.lit(run_time).cast("timestamp"),
        )
    )

    return add_version_columns(
        changes,
        valid_from,
        F.lit(run_time).cast("timestamp"),
    )


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("scd2-dim-store")
        .getOrCreate()
    )

    spark.conf.set(
        "spark.sql.session.timeZone",
        "UTC",
    )

    spark.sql(
        "CREATE NAMESPACE IF NOT EXISTS lake.gold"
    )

    run_time = (
        datetime.now(timezone.utc)
        .replace(tzinfo=None)
    )

    try:
        source = build_source(spark)

        if not spark.catalog.tableExists(DIM_STORE):
            initial_dimension = (
                build_initial_dimension(
                    source,
                    run_time,
                )
            )

            (
                initial_dimension.writeTo(DIM_STORE)
                .using("iceberg")
                .create()
            )

            print(
                f"{DIM_STORE}: initial versions created."
            )
            return

        existing = (
            spark.table(DIM_STORE)
            .localCheckpoint(eager=True)
        )

        validate_existing_dimension(existing)

        current_dimension = existing.filter(
            F.col("is_current")
        )

        changes = (
            find_changes(
                source,
                current_dimension,
            )
            .cache()
        )

        change_count = changes.count()

        if change_count == 0:
            print(
                f"{DIM_STORE}: no attribute changes found."
            )
            changes.unpersist()
            return

        updated_history = close_changed_versions(
            existing,
            changes,
            run_time,
        )

        new_versions = build_new_versions(
            changes,
            run_time,
        )

        final_dimension = updated_history.unionByName(
            new_versions
        )

        (
            final_dimension.writeTo(DIM_STORE)
            .using("iceberg")
            .createOrReplace()
        )

        changes.unpersist()

        print(
            f"{DIM_STORE}: applied {change_count} "
            f"new or changed store versions."
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
