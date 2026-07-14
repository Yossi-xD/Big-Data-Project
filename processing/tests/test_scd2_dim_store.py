from datetime import datetime, timezone
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


sys.path.insert(0, "/opt/processing/jobs")

from scd2_dim_store import (  # noqa: E402
    build_new_versions,
    build_source,
    close_changed_versions,
    find_changes,
)


TEST_SUFFIX = " [SCD2 TEST]"


def assert_equal(
    actual,
    expected,
    message: str,
) -> None:
    if actual != expected:
        raise AssertionError(
            f"{message}: expected={expected}, actual={actual}"
        )


def count_overlapping_periods(
    dimension,
) -> int:
    first_version = dimension.alias("first_version")
    second_version = dimension.alias("second_version")

    return (
        first_version
        .join(
            second_version,
            (
                F.col("first_version.store_id")
                == F.col("second_version.store_id")
            )
            & (
                F.col("first_version.store_key")
                < F.col("second_version.store_key")
            )
            & (
                F.col("first_version.valid_from")
                < F.col("second_version.valid_to")
            )
            & (
                F.col("second_version.valid_from")
                < F.col("first_version.valid_to")
            ),
            "inner",
        )
        .count()
    )


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("test-scd2-dim-store")
        .getOrCreate()
    )

    spark.conf.set(
        "spark.sql.session.timeZone",
        "UTC",
    )
    spark.sparkContext.setLogLevel("WARN")

    existing = None
    changes = None
    simulated_dimension = None

    try:
        existing = (
            spark.table("lake.gold.dim_store")
            .cache()
        )

        existing_row_count = existing.count()
        existing_current_count = (
            existing
            .filter(F.col("is_current"))
            .count()
        )

        selected_store = (
            existing
            .filter(F.col("is_current"))
            .orderBy(
                F.col("store_id").cast("int"),
                F.col("store_id"),
            )
            .select("store_id")
            .first()
        )

        if selected_store is None:
            raise AssertionError(
                "dim_store has no current store version."
            )

        test_store_id = selected_store["store_id"]

        source = build_source(spark)

        simulated_source = source.withColumn(
            "store_name",
            F.when(
                F.col("store_id") == test_store_id,
                F.concat(
                    F.coalesce(
                        F.col("store_name"),
                        F.lit("Unnamed Store"),
                    ),
                    F.lit(TEST_SUFFIX),
                ),
            ).otherwise(F.col("store_name")),
        )

        current_dimension = existing.filter(
            F.col("is_current")
        )

        changes = (
            find_changes(
                simulated_source,
                current_dimension,
            )
            .cache()
        )

        change_count = changes.count()
        changed_store_ids = (
            changes
            .select("store_id")
            .distinct()
            .count()
        )

        assert_equal(
            change_count,
            1,
            "SCD2 should detect exactly one changed row",
        )
        assert_equal(
            changed_store_ids,
            1,
            "SCD2 should detect exactly one changed store",
        )

        detected_store_id = (
            changes
            .select("store_id")
            .first()["store_id"]
        )

        assert_equal(
            detected_store_id,
            test_store_id,
            "SCD2 detected the wrong store",
        )

        run_time = (
            datetime.now(timezone.utc)
            .replace(tzinfo=None)
        )

        updated_history = close_changed_versions(
            existing,
            changes,
            run_time,
        )

        new_versions = build_new_versions(
            changes,
            run_time,
        )

        simulated_dimension = (
            updated_history
            .unionByName(new_versions)
            .cache()
        )

        simulated_row_count = simulated_dimension.count()
        simulated_current_count = (
            simulated_dimension
            .filter(F.col("is_current"))
            .count()
        )

        selected_store_versions = (
            simulated_dimension
            .filter(F.col("store_id") == test_store_id)
        )

        selected_current_count = (
            selected_store_versions
            .filter(F.col("is_current"))
            .count()
        )

        selected_new_version_count = (
            selected_store_versions
            .filter(
                F.col("is_current")
                & F.col("store_name").endswith(TEST_SUFFIX)
                & (
                    F.col("valid_from")
                    == F.lit(run_time).cast("timestamp")
                )
            )
            .count()
        )

        selected_closed_version_count = (
            selected_store_versions
            .filter(
                ~F.col("is_current")
                & (
                    F.col("valid_to")
                    == F.lit(run_time).cast("timestamp")
                )
            )
            .count()
        )

        invalid_period_count = (
            simulated_dimension
            .filter(
                F.col("valid_from").isNull()
                | F.col("valid_to").isNull()
                | (
                    F.col("valid_from")
                    >= F.col("valid_to")
                )
            )
            .count()
        )

        assert_equal(
            simulated_row_count,
            existing_row_count + 1,
            "A change should add exactly one SCD2 version",
        )
        assert_equal(
            simulated_current_count,
            existing_current_count,
            "The number of current stores must remain unchanged",
        )
        assert_equal(
            selected_current_count,
            1,
            "The changed store must have one current version",
        )
        assert_equal(
            selected_new_version_count,
            1,
            "The changed store must have one new current version",
        )
        assert_equal(
            selected_closed_version_count,
            1,
            "The previous current version must be closed",
        )
        assert_equal(
            invalid_period_count,
            0,
            "SCD2 periods must remain valid",
        )
        assert_equal(
            count_overlapping_periods(simulated_dimension),
            0,
            "SCD2 periods must not overlap",
        )

        production_row_count_after_test = (
            spark.table("lake.gold.dim_store").count()
        )
        production_current_count_after_test = (
            spark.table("lake.gold.dim_store")
            .filter(F.col("is_current"))
            .count()
        )

        assert_equal(
            production_row_count_after_test,
            existing_row_count,
            "The test must not modify production row count",
        )
        assert_equal(
            production_current_count_after_test,
            existing_current_count,
            "The test must not modify production current versions",
        )

        print(
            "[PASS] SCD2 detected one simulated change, "
            "closed the previous version, created one new "
            "version, preserved valid periods, and did not "
            "modify the production table."
        )

    finally:
        if simulated_dimension is not None:
            simulated_dimension.unpersist()
        if changes is not None:
            changes.unpersist()
        if existing is not None:
            existing.unpersist()

        spark.stop()


if __name__ == "__main__":
    main()
    