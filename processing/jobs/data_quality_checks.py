from pyspark.sql import SparkSession


REQUIRED_TABLES = [
    "lake.bronze.reviews_raw",
    "lake.bronze.traffic_raw",
    "lake.bronze.orders_stream_raw",
    "lake.silver.reviews_clean",
    "lake.silver.reviews_rejected",
    "lake.silver.traffic_clean",
    "lake.silver.traffic_rejected",
    "lake.silver.orders_clean",
    "lake.silver.orders_rejected",
    "lake.silver.store_reference",
    "lake.silver.reviews_enriched",
    "lake.silver.traffic_enriched",
    "lake.gold.dim_date",
    "lake.gold.dim_road_segment",
    "lake.gold.dim_store",
    "lake.gold.fact_orders",
    "lake.gold.fact_customer_reviews",
    "lake.gold.fact_traffic_observations",
    "lake.gold.agg_store_performance",
]


CHECKS = [
    (
        "reviews Bronze-to-Silver reconciliation",
        """
        SELECT ABS(
            (SELECT COUNT(*) FROM lake.bronze.reviews_raw)
            -
            (
                (SELECT COUNT(*) FROM lake.silver.reviews_clean)
                +
                (SELECT COUNT(*) FROM lake.silver.reviews_rejected)
            )
        ) AS violations
        """,
    ),
    (
        "traffic Bronze-to-Silver reconciliation",
        """
        SELECT ABS(
            (SELECT COUNT(*) FROM lake.bronze.traffic_raw)
            -
            (
                (SELECT COUNT(*) FROM lake.silver.traffic_clean)
                +
                (SELECT COUNT(*) FROM lake.silver.traffic_rejected)
            )
        ) AS violations
        """,
    ),
    (
        "orders Bronze-to-Silver reconciliation",
        """
        SELECT ABS(
            (SELECT COUNT(*) FROM lake.bronze.orders_stream_raw)
            -
            (
                (SELECT COUNT(*) FROM lake.silver.orders_clean)
                +
                (SELECT COUNT(*) FROM lake.silver.orders_rejected)
            )
        ) AS violations
        """,
    ),
    (
        "Silver conformed row reconciliation",
        """
        SELECT
            ABS(
                (SELECT COUNT(*) FROM lake.silver.reviews_clean)
                -
                (SELECT COUNT(*) FROM lake.silver.reviews_enriched)
            )
            +
            ABS(
                (SELECT COUNT(*) FROM lake.silver.traffic_clean)
                -
                (SELECT COUNT(*) FROM lake.silver.traffic_enriched)
            ) AS violations
        """,
    ),
    (
        "Gold fact row reconciliation",
        """
        SELECT
            ABS(
                (SELECT COUNT(*) FROM lake.silver.orders_clean)
                -
                (SELECT COUNT(*) FROM lake.gold.fact_orders)
            )
            +
            ABS(
                (SELECT COUNT(*) FROM lake.silver.reviews_enriched)
                -
                (
                    SELECT COUNT(*)
                    FROM lake.gold.fact_customer_reviews
                )
            )
            +
            ABS(
                (SELECT COUNT(*) FROM lake.silver.traffic_enriched)
                -
                (
                    SELECT COUNT(*)
                    FROM lake.gold.fact_traffic_observations
                )
            ) AS violations
        """,
    ),
    (
        "date dimension continuity and uniqueness",
        """
        SELECT
            ABS(
                DATEDIFF(
                    MAX(calendar_date),
                    MIN(calendar_date)
                ) + 1 - COUNT(*)
            )
            +
            (
                COUNT(*)
                - COUNT(DISTINCT date_key)
            ) AS violations
        FROM lake.gold.dim_date
        """,
    ),
    (
        "road dimension keys",
        """
        SELECT
            SUM(
                CASE
                WHEN road_segment_key IS NULL
                  OR road_segment_id IS NULL
                  OR latitude IS NULL
                  OR longitude IS NULL
                    THEN 1
                    ELSE 0
                END
            )
            +
            (
                COUNT(*)
                - COUNT(DISTINCT road_segment_key)
            )
            +
            (
                COUNT(*)
                - COUNT(DISTINCT road_segment_id)
            ) AS violations
        FROM lake.gold.dim_road_segment
        """,
    ),
    (
        "store dimension keys and periods",
        """
        SELECT COUNT(*) AS violations
        FROM lake.gold.dim_store
        WHERE store_key IS NULL
           OR store_id IS NULL
           OR store_name IS NULL
           OR store_profile_source IS NULL
           OR valid_from IS NULL
           OR valid_to IS NULL
           OR is_current IS NULL
           OR valid_from >= valid_to
        """,
    ),
    (
        "one current SCD2 version per store",
        """
        SELECT COUNT(*) AS violations
        FROM (
            SELECT store_id
            FROM lake.gold.dim_store
            GROUP BY store_id
            HAVING SUM(
                CASE WHEN is_current THEN 1 ELSE 0 END
            ) <> 1
        )
        """,
    ),
    (
        "SCD2 period overlap",
        """
        SELECT COUNT(*) AS violations
        FROM lake.gold.dim_store AS first_version
        JOIN lake.gold.dim_store AS second_version
          ON first_version.store_id = second_version.store_id
         AND first_version.store_key < second_version.store_key
         AND first_version.valid_from < second_version.valid_to
         AND second_version.valid_from < first_version.valid_to
        """,
    ),
    (
        "current SCD2 open-end date",
        """
        SELECT COUNT(*) AS violations
        FROM lake.gold.dim_store
        WHERE is_current
          AND valid_to <> TIMESTAMP '9999-12-31 00:00:00'
        """,
    ),
    (
        "order fact primary keys",
        """
        SELECT
            SUM(
                CASE
                    WHEN order_key IS NULL
                      OR store_key IS NULL
                      OR date_key IS NULL
                    THEN 1
                    ELSE 0
                END
            )
            +
            (
                COUNT(*)
                - COUNT(DISTINCT order_key)
            ) AS violations
        FROM lake.gold.fact_orders
        """,
    ),
    (
        "review fact primary keys",
        """
        SELECT
            SUM(
                CASE
                    WHEN review_key IS NULL
                      OR store_key IS NULL
                      OR date_key IS NULL
                    THEN 1
                    ELSE 0
                END
            )
            +
            (
                COUNT(*)
                - COUNT(DISTINCT review_key)
            ) AS violations
        FROM lake.gold.fact_customer_reviews
        """,
    ),
    (
        "traffic fact primary keys",
        """
        SELECT
            SUM(
                CASE
                    WHEN traffic_observation_key IS NULL
                      OR store_key IS NULL
                      OR road_segment_key IS NULL
                      OR date_key IS NULL
                    THEN 1
                    ELSE 0
                END
            )
            +
            (
                COUNT(*)
                - COUNT(DISTINCT traffic_observation_key)
            ) AS violations
        FROM lake.gold.fact_traffic_observations
        """,
    ),
    (
        "order fact foreign keys",
        """
        SELECT
            (
                SELECT COUNT(*)
                FROM lake.gold.fact_orders AS fact
                LEFT ANTI JOIN lake.gold.dim_store AS dimension
                  ON fact.store_key = dimension.store_key
            )
            +
            (
                SELECT COUNT(*)
                FROM lake.gold.fact_orders AS fact
                LEFT ANTI JOIN lake.gold.dim_date AS dimension
                  ON fact.date_key = dimension.date_key
            ) AS violations
        """,
    ),
    (
        "review fact foreign keys",
        """
        SELECT
            (
                SELECT COUNT(*)
                FROM lake.gold.fact_customer_reviews AS fact
                LEFT ANTI JOIN lake.gold.dim_store AS dimension
                  ON fact.store_key = dimension.store_key
            )
            +
            (
                SELECT COUNT(*)
                FROM lake.gold.fact_customer_reviews AS fact
                LEFT ANTI JOIN lake.gold.dim_date AS dimension
                  ON fact.date_key = dimension.date_key
            ) AS violations
        """,
    ),
    (
        "traffic fact foreign keys",
        """
        SELECT
            (
                SELECT COUNT(*)
                FROM lake.gold.fact_traffic_observations AS fact
                LEFT ANTI JOIN lake.gold.dim_store AS dimension
                  ON fact.store_key = dimension.store_key
            )
            +
            (
                SELECT COUNT(*)
                FROM lake.gold.fact_traffic_observations AS fact
                LEFT ANTI JOIN lake.gold.dim_road_segment AS dimension
                  ON fact.road_segment_key =
                     dimension.road_segment_key
            )
            +
            (
                SELECT COUNT(*)
                FROM lake.gold.fact_traffic_observations AS fact
                LEFT ANTI JOIN lake.gold.dim_date AS dimension
                  ON fact.date_key = dimension.date_key
            ) AS violations
        """,
    ),
    (
        "order value ranges",
        """
        SELECT COUNT(*) AS violations
        FROM lake.gold.fact_orders
        WHERE order_value IS NULL
           OR order_value <= 0
           OR customer_rating IS NULL
           OR customer_rating NOT BETWEEN 1 AND 5
        """,
    ),
    (
        "review rating ranges",
        """
        SELECT COUNT(*) AS violations
        FROM lake.gold.fact_customer_reviews
        WHERE rating IS NULL
           OR rating NOT BETWEEN 1 AND 5
        """,
    ),
    (
        "traffic value ranges",
        """
        SELECT COUNT(*) AS violations
        FROM lake.gold.fact_traffic_observations
        WHERE vehicle_count IS NULL
           OR vehicle_count < 0
           OR average_speed IS NULL
           OR average_speed < 0
           OR lane_occupancy_rate IS NULL
           OR lane_occupancy_rate < 0
           OR jam_density_index IS NULL
           OR jam_density_index < 0
        """,
    ),
    (
        "late-arriving order range",
        """
        SELECT COUNT(*) AS violations
        FROM lake.silver.orders_clean
        WHERE lateness_hours IS NULL
           OR lateness_hours < 0
           OR lateness_hours > 48
        """,
    ),
    (
        "aggregate store uniqueness",
        """
        SELECT
            SUM(
                CASE
                    WHEN store_key IS NULL
                      OR store_id IS NULL
                    THEN 1
                    ELSE 0
                END
            )
            +
            (
                COUNT(*)
                - COUNT(DISTINCT store_id)
            ) AS violations
        FROM lake.gold.agg_store_performance
        """,
    ),
    (
        "aggregate current-store coverage",
        """
        SELECT ABS(
            (
                SELECT COUNT(*)
                FROM lake.gold.dim_store
                WHERE is_current
            )
            -
            (
                SELECT COUNT(*)
                FROM lake.gold.agg_store_performance
            )
        ) AS violations
        """,
    ),
    (
        "aggregate rate ranges",
        """
        SELECT COUNT(*) AS violations
        FROM lake.gold.agg_store_performance
        WHERE (
            order_count > 0
            AND (
                delayed_order_rate IS NULL
                OR delayed_order_rate NOT BETWEEN 0 AND 1
            )
        )
        OR (
            order_count = 0
            AND delayed_order_rate IS NOT NULL
        )
        OR (
            review_count > 0
            AND (
                negative_review_rate IS NULL
                OR negative_review_rate NOT BETWEEN 0 AND 1
            )
        )
        OR (
            review_count = 0
            AND negative_review_rate IS NOT NULL
        )
        OR (
            traffic_observation_count > 0
            AND (
                traffic_anomaly_rate IS NULL
                OR traffic_anomaly_rate NOT BETWEEN 0 AND 1
            )
        )
        OR (
            traffic_observation_count = 0
            AND traffic_anomaly_rate IS NOT NULL
        )
        """,
    ),
    (
        "aggregate-to-fact reconciliation",
        """
        SELECT
            ABS(
                aggregate_totals.orders
                - fact_totals.orders
            )
            +
            ABS(
                aggregate_totals.reviews
                - fact_totals.reviews
            )
            +
            ABS(
                aggregate_totals.traffic
                - fact_totals.traffic
            ) AS violations
        FROM (
            SELECT
                SUM(order_count) AS orders,
                SUM(review_count) AS reviews,
                SUM(traffic_observation_count) AS traffic
            FROM lake.gold.agg_store_performance
        ) AS aggregate_totals
        CROSS JOIN (
            SELECT
                (
                    SELECT COUNT(*)
                    FROM lake.gold.fact_orders
                ) AS orders,
                (
                    SELECT COUNT(*)
                    FROM lake.gold.fact_customer_reviews
                ) AS reviews,
                (
                    SELECT COUNT(*)
                    FROM lake.gold.fact_traffic_observations
                ) AS traffic
        ) AS fact_totals
        """,
    ),
]


def validate_required_tables(
    spark: SparkSession,
) -> None:
    missing_tables = [
        table_name
        for table_name in REQUIRED_TABLES
        if not spark.catalog.tableExists(table_name)
    ]

    if missing_tables:
        raise ValueError(
            "Missing required tables: "
            + ", ".join(missing_tables)
        )


def run_checks(
    spark: SparkSession,
) -> None:
    failures = []

    for check_name, query in CHECKS:
        result = spark.sql(query).first()
        raw_violations = result["violations"]

        if raw_violations is None:
            print(
                f"[FAIL] {check_name}: "
                "check returned NULL"
            )
            failures.append(
                f"{check_name} (NULL result)"
            )
            continue

        violations = int(raw_violations)

        if violations == 0:
            print(f"[PASS] {check_name}")
        else:
            print(
                f"[FAIL] {check_name}: "
                f"{violations} violation(s)"
            )
            failures.append(
                f"{check_name} ({violations})"
            )

    if failures:
        raise ValueError(
            "Data quality checks failed: "
            + ", ".join(failures)
        )

    print(
        f"All {len(CHECKS)} data quality checks passed."
    )


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("data-quality-checks")
        .getOrCreate()
    )

    spark.conf.set(
        "spark.sql.session.timeZone",
        "UTC",
    )

    try:
        validate_required_tables(spark)
        run_checks(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
    