# Data Model — Bronze / Silver / Gold

Generated directly from the Spark job definitions in `processing/jobs/` (not an
idealized model) -- every attribute below is a real column the corresponding
job writes. Catalog: `lake` (Iceberg REST). Namespaces map 1:1 to the sections
below (`lake.bronze`, `lake.silver`, `lake.gold`).

## Layer lineage

```mermaid
flowchart LR
    subgraph Sources
        reviewscsv["reviews.csv"]
        trafficcsv["traffic_sample.csv"]
        kafka[("Kafka topic\norders_stream")]
    end

    subgraph Bronze
        reviews_raw
        traffic_raw
        orders_stream_raw
    end

    subgraph Silver
        reviews_clean
        traffic_clean
        orders_clean
        store_reference
        review_store_map
        reviews_enriched
        store_road_segment_map
        traffic_enriched
    end

    subgraph Gold
        dim_date
        dim_store["dim_store (SCD2)"]
        dim_road_segment
        fact_orders
        fact_customer_reviews
        fact_traffic_observations
        agg_store_performance
    end

    reviewscsv -->|batch_to_bronze.py| reviews_raw
    trafficcsv -->|batch_to_bronze.py| traffic_raw
    kafka -->|stream_orders_to_bronze.py| orders_stream_raw

    reviews_raw -->|bronze_to_silver.py| reviews_clean
    traffic_raw -->|bronze_to_silver.py| traffic_clean
    orders_stream_raw -->|bronze_to_silver.py, parses raw_payload JSON| orders_clean

    orders_clean -->|build_silver_conformed.py| store_reference
    reviews_clean -->|build_silver_conformed.py| review_store_map
    review_store_map --> store_reference
    reviews_clean -->|+ store_id| reviews_enriched
    review_store_map --> reviews_enriched
    traffic_clean -->|build_silver_conformed.py| store_road_segment_map
    traffic_clean -->|+ store_id| traffic_enriched
    store_road_segment_map --> traffic_enriched

    orders_clean -->|build_gold_dimensions.py| dim_date
    reviews_enriched --> dim_date
    traffic_enriched --> dim_date
    traffic_enriched -->|build_gold_dimensions.py| dim_road_segment
    store_reference -->|scd2_dim_store.py| dim_store

    orders_clean -->|build_gold_facts.py| fact_orders
    reviews_enriched -->|build_gold_facts.py| fact_customer_reviews
    traffic_enriched -->|build_gold_facts.py| fact_traffic_observations
    dim_store --> fact_orders
    dim_store --> fact_customer_reviews
    dim_store --> fact_traffic_observations
    dim_road_segment --> fact_traffic_observations

    fact_orders -->|build_gold_aggregates.py| agg_store_performance
    fact_customer_reviews --> agg_store_performance
    fact_traffic_observations --> agg_store_performance
    dim_store --> agg_store_performance
```

Every clean/enriched/fact/dim table also gets `*_clean`/`*_rejected` (Silver)
or a `dq_status` column split via `bronze_to_silver.py`'s
`write_quality_results`, and 25 automated checks run across all three layers
in `processing/jobs/data_quality_checks.py` (see that file / `docs/setup.md`).

## Bronze layer

Raw landing tables. One row per source record, no transformation beyond
adding ingestion metadata. `reviews_raw`/`traffic_raw` keep every source
column verbatim as `string` (`inferSchema=False`); `orders_stream_raw` stores
the *entire* Kafka message as one JSON string (`raw_payload`) plus the Kafka
envelope -- parsing into typed fields happens only in Silver.

```mermaid
erDiagram
    reviews_raw {
        string reviewer_id
        string store_name
        string category
        string store_address
        string latitude "raw header has a trailing space"
        string longitude
        string rating_count "e.g. '1,240'"
        string review_time "relative text, e.g. '3 months ago'"
        string review
        string rating "e.g. '1 star'"
        timestamp ingestion_time
        date ingestion_date
        string source_file
        string source_system
    }

    traffic_raw {
        string timestamp
        string time_of_day
        string day_of_week
        string gps_latitude
        string gps_longitude
        string road_segment_id
        string distance_to_intersection
        string vehicle_count
        string average_speed
        string lane_occupancy_rate
        string jam_density_index
        string hard_braking_events
        string rapid_acceleration_events
        string lane_changes_per_minute
        string stop_duration_avg
        string weather_condition
        string visibility_range
        string road_surface_status
        string v2x_packet_loss_rate
        string v2v_beacon_interval_avg
        string v2x_message_delay_avg
        string anomaly_label
        string incident_type
        timestamp ingestion_time
        date ingestion_date
        string source_file
        string source_system
    }

    orders_stream_raw {
        string raw_payload "full Kafka JSON: order_id, restaurant_id, store_id, order_value, traffic_condition, ... every food_delivery_dataset.csv column, plus event_time/ingestion_time"
        string kafka_key
        string kafka_topic
        int kafka_partition
        bigint kafka_offset
        timestamp kafka_timestamp
        timestamp ingestion_time
        date ingestion_date
        string source_system
    }
```

## Silver layer

`bronze_to_silver.py` casts/normalizes each Bronze table and splits it into a
`_clean` and `_rejected` pair on the same schema plus `dq_status`
(`reviews_rejected`/`traffic_rejected`/`orders_rejected` are schema-identical
to their `_clean` counterpart, omitted below to avoid duplication).
`build_silver_conformed.py` then links all three sources, which don't share a
natural key, through two **persistent** mapping tables (new rows only ever
get appended, existing mappings are never regenerated).

```mermaid
erDiagram
    orders_clean ||--o{ store_reference : "store_id is the store universe"
    review_store_map ||--o{ store_reference : "store_address -> store_id"
    reviews_clean ||--o{ review_store_map : "store_address"
    reviews_clean ||--o{ reviews_enriched : "+ store_id"
    review_store_map ||--o{ reviews_enriched : "store_id"
    traffic_clean ||--o{ store_road_segment_map : "road_segment_id"
    traffic_clean ||--o{ traffic_enriched : "+ store_id"
    store_road_segment_map ||--o{ traffic_enriched : "store_id"

    reviews_clean {
        string reviewer_id
        string store_name
        string category
        string store_address
        double latitude
        double longitude
        long rating_count
        string review_time
        string review
        int rating "parsed 1-5 from 'N star(s)'"
        boolean has_coordinates
        timestamp silver_processed_time
        string dq_status "valid | rejected"
    }

    traffic_clean {
        string road_segment_id
        double gps_latitude
        double gps_longitude
        int vehicle_count
        double average_speed
        double lane_occupancy_rate
        double jam_density_index
        int hard_braking_events
        int rapid_acceleration_events
        double lane_changes_per_minute
        double stop_duration_avg
        string weather_condition
        double visibility_range
        string road_surface_status
        int anomaly_label
        string incident_type
        timestamp event_time
        date event_date
        timestamp silver_processed_time
        string dq_status "valid | rejected"
    }

    orders_clean {
        string order_id
        string restaurant_id
        string store_id "copy of restaurant_id"
        string food_item
        date order_date
        date delivery_date
        double delivery_distance
        double order_value
        string delivery_method
        string traffic_condition
        string weather_condition
        double delivery_delay
        int customer_rating
        double customer_satisfaction
        double route_efficiency
        timestamp event_time
        timestamp event_ingestion_time
        double lateness_hours "event_ingestion_time - event_time"
        boolean is_late "48h late-arrival window enforced in the valid predicate"
        timestamp silver_processed_time
        string dq_status "valid | rejected"
    }

    store_reference {
        string store_id PK
        string store_name
        string store_address
        double latitude
        double longitude
        string store_profile_source "reviews_enriched_profile | orders_only_profile"
        timestamp conformed_processed_time
    }

    review_store_map {
        string store_address PK
        string store_id UK "1:1, validated"
        string mapping_method
        timestamp mapping_created_time
    }

    store_road_segment_map {
        string road_segment_id PK
        string store_id "round-robin, many segments per store"
        string mapping_method
        timestamp mapping_created_time
    }

    reviews_enriched {
        string reviewer_id
        string store_address
        string store_id FK
        string store_mapping_method
        timestamp conformed_processed_time
    }

    traffic_enriched {
        string road_segment_id
        string store_id FK
        string store_mapping_method
        timestamp conformed_processed_time
    }
```

## Gold layer (star schema)

`dim_store` is a **Type 2 SCD** (`scd2_dim_store.py`): a change in
`store_name`/`store_address`/`latitude`/`longitude`/`store_profile_source`
closes the current version (`valid_to` = now, `is_current` = false) and opens
a new one; unchanged attributes append nothing. Open versions carry
`valid_to = 9999-12-31` (no `NULL` sentinel). Every fact row resolves its
`store_key` by joining on `store_id` **and** `event_time` falling inside
`[valid_from, valid_to)`, so a fact always points at the dimension version
that was active when the event happened.

```mermaid
erDiagram
    dim_date ||--o{ fact_orders : date_key
    dim_date ||--o{ fact_customer_reviews : date_key
    dim_date ||--o{ fact_traffic_observations : date_key
    dim_store ||--o{ fact_orders : store_key
    dim_store ||--o{ fact_customer_reviews : store_key
    dim_store ||--o{ fact_traffic_observations : store_key
    dim_store ||--o{ agg_store_performance : store_key
    dim_road_segment ||--o{ fact_traffic_observations : road_segment_key

    dim_date {
        int date_key PK "yyyyMMdd"
        date calendar_date
        int year
        int quarter
        int month
        string month_name
        int day_of_month
        int day_of_week
        string day_name
        boolean is_weekend
    }

    dim_store {
        string store_key PK "sha2(store_id, valid_from)"
        string store_id "business key, repeats across versions"
        string store_name
        string store_address
        double latitude
        double longitude
        string store_profile_source
        timestamp valid_from
        timestamp valid_to "9999-12-31 = open version"
        boolean is_current
        timestamp gold_processed_time
    }

    dim_road_segment {
        string road_segment_key PK "sha2(road_segment_id)"
        string road_segment_id "business key"
        double latitude "avg gps_latitude"
        double longitude "avg gps_longitude"
    }

    fact_orders {
        string order_key PK "sha2(order_id)"
        string order_id
        string store_key FK
        int date_key FK
        string customer_id
        string food_item
        double order_value
        double delivery_distance
        double delivery_delay
        string delivery_method
        int customer_rating
        double customer_satisfaction
        double route_efficiency
        string traffic_condition
        string weather_condition
        boolean is_delayed_delivery
        boolean is_late_arriving
        timestamp event_time
        timestamp event_ingestion_time
    }

    fact_customer_reviews {
        string review_key PK "sha2(reviewer_id)"
        string reviewer_id
        string store_key FK
        int date_key FK
        int rating
        string review
        string review_time
        boolean has_coordinates
    }

    fact_traffic_observations {
        string traffic_observation_key PK "sha2 of segment+time+metrics"
        string store_key FK
        string road_segment_key FK
        int date_key FK
        timestamp event_time
        int vehicle_count
        double average_speed
        double lane_occupancy_rate
        double jam_density_index
        int hard_braking_events
        int rapid_acceleration_events
        string weather_condition
        string road_surface_status
        string incident_type
        int anomaly_label
    }

    agg_store_performance {
        string store_key PK "= dim_store.store_key, current version only"
        string store_id
        int order_count
        int unique_customers
        double total_revenue
        double average_order_value
        double delayed_order_rate
        double average_customer_rating
        int late_arriving_order_count
        int review_count
        double average_review_rating
        double negative_review_rate
        int traffic_observation_count
        double average_traffic_speed
        double traffic_anomaly_rate
        timestamp gold_processed_time
    }
```

`build_gold_facts.py` validates every fact table on write: no null primary or
foreign keys, no duplicate primary keys. `scd2_dim_store.py` validates
`dim_store` on every run: at most one `is_current` version per `store_id`,
and no overlapping/invalid `[valid_from, valid_to)` periods.
