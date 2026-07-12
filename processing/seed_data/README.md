# `/processing/seed_data` -- raw batch source landing

The raw batch source files live here (Tama's): the McDonald's Store Reviews
dataset and the Traffic dataset (TrafficTab23), which replaced the originally
planned Stores dataset per the lecturer's feedback on the mid-term submission:

```
seed_data/
  reviews.csv          # McDonald's Store Reviews dataset (full)
  traffic_sample.csv   # reproducible 20,000-row random sample of the full
                       # 571MB TrafficTab23 dataset -- regenerate the exact
                       # same sample (seed 42) with
                       # processing/data_prep/create_traffic_sample.py
```

Anything placed here is automatically uploaded into MinIO's `landing` bucket
(`s3://landing/<same relative path>`) by the one-time `mc-init` service the
first time `processing/docker-compose.yml` comes up (see `../mc-init.sh`).
Nothing needs rebuilding for this -- it's a bind mount, not baked into an
image -- but `mc-init` only runs once per fresh MinIO volume, so after adding
files here for the first time, either re-run it directly or `docker compose
-f processing/docker-compose.yml up -d --force-recreate mc-init`.

Batch jobs (`batch_to_bronze.py`) should read from there via the same S3A
config already set up for Spark, e.g.:

```python
reviews_df = spark.read.option("header", True).csv("s3a://landing/reviews.csv")
traffic_df = spark.read.option("header", True).csv("s3a://landing/traffic_sample.csv")
```

## Why this dataset needs late-arrival handling (and why it's here, not Kafka)

Per the team's own architecture (`McDonald's analysis.pdf`, Data Sources
Overview): the **Reviews** dataset is the "Late Arrival Source" -- a review's
`review_time` (when the customer actually left it) can be hours-to-days
before the batch file containing it gets ingested (`ingestion_time`). That's
a batch-with-delay pattern, not a Kafka stream, which is why reviews land
here rather than going through `/streaming`. The 48-hour late-arrival
handling requirement (watermarking in `bronze_to_silver.py`) should key off
these two timestamps for this dataset.

The Kafka `orders_stream` producer (see `/streaming/producer`) *also*
occasionally backdates `event_time` as a secondary demonstration of
late-arrival handling on the streaming side, but per the team's own design
this dataset -- Reviews -- is the primary, required late-arrival source.

Traffic is plain static/batch context data (no late-arrival dimension). There
is no external Stores file: `dim_store` (SCD Type 2) is derived during
processing from the store IDs that link the three sources together.
