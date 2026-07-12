# `/processing/seed_data` -- raw batch source landing

Drop the raw batch source files here (Tama): the McDonald's Store Reviews
dataset and the McDonald's Stores dataset (see the links in `McDonald's
analysis.pdf` / `docs/data_model.md`), as CSV/JSON, e.g.:

```
seed_data/
  reviews.csv    # McDonald's Store Reviews dataset (raw, or trimmed to a
                 # demo-sized sample -- this is what gets committed to the
                 # repo and graded, so keep it small enough to check in)
  stores.csv     # McDonald's Stores dataset
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
stores_df = spark.read.option("header", True).csv("s3a://landing/stores.csv")
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

Stores is plain static/batch reference data (no late-arrival dimension) --
used for `dim_store` (SCD Type 2).
