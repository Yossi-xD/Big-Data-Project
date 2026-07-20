import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


TABLE_NAME = "lake.bronze.orders_stream_raw"
KAFKA_SERVERS = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:9092",
)
KAFKA_TOPIC = os.environ.get(
    "KAFKA_TOPIC",
    "orders_stream",
)
CHECKPOINT_LOCATION = os.environ.get(
    "CHECKPOINT_LOCATION",
    "s3a://warehouse/checkpoints/orders_stream_raw",
)
TRIGGER_MODE = os.environ.get(
    "STREAM_TRIGGER_MODE",
    "available_now",
)


def create_bronze_table(spark: SparkSession) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.bronze")

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            raw_payload STRING,
            kafka_key STRING,
            kafka_topic STRING,
            kafka_partition INT,
            kafka_offset BIGINT,
            kafka_timestamp TIMESTAMP,
            ingestion_time TIMESTAMP,
            ingestion_date DATE,
            source_system STRING
        )
        USING iceberg
        PARTITIONED BY (ingestion_date)
        """
    )


def append_new_messages(
    spark: SparkSession,
    batch_dataframe: DataFrame,
    batch_id: int,
) -> None:
    if batch_dataframe.rdd.isEmpty():
        print(f"Batch {batch_id}: no messages.")
        return

    existing_offsets = (
        spark.table(TABLE_NAME)
        .select(
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
        )
    )

    new_messages = batch_dataframe.join(
        existing_offsets,
        on=[
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
        ],
        how="left_anti",
    ).cache()

    new_count = new_messages.count()

    if new_count > 0:
        new_messages.writeTo(TABLE_NAME).append()

    new_messages.unpersist()

    print(f"Batch {batch_id}: wrote {new_count} new messages.")


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("stream-orders-to-bronze")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    create_bronze_table(spark)

    kafka_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    bronze_stream = kafka_stream.select(
        F.col("value").cast("string").alias("raw_payload"),
        F.col("key").cast("string").alias("kafka_key"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.current_timestamp().alias("ingestion_time"),
        F.current_date().alias("ingestion_date"),
        F.lit("kafka_orders_stream").alias("source_system"),
    )

    writer = (
        bronze_stream.writeStream
        .foreachBatch(
            lambda dataframe, batch_id: append_new_messages(
                spark,
                dataframe,
                batch_id,
            )
        )
        .option("checkpointLocation", CHECKPOINT_LOCATION)
    )

    if TRIGGER_MODE == "continuous":
        query = writer.trigger(processingTime="10 seconds").start()
    else:
        query = writer.trigger(availableNow=True).start()

    query.awaitTermination()
    spark.stop()


if __name__ == "__main__":
    main()