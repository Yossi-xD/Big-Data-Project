"""Streams the real Food Delivery dataset onto Kafka as orders_stream events.

Replays data/food_delivery_dataset.csv (committed with the repo: 20,000 real
orders across 100 restaurants) row by row, mapped onto the team's data contract:

    order_id, store_id, order_value, delivery_duration, traffic_condition,
    event_time, ingestion_time

order_id, store_id (the dataset's restaurant_id), order_value and
traffic_condition come straight from the dataset. The dataset's order/delivery
times are date-only, so no real duration can be derived from it and
delivery_duration is synthesized. event_time / ingestion_time are stamped at
emit time: a configurable fraction of events is emitted with a backdated
event_time (up to MAX_LATE_HOURS in the past) to exercise late-arrival handling
in the silver layer, per the assignment's "up to 48 hours after event time"
requirement.

When the file is exhausted the producer starts over from the top; order_ids in
replay cycles get a "_R<cycle>" suffix so they stay unique across cycles.
"""

import csv
import json
import logging
import os
import random
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("orders_producer")

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "orders_stream")
MESSAGES_PER_SECOND = float(os.environ.get("MESSAGES_PER_SECOND", "2"))
LATE_RATIO = float(os.environ.get("LATE_RATIO", "0.15"))
MAX_LATE_HOURS = float(os.environ.get("MAX_LATE_HOURS", "48"))
DATASET_PATH = Path(os.environ.get("DATASET_PATH", "data/food_delivery_dataset.csv"))


def load_orders(dataset_path: Path) -> list[dict]:
    with dataset_path.open("r", encoding="utf-8-sig", newline="") as dataset_file:
        return list(csv.DictReader(dataset_file))


def build_event(row: dict, cycle: int) -> dict:
    now = datetime.now(timezone.utc)

    if random.random() < LATE_RATIO:
        delay_hours = random.uniform(0.5, MAX_LATE_HOURS)
        event_time = now - timedelta(hours=delay_hours)
    else:
        event_time = now

    order_id = row["order_id"] if cycle == 0 else f"{row['order_id']}_R{cycle}"

    return {
        "order_id": order_id,
        "store_id": row["restaurant_id"],
        "order_value": float(row["order_value"]),
        # the dataset's order/delivery times are date-only, so an actual
        # duration cannot be derived from it
        "delivery_duration": random.randint(10, 65),
        "traffic_condition": row["traffic_condition"],
        "event_time": event_time.isoformat(),
        "ingestion_time": now.isoformat(),
    }


def delivery_report(err, msg) -> None:
    if err is not None:
        log.error("delivery failed for key=%s: %s", msg.key(), err)


def main() -> None:
    orders = load_orders(DATASET_PATH)
    log.info("loaded %d orders from %s", len(orders), DATASET_PATH)

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "client.id": "orders-producer",
    })

    log.info(
        "producing to topic=%s at ~%.2f msg/s (late_ratio=%.2f, max_late_hours=%.1f)",
        TOPIC, MESSAGES_PER_SECOND, LATE_RATIO, MAX_LATE_HOURS,
    )

    sleep_s = 1.0 / MESSAGES_PER_SECOND if MESSAGES_PER_SECOND > 0 else 1.0

    cycle = 0
    try:
        while True:
            for row in orders:
                event = build_event(row, cycle)
                producer.produce(
                    TOPIC,
                    key=event["order_id"],
                    value=json.dumps(event),
                    callback=delivery_report,
                )
                producer.poll(0)
                log.info("sent %s", event)
                time.sleep(sleep_s)

            cycle += 1
            log.info("dataset exhausted -- replaying from the top (cycle %d)", cycle)
    except KeyboardInterrupt:
        log.info("shutting down producer")
    finally:
        producer.flush(10)


if __name__ == "__main__":
    main()
