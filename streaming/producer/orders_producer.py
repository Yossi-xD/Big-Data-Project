"""Streams food-delivery dataset rows to Kafka as order events."""

import csv
import json
import logging
import os
import random
import signal
import time
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("orders_producer")

BOOTSTRAP_SERVERS = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:9092",
)
TOPIC = os.environ.get(
    "KAFKA_TOPIC",
    "orders_stream",
)
CSV_PATH = os.environ.get(
    "ORDERS_CSV_PATH",
    "/app/data/food_delivery_dataset.csv",
)
MESSAGES_PER_SECOND = float(
    os.environ.get("MESSAGES_PER_SECOND", "2")
)
LATE_RATIO = float(
    os.environ.get("LATE_RATIO", "0.15")
)
MAX_LATE_HOURS = float(
    os.environ.get("MAX_LATE_HOURS", "48")
)

running = True


def request_shutdown(signum, frame) -> None:
    global running
    running = False
    log.info("shutdown requested")


def load_orders() -> list:
    with open(
        CSV_PATH,
        newline="",
        encoding="utf-8-sig",
    ) as file:
        orders = list(csv.DictReader(file))

    if not orders:
        raise ValueError(f"No orders found in {CSV_PATH}")

    return orders


def build_event(source_order: dict) -> dict:
    now = datetime.now(timezone.utc)

    if random.random() < LATE_RATIO:
        delay_hours = random.uniform(0.5, MAX_LATE_HOURS)
        event_time = now - timedelta(hours=delay_hours)
    else:
        event_time = now

    event = dict(source_order)
    event["store_id"] = source_order["restaurant_id"]
    event["event_time"] = event_time.isoformat()
    event["ingestion_time"] = now.isoformat()
    event["source_dataset"] = "food_delivery_dataset.csv"

    return event


def delivery_report(err, msg) -> None:
    if err is not None:
        log.error(
            "delivery failed for key=%s: %s",
            msg.key(),
            err,
        )


def main() -> None:
    orders = load_orders()

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "client.id": "orders-producer",
    })

    log.info(
        "loaded %d dataset orders; producing to topic=%s "
        "at ~%.2f msg/s",
        len(orders),
        TOPIC,
        MESSAGES_PER_SECOND,
    )

    sleep_seconds = (
        1.0 / MESSAGES_PER_SECOND
        if MESSAGES_PER_SECOND > 0
        else 1.0
    )

    order_index = 0

    while running:
        source_order = orders[order_index]
        event = build_event(source_order)

        producer.produce(
            TOPIC,
            key=event["order_id"],
            value=json.dumps(event),
            callback=delivery_report,
        )
        producer.poll(0)

        log.info(
            "sent order_id=%s store_id=%s event_time=%s",
            event["order_id"],
            event["store_id"],
            event["event_time"],
        )

        order_index = (order_index + 1) % len(orders)
        time.sleep(sleep_seconds)

    producer.flush(10)
    log.info("producer stopped")


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, request_shutdown)
    signal.signal(signal.SIGINT, request_shutdown)
    main()
    