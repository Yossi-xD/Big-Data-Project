"""Generates synthetic orders_stream events onto Kafka.

Schema (JSON), per the team's data contract:
    order_id, store_id, order_value, delivery_duration, traffic_condition,
    event_time, ingestion_time

A configurable fraction of events are emitted with a backdated event_time
(up to MAX_LATE_HOURS in the past) to exercise late-arrival handling in the
silver layer, per the assignment's "up to 48 hours after event time" requirement.
"""

import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("orders_producer")

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "orders_stream")
MESSAGES_PER_SECOND = float(os.environ.get("MESSAGES_PER_SECOND", "2"))
LATE_RATIO = float(os.environ.get("LATE_RATIO", "0.15"))
MAX_LATE_HOURS = float(os.environ.get("MAX_LATE_HOURS", "48"))
STORE_IDS = os.environ.get("STORE_IDS", "5021,3340,1042,7788,9901").split(",")
TRAFFIC_CONDITIONS = ["Low", "Medium", "High"]


def build_order() -> dict:
    now = datetime.now(timezone.utc)

    if random.random() < LATE_RATIO:
        delay_hours = random.uniform(0.5, MAX_LATE_HOURS)
        event_time = now - timedelta(hours=delay_hours)
    else:
        event_time = now

    return {
        "order_id": f"ORD_{uuid.uuid4().hex[:10].upper()}",
        "store_id": random.choice(STORE_IDS),
        "order_value": round(random.uniform(5.0, 80.0), 2),
        "delivery_duration": random.randint(10, 65),
        "traffic_condition": random.choice(TRAFFIC_CONDITIONS),
        "event_time": event_time.isoformat(),
        "ingestion_time": now.isoformat(),
    }


def delivery_report(err, msg) -> None:
    if err is not None:
        log.error("delivery failed for key=%s: %s", msg.key(), err)


def main() -> None:
    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "client.id": "orders-producer",
    })

    log.info(
        "producing to topic=%s at ~%.2f msg/s (late_ratio=%.2f, max_late_hours=%.1f)",
        TOPIC, MESSAGES_PER_SECOND, LATE_RATIO, MAX_LATE_HOURS,
    )

    sleep_s = 1.0 / MESSAGES_PER_SECOND if MESSAGES_PER_SECOND > 0 else 1.0

    try:
        while True:
            order = build_order()
            producer.produce(
                TOPIC,
                key=order["order_id"],
                value=json.dumps(order),
                callback=delivery_report,
            )
            producer.poll(0)
            log.info("sent %s", order)
            time.sleep(sleep_s)
    except KeyboardInterrupt:
        log.info("shutting down producer")
    finally:
        producer.flush(10)


if __name__ == "__main__":
    main()
