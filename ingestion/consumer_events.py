import sys
import os
import logging
import json
import signal
from typing import List, Tuple
from confluent_kafka import Consumer, KafkaError
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.connections import get_clickhouse_client, get_kafka_bootstrap

# --- Configure logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


def main():
    consumer_group = "consumer_events_v1"
    topic = "user_events"
    batch_size = 500  # Insert into ClickHouse after this many messages

    bootstrap = get_kafka_bootstrap()
    logger.info(f"Connecting to Kafka broker at {bootstrap}")

    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": consumer_group,
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    })

    consumer.subscribe([topic])
    logger.info(f"Subscribed to topic: {topic}")

    client = get_clickhouse_client()
    logger.info("Connected to ClickHouse")

    batch: List[Tuple] = []
    total_messages = 0
    running = True

    def _stop(signum, frame):
        nonlocal running
        logger.info(f"Received signal {signum}, shutting down...")
        running = False

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    try:
        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error(f"Consumer error: {msg.error()}")
                continue

            e = json.loads(msg.value().decode("utf-8"))
            timestamp_str = e.get("event_timestamp") or e.get("event_time")
            try:
                timestamp = datetime.fromisoformat(timestamp_str) if timestamp_str else None
            except ValueError:
                timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S") if timestamp_str else None
            batch.append(
                (
                    timestamp,
                    int(e.get("user_id", 0)),
                    str(e.get("session_id", "")),
                    str(e.get("event_name") or e.get("event_type") or ""),
                    str(e.get("traffic_channel") or e.get("channel") or ""),
                    int(e.get("premium_amount", 0)),
                )
            )
            total_messages += 1

            if len(batch) >= batch_size:
                client.execute(
                    "INSERT INTO default.user_events (event_timestamp, user_id, session_id, event_name, traffic_channel, premium_amount) VALUES",
                    batch,
                )
                logger.info(f"Inserted batch of {len(batch)} messages (total processed: {total_messages})")
                batch.clear()

    finally:
        if batch:
            client.execute(
                "INSERT INTO default.user_events (event_timestamp, user_id, session_id, event_name, traffic_channel, premium_amount) VALUES",
                batch,
            )
            logger.info(f"Inserted final batch of {len(batch)} messages (total processed: {total_messages})")

        consumer.close()
        logger.info(f"Kafka consumer closed (total messages processed: {total_messages})")


if __name__ == "__main__":
    main()
