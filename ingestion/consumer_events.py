import os
import json
import signal
from typing import List, Tuple

from kafka import KafkaConsumer
from clickhouse_driver import Client
from utils.connections import get_clickhouse_client, get_kafka_bootstrap


def _clickhouse_client() -> Client:
    return Client(
        host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
        port=int(os.getenv("CLICKHOUSE_PORT", "9000")),
        user=os.getenv("CLICKHOUSE_USER", "azki_user"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "azki_pass"),
        database=os.getenv("CLICKHOUSE_DB", "azki"),
    )


def main():
    bootstrap = get_kafka_bootstrap()
    consumer = KafkaConsumer(
        "user_events",
        bootstrap_servers=bootstrap,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=os.getenv("KAFKA_CONSUMER_GROUP", "consumer_events_v1"),
    )

    client = get_clickhouse_client()

    batch: List[Tuple] = []
    running = True


    def _stop(signum, frame):
        nonlocal running
        running = False


    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    try:
        for msg in consumer:
            if not running:
                break
            e = msg.value
            # map the CSV fields to the ClickHouse `user_events` table
            batch.append(
                (
                    e.get("event_time"),
                    int(e.get("user_id", 0)),
                    str(e.get("session_id", "")),
                    str(e.get("event_type", "")),
                    str(e.get("channel", "")),
                    int(e.get("premium_amount", 0)),
                )
            )

            if len(batch) >= 500:
                client.execute(
                    "INSERT INTO user_events (event_timestamp, user_id, session_id, event_name, traffic_channel, premium_amount) VALUES",
                    batch,
                )
                batch.clear()

    finally:
        if batch:
            client.execute(
                "INSERT INTO user_events (event_timestamp, user_id, session_id, event_name, traffic_channel, premium_amount) VALUES",
                batch,
            )
        consumer.close()


if __name__ == "__main__":
    main()
