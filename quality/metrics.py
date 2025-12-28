from __future__ import annotations
from kafka import KafkaConsumer
from utils.connections import get_clickhouse_client
import os

client = get_clickhouse_client()
TABLE = "user_events"

def ingestion_delay_p95(hours: int = 1):
    query = f"""
        SELECT quantile(0.95)(now() - ingestion_time) AS p95_delay_sec
        FROM {TABLE}
        WHERE ingestion_time >= now() - INTERVAL {hours} HOUR
    """
    delay = client.execute(query)[0][0]
    print(f"P95 ingestion delay over last {hours} hours: {delay} seconds")

def consumer_lag(topic: str, group: str, bootstrap_servers=None):
    if bootstrap_servers is None:
        bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group,
        enable_auto_commit=False
    )

    partitions = consumer.partitions_for_topic(topic)
    lag = {}
    for p in partitions:
        tp = (topic, p)
        committed = consumer.committed(tp) or 0
        latest = consumer.end_offsets([tp])[tp]
        lag[tp] = latest - committed
    print("Consumer lag per partition:", lag)

if __name__ == "__main__":
    ingestion_delay_p95()
    consumer_lag(topic="user_events", group="consumer_events_v1")
