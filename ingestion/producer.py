import os
import json
import time
import pandas as pd
from kafka import KafkaProducer


def main(csv_path: str | None = None):
    from utils.connections import get_kafka_bootstrap

    bootstrap = get_kafka_bootstrap()

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    csv_path = csv_path or os.getenv("USER_EVENTS_CSV", "data/user_events.csv")
    df = pd.read_csv(csv_path)

    for _, row in df.iterrows():
        producer.send("user_events", row.to_dict())
        time.sleep(0.01)

    producer.flush()


if __name__ == "__main__":
    main()
