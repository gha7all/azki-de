from __future__ import annotations

import sys
import os
import logging
import json
import time
import pandas as pd
from confluent_kafka import Producer

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.connections import get_kafka_bootstrap

# --- Configure logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


def main(csv_path: str | None = None):
    bootstrap = get_kafka_bootstrap()
    logger.info(f"Connecting to Kafka broker at {bootstrap}")

    producer = Producer({'bootstrap.servers': bootstrap})

    csv_path = csv_path or "data/sample_user_events.csv"
    logger.info(f"Loading events from {csv_path}")

    df = pd.read_csv(csv_path)
    logger.info(f"Loaded {len(df)} events")

    # --- Delivery callback ---
    def delivery_report(err, msg):
        if err:
            logger.error(f"Failed to deliver message: {err}")
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    for i, (_, row) in enumerate(df.iterrows(), start=1):
        producer.produce(
            topic="user_events",
            value=json.dumps(row.to_dict()),
            callback=delivery_report
        )
        producer.poll(0)  # Serve delivery reports

        if i % 100 == 0:  # Log every 100 messages
            logger.info(f"Produced {i}/{len(df)} messages")

        time.sleep(0.01)

    producer.flush()
    logger.info(f"Finished producing {len(df)} messages")


if __name__ == "__main__":
    main()
