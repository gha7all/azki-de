"""Publish a few sample purchase events to Kafka for the user_events topic.

This script uses the project's connection helpers and defaults to the
`data/user_events.csv` path if you want to adapt it. For a quick smoke test
it emits a handful of `purchase` events with matching `order_id` values.
"""
from __future__ import annotations

import json
import time
from kafka import KafkaProducer
from utils.connections import get_kafka_bootstrap


def main():
    bootstrap = get_kafka_bootstrap()
    producer = KafkaProducer(bootstrap_servers=bootstrap, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    events = [
        {
            'event_timestamp': '2025-12-01 10:00:00',
            'user_id': 1,
            'session_id': 's1',
            'event_name': 'purchase',
            'traffic_channel': 'web',
            'premium_amount': 0,
            'order_id': 1001,
        },
        {
            'event_timestamp': '2025-12-02 11:00:00',
            'user_id': 2,
            'session_id': 's2',
            'event_name': 'purchase',
            'traffic_channel': 'mobile',
            'premium_amount': 0,
            'order_id': 2001,
        },
        {
            'event_timestamp': '2025-12-03 12:00:00',
            'user_id': 3,
            'session_id': 's3',
            'event_name': 'purchase',
            'traffic_channel': 'web',
            'premium_amount': 0,
            'order_id': 3001,
        },
    ]

    for e in events:
        producer.send('user_events', e)
        time.sleep(0.05)

    producer.flush()
    print('published sample purchase events')


if __name__ == '__main__':
    main()
