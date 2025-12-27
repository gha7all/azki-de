"""Small verification script that runs quick ClickHouse queries and prints results.

Usage: python scripts/verify_clickhouse.py
"""
from __future__ import annotations

from utils.connections import get_clickhouse_client


def main():
    client = get_clickhouse_client()

    q1 = 'SELECT count() FROM user_events'
    q2 = 'SELECT count() FROM user_events_denorm'
    q3 = 'SELECT * FROM user_events_denorm LIMIT 5'

    print('user_events count:', client.execute(q1))
    print('user_events_denorm count:', client.execute(q2))
    print('user_events_denorm sample:')
    for row in client.execute(q3):
        print(row)


if __name__ == '__main__':
    main()
