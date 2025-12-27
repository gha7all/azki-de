"""Seed small sample rows into product_* tables for local testing.

Run inside the repo root (it uses Docker hostnames by default).
"""
from __future__ import annotations

from utils.connections import get_clickhouse_client


def main():
    client = get_clickhouse_client()

    # small sample data
    third = [(1001, 501, 'third product details A'), (1002, 502, 'third product details B')]
    body = [(2001, 601, 'body product A')]
    medical = [(3001, 701, 'medical details A')]
    fire = [(4001, 801, 'fire product A')]

    client.execute('INSERT INTO product_third (order_id, product_id, details) VALUES', third)
    client.execute('INSERT INTO product_body (order_id, product_id, details) VALUES', body)
    client.execute('INSERT INTO product_medical (order_id, product_id, details) VALUES', medical)
    client.execute('INSERT INTO product_fire (order_id, product_id, details) VALUES', fire)

    print('seeded product tables')


if __name__ == '__main__':
    main()
