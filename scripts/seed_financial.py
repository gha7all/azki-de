from __future__ import annotations

from utils.connections import get_clickhouse_client


def main():
    client = get_clickhouse_client()

    rows = [
        (1001, 1999, 'paid'),
        (2001, 2999, 'pending'),
        (3001, 4999, 'paid'),
        (4001, 1299, 'failed'),
    ]

    client.execute('INSERT INTO financial_order (order_id, amount, payment_status) VALUES', rows)
    print('seeded financial_order')


if __name__ == '__main__':
    main()
