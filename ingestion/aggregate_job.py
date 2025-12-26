from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Tuple

from clickhouse_driver import Client as CHClient
from dateutil import parser as date_parser
from utils.connections import get_clickhouse_client, get_mysql_connection

def fetch_events(ch: CHClient, since: datetime, until: datetime):
    query = """
        SELECT event_timestamp, user_id, event_name, premium_amount
        FROM user_events
        WHERE event_timestamp >= %(since)s AND event_timestamp < %(until)s
    """
    params = {"since": since.strftime("%Y-%m-%d %H:%M:%S"), "until": until.strftime("%Y-%m-%d %H:%M:%S")}
    for row in ch.execute(query, params):
        yield row


def fetch_users_from_mysql() -> Dict[int, str]:
    users: Dict[int, str] = {}
    csv_fallback = Path(__file__).parent.parent / "data" / "users.csv"
    conn = get_mysql_connection()

    if conn is None:
        if csv_fallback.exists():
            with open(csv_fallback, newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    users[int(row["user_id"])] = row.get("city", "")
            return users
        raise RuntimeError("MySQL not available and users.csv not found.")

    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT user_id, city FROM users")
            for user_id, city in cur.fetchall():
                users[int(user_id)] = city or ""

    return users


def aggregate_and_write(ch: CHClient, users_map: Dict[int, str], since: datetime, until: datetime):
    aggs: Dict[Tuple[str, str, str], Tuple[int, int]] = defaultdict(lambda: (0, 0))

    for ts, user_id, event_name, premium in fetch_events(ch, since, until):
        ts = ts if isinstance(ts, datetime) else date_parser.parse(ts)
        month = ts.strftime("%Y-%m")
        city = users_map.get(int(user_id), "unknown")
        cnt, total = aggs[(month, city, event_name)]
        aggs[(month, city, event_name)] = (cnt + 1, total + int(premium or 0))

    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    rows = [(month, city, event_name, cnt, total, now) for (month, city, event_name), (cnt, total) in aggs.items()]

    if rows:
        ch.execute(
            "INSERT INTO events_agg (agg_month, city, event_name, event_count, total_premium, updated_at) VALUES",
            rows,
        )

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--since", type=str, help="start datetime YYYY-MM-DD or YYYY-MM-DD HH:MM:SS")
    parser.add_argument("--until", type=str, help="end datetime (exclusive)")
    parser.add_argument("--days", type=int, default=1, help="window size in days if --since not provided")
    return parser.parse_args()


def parse_dt(s: str) -> datetime:
    try:
        return date_parser.parse(s)
    except Exception:
        raise ValueError(f"Invalid date format: {s}")

def main():
    args = parse_args()
    
    if args.since:
        since = parse_dt(args.since)
        until = parse_dt(args.until) if args.until else since + timedelta(days=args.days)
    else:
        until = datetime.now(timezone.utc)
        since = until - timedelta(days=args.days)

    ch = get_clickhouse_client()
    users_map = fetch_users_from_mysql()
    aggregate_and_write(ch, users_map, since, until)


if __name__ == "__main__":
    main()
