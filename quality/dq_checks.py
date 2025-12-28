from __future__ import annotations
from datetime import datetime, timedelta
from utils.connections import get_clickhouse_client

client = get_clickhouse_client()
TABLE = "user_events"

def check_row_counts(start_date: str, end_date: str):
    query = f"""
        SELECT toDate(event_timestamp) AS day, count() AS cnt
        FROM {TABLE}
        WHERE event_timestamp >= '{start_date}' AND event_timestamp < '{end_date}'
        GROUP BY day
        ORDER BY day
    """
    rows = client.execute(query)
    print("=== Row Counts per Day ===")
    for day, cnt in rows:
        print(f"{day}: {cnt} rows")

def check_missing_fields():
    query = f"""
        SELECT count() AS cnt
        FROM {TABLE}
        WHERE user_id = 0 OR session_id = '' OR event_timestamp IS NULL
    """
    count = client.execute(query)[0][0]
    print(f"Missing/invalid fields count: {count}")

def check_duplicates():
    query = f"""
        SELECT count() AS dup_count
        FROM (
            SELECT user_id, session_id, event_timestamp, COUNT() AS cnt
            FROM {TABLE}
            GROUP BY user_id, session_id, event_timestamp
            HAVING cnt > 1
        )
    """
    dup_count = client.execute(query)[0][0]
    print(f"Duplicate events (user_id+session_id+event_timestamp): {dup_count}")

def check_ingestion_delay(p95_window_hours: int = 1):
    query = f"""
        SELECT quantile(0.95)(now() - ingestion_time) AS p95_delay_sec
        FROM {TABLE}
        WHERE ingestion_time >= now() - INTERVAL {p95_window_hours} HOUR
    """
    delay = client.execute(query)[0][0]
    print(f"P95 ingestion delay over last {p95_window_hours}h: {delay} seconds")

if __name__ == "__main__":
    yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
    today = datetime.utcnow().strftime("%Y-%m-%d")

    check_row_counts(yesterday, today)
    check_missing_fields()
    check_duplicates()
    check_ingestion_delay()
