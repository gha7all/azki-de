from __future__ import annotations

from datetime import datetime, timezone
from typing import Tuple

import pandas as pd
from utils.connections import get_clickhouse_client


def _normalize_row(r) -> Tuple[int, str, str, str, str]:
    user_id = int(r.user_id)
    signup_date = str(getattr(r, "signup_date", "1970-01-01"))
    city = str(getattr(r, "city", ""))
    device_type = str(getattr(r, "device_type", "unknown"))
    updated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    return (user_id, signup_date, city, device_type, updated_at)


def load_users_snapshot(csv_path: str, batch_size: int = 1000) -> int:
    df = pd.read_csv(csv_path)

    if "user_id" not in df.columns:
        raise ValueError("CSV missing required column: user_id")

    if "signup_date" not in df.columns:
        df["signup_date"] = "1970-01-01"
    if "city" not in df.columns:
        df["city"] = ""
    if "device_type" not in df.columns:
        df["device_type"] = "unknown"

    df = df.dropna(subset=["user_id"])

    client = get_clickhouse_client()
    inserted = 0
    batch = []

    for r in df.itertuples(index=False):
        batch.append(_normalize_row(r))
        if len(batch) >= batch_size:
            client.execute(
                "INSERT INTO users (user_id, signup_date, city, device_type, updated_at) VALUES",
                batch,
            )
            inserted += len(batch)
            batch.clear()

    if batch:
        client.execute(
            "INSERT INTO users (user_id, signup_date, city, device_type, updated_at) VALUES",
            batch,
        )
        inserted += len(batch)

    return inserted


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("usage: python -m ingestion.loaders <users_csv_path>")
        raise SystemExit(2)

    csv_file_path = sys.argv[1]
    count = load_users_snapshot(csv_file_path)
    print(f"Inserted {count} rows into ClickHouse users table")
