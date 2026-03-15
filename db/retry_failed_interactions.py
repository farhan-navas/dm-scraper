"""
Retry all failed interactions saved in db_logs/failed_interactions-*.csv.

Run this after scraping multiple forums — interactions that failed due to
missing FK targets (cross-forum quotes, unscraped users) may now succeed.

Usage:
    uv run db/retry_failed_interactions.py
"""

import csv
import os
import sys
from pathlib import Path

import psycopg2
import psycopg2.errors
from dotenv import load_dotenv

from scraper.data_model import INTERACTIONS_FIELDNAMES

load_dotenv()

csv.field_size_limit(sys.maxsize)

LOG_DIR = Path("db_logs")

# Columns that must be cast to int for BIGINT DB columns
_BIGINT_COLS = {"user_id", "thread_id", "source_user_id", "target_user_id"}


def _safe_bigint(val):
    if val is None or val == "":
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _clean_text(val):
    if isinstance(val, str):
        return val.replace("\x00", "")
    return val


def _row_values(row: dict) -> list:
    vals = []
    for col in INTERACTIONS_FIELDNAMES:
        v = row.get(col)
        if not v or v == "":
            v = None
        if col in _BIGINT_COLS:
            v = _safe_bigint(v)
        else:
            v = _clean_text(v)
        vals.append(v)
    return vals


_INSERT_SQL = (
    "INSERT INTO interactions ({cols}) VALUES ({phs}) ON CONFLICT (interaction_id) DO NOTHING"
).format(
    cols=", ".join(INTERACTIONS_FIELDNAMES),
    phs=", ".join(["%s"] * len(INTERACTIONS_FIELDNAMES)),
)


def main() -> None:
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL not set")

    csv_files = sorted(LOG_DIR.glob("failed_interactions-*.csv"))
    if not csv_files:
        print("[retry] No failed interaction CSVs found in db_logs/")
        return

    conn = psycopg2.connect(db_url, connect_timeout=10)

    total_retried = 0
    total_recovered = 0
    total_still_failing = 0

    try:
        for csv_path in csv_files:
            with csv_path.open("r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                rows = list(reader)

            if not rows:
                print(f"[retry] {csv_path.name}: empty, removing")
                csv_path.unlink()
                continue

            recovered = 0
            still_failing: list[dict] = []

            with conn.cursor() as cur:
                for row in rows:
                    vals = _row_values(row)
                    cur.execute("SAVEPOINT retry_sp")
                    try:
                        cur.execute(_INSERT_SQL, vals)
                        cur.execute("RELEASE SAVEPOINT retry_sp")
                        recovered += 1
                    except psycopg2.errors.ForeignKeyViolation:
                        cur.execute("ROLLBACK TO SAVEPOINT retry_sp")
                        still_failing.append(row)

            conn.commit()

            total_retried += len(rows)
            total_recovered += recovered
            total_still_failing += len(still_failing)

            if still_failing:
                # Overwrite with only the ones that still fail
                with csv_path.open("w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=INTERACTIONS_FIELDNAMES)
                    writer.writeheader()
                    writer.writerows(still_failing)
                print(f"[retry] {csv_path.name}: {recovered} recovered, {len(still_failing)} still failing")
            else:
                csv_path.unlink()
                print(f"[retry] {csv_path.name}: all {recovered} recovered, file removed")

        print(f"\n[retry] Done. {total_recovered}/{total_retried} recovered, {total_still_failing} still failing.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
