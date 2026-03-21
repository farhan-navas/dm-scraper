"""
Backfill thread_title for threads already in the DB.

Fetches each thread's page and extracts the <h1> title. Skips threads
that already have a title set.

Usage:
    uv run db/backfill_thread_titles.py
    uv run db/backfill_thread_titles.py --max-threads 50   # limit for testing
"""

import argparse
import os

import psycopg2
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from scraper.rate_limiter import fetch

load_dotenv()

ALTER_DDL = "ALTER TABLE threads ADD COLUMN IF NOT EXISTS thread_title TEXT"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill thread titles from thread pages")
    parser.add_argument(
        "--max-threads",
        type=int,
        default=None,
        help="Limit number of threads to process (for testing)",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL not set")

    conn = psycopg2.connect(db_url, connect_timeout=10)

    with conn.cursor() as cur:
        cur.execute(ALTER_DDL)
    conn.commit()

    with conn.cursor() as cur:
        cur.execute(
            "SELECT thread_id, thread_url FROM threads "
            "WHERE thread_title IS NULL AND thread_url IS NOT NULL"
        )
        threads = cur.fetchall()

    print(f"[backfill] {len(threads)} threads missing titles")

    if args.max_threads:
        threads = threads[:args.max_threads]
        print(f"[backfill] Limited to {args.max_threads}")

    if not threads:
        print("[backfill] Nothing to do.")
        conn.close()
        return

    updated = 0
    errors = 0

    try:
        for i, (thread_id, thread_url) in enumerate(threads, start=1):
            print(f"[backfill] ({i}/{len(threads)}) {thread_url}")

            try:
                html = fetch(thread_url)
            except Exception as exc:
                print(f"[backfill] Error fetching {thread_url}: {exc}")
                errors += 1
                continue

            soup = BeautifulSoup(html, "html.parser")
            h1 = soup.select_one("h1")
            if not h1:
                continue

            title = h1.get_text(strip=True)
            if not title:
                continue

            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE threads SET thread_title = %s WHERE thread_id = %s",
                    (title, thread_id),
                )
            updated += 1

            if updated % 50 == 0:
                conn.commit()

        conn.commit()
    finally:
        conn.close()

    print(f"\n[backfill] Done. {updated} titles updated, {errors} errors.")


if __name__ == "__main__":
    main()
