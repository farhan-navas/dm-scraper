"""
Show row counts for all tables in the database.

Usage:
    uv run db/check_counts.py
"""

import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def main() -> None:
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL not set")

    conn = psycopg2.connect(db_url, connect_timeout=10)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 'threads' as tbl, count(*) FROM threads
                UNION ALL SELECT 'posts', count(*) FROM posts
                UNION ALL SELECT 'users', count(*) FROM users
                UNION ALL SELECT 'interactions', count(*) FROM interactions
                ORDER BY tbl
            """)
            rows = cur.fetchall()

        print(f"\n{'Table':<20} {'Rows':>12}")
        print("-" * 33)
        total = 0
        for tbl, count in rows:
            print(f"{tbl:<20} {count:>12,}")
            total += count
        print("-" * 33)
        print(f"{'total':<20} {total:>12,}")

        with conn.cursor() as cur:
            cur.execute("SELECT pg_size_pretty(pg_database_size(current_database()))")
            size = cur.fetchone()[0]
        print(f"\nDB size on disk: {size}\n")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
