"""
Fix interaction types in the DB:
- Delete all rows with interaction_type = 'thread_create'
- Change interaction_type = 'comment' to 'reply'

Usage:
    uv run db/fix_interaction_types.py
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
            cur.execute("DELETE FROM interactions WHERE interaction_type = 'thread_create'")
            deleted = cur.rowcount
            print(f"Deleted {deleted} thread_create rows")

            cur.execute("UPDATE interactions SET interaction_type = 'reply' WHERE interaction_type = 'comment'")
            updated = cur.rowcount
            print(f"Updated {updated} comment -> reply rows")

        conn.commit()
        print("Done.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
