"""
Backfill target_user_id on interaction rows where it's NULL.

Joins interactions against the posts table to look up the author of
the target post. No scraping needed — purely a DB operation.

Usage:
    uv run db/backfill_target_user_ids.py
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
            # Count total with NULL target_user_id
            cur.execute("SELECT count(*) FROM interactions WHERE target_user_id IS NULL")
            total_null = cur.fetchone()[0]
            print(f"Interactions with NULL target_user_id: {total_null}")

            # Count how many can be filled (target_post_id exists in posts)
            cur.execute("""
                SELECT count(*) FROM interactions i
                JOIN posts p ON i.target_post_id = p.post_id
                WHERE i.target_user_id IS NULL
                  AND p.user_id IS NOT NULL
            """)
            fillable = cur.fetchone()[0]
            print(f"Fillable (target_post_id found in posts): {fillable}")
            print(f"Unfillable (post not in DB): {total_null - fillable}")

            if fillable == 0:
                print("Nothing to update.")
                return

            # Do the update
            cur.execute("""
                UPDATE interactions i
                SET target_user_id = p.user_id
                FROM posts p
                WHERE i.target_post_id = p.post_id
                  AND i.target_user_id IS NULL
                  AND p.user_id IS NOT NULL
            """)
            updated = cur.rowcount

        conn.commit()
        print(f"Updated {updated} rows.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
