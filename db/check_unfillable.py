"""
Check how many unfillable interactions have a thread_id set.

Usage:
    uv run db/check_unfillable.py
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
            print("[check] Counting unfillable interactions...")

            cur.execute("""
                SELECT
                    count(*) as total,
                    count(i.thread_id) as has_thread_id,
                    count(*) - count(i.thread_id) as no_thread_id
                FROM interactions i
                WHERE i.target_user_id IS NULL
                  AND NOT EXISTS (
                    SELECT 1 FROM posts p
                    WHERE p.post_id = i.target_post_id
                      AND p.user_id IS NOT NULL
                  )
            """)
            total, has_tid, no_tid = cur.fetchone()
            print(f"Total unfillable:        {total:,}")
            print(f"  Has thread_id:         {has_tid:,}")
            print(f"  No thread_id (NULL):   {no_tid:,}")

            if has_tid > 0:
                # How many of those threads are already scraped vs missing?
                cur.execute("""
                    SELECT
                        count(DISTINCT i.thread_id) as distinct_threads,
                        count(DISTINCT CASE WHEN t.thread_id IS NOT NULL THEN i.thread_id END) as already_scraped,
                        count(DISTINCT CASE WHEN t.thread_id IS NULL THEN i.thread_id END) as not_scraped
                    FROM interactions i
                    LEFT JOIN threads t ON t.thread_id = i.thread_id
                    WHERE i.target_user_id IS NULL
                      AND i.thread_id IS NOT NULL
                      AND NOT EXISTS (
                        SELECT 1 FROM posts p
                        WHERE p.post_id = i.target_post_id
                          AND p.user_id IS NOT NULL
                      )
                """)
                distinct, scraped, not_scraped = cur.fetchone()
                print(f"\n  Distinct threads referenced: {distinct:,}")
                print(f"    Already in threads table:  {scraped:,}")
                print(f"    Not in threads table:      {not_scraped:,}")

                # Show which forums those threads belong to
                cur.execute("""
                    SELECT t.forum_url, count(DISTINCT i.thread_id) as thread_count,
                           count(*) as interaction_count
                    FROM interactions i
                    JOIN threads t ON t.thread_id = i.thread_id
                    WHERE i.target_user_id IS NULL
                      AND i.thread_id IS NOT NULL
                      AND NOT EXISTS (
                        SELECT 1 FROM posts p
                        WHERE p.post_id = i.target_post_id
                          AND p.user_id IS NOT NULL
                      )
                    GROUP BY t.forum_url
                    ORDER BY thread_count DESC
                """)
                rows = cur.fetchall()
                if rows:
                    print(f"\n  Forum breakdown:")
                    print(f"  {'Forum URL':<70s} {'Threads':>8s} {'Interactions':>13s}")
                    print(f"  {'-'*91}")
                    for forum_url, tc, ic in rows:
                        print(f"  {(forum_url or 'unknown'):<70s} {tc:>8,} {ic:>13,}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
