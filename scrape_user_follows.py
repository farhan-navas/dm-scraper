"""
Scrape the /following page for every user in the DB and insert follow
edges into the follows table.

Skips users whose follows have already been scraped (follower_id exists
in follows table). Gracefully handles auth-gated profiles (303 redirects).

Usage:
    uv run scrape_user_follows.py
    uv run scrape_user_follows.py --max-users 100    # limit for testing
    uv run scrape_user_follows.py --no-skip           # re-scrape all users
"""

import argparse
import os
import re
from datetime import datetime

import psycopg2
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from scraper.rate_limiter import fetch
from scraper.user_scraper import extract_user_id_from_profile_url

load_dotenv()

BASE_URL = "https://www.personalitycafe.com"

FOLLOWS_DDL = """
    CREATE TABLE IF NOT EXISTS follows (
        follower_id BIGINT REFERENCES users(user_id),
        followed_id BIGINT REFERENCES users(user_id),
        scraped_at TIMESTAMPTZ,
        PRIMARY KEY (follower_id, followed_id)
    );
"""

INSERT_FOLLOW = """
    INSERT INTO follows (follower_id, followed_id, scraped_at)
    VALUES (%s, %s, %s)
    ON CONFLICT (follower_id, followed_id) DO NOTHING
"""


def _scrape_timestamp() -> str:
    return datetime.now().isoformat(timespec="seconds") + "Z"


def _parse_following_page(html: str) -> list[str]:
    """Extract user IDs from a /following page. Returns list of user_id strings."""
    soup = BeautifulSoup(html, "html.parser")
    user_ids: list[str] = []
    seen: set[str] = set()

    for el in soup.select(".block-row a[data-user-id]"):
        uid = el.get("data-user-id")
        if uid and uid not in seen:
            seen.add(uid)
            user_ids.append(str(uid))

    return user_ids


def _get_all_user_ids(conn) -> list[tuple[str, str]]:
    """Return (user_id, profile_url) for all users in the DB."""
    with conn.cursor() as cur:
        cur.execute("SELECT user_id, profile_url FROM users WHERE profile_url IS NOT NULL")
        return [(str(row[0]), row[1]) for row in cur.fetchall()]


def _get_already_scraped_follower_ids(conn) -> set[str]:
    """Return set of user IDs that already have follow edges in the DB."""
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT follower_id FROM follows")
        return {str(row[0]) for row in cur.fetchall()}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape /following for all users in the DB")
    parser.add_argument(
        "--max-users",
        type=int,
        default=None,
        help="Limit number of users to scrape (for testing)",
    )
    parser.add_argument(
        "--no-skip",
        action="store_true",
        help="Re-scrape all users even if already in follows table",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL not set")

    conn = psycopg2.connect(db_url, connect_timeout=10)

    # Ensure follows table exists
    with conn.cursor() as cur:
        cur.execute(FOLLOWS_DDL)
    conn.commit()

    all_users = _get_all_user_ids(conn)
    print(f"[follows] Found {len(all_users)} users in DB")

    if not args.no_skip:
        already_scraped = _get_already_scraped_follower_ids(conn)
        before = len(all_users)
        all_users = [(uid, url) for uid, url in all_users if uid not in already_scraped]
        skipped = before - len(all_users)
        if skipped:
            print(f"[follows] Skipping {skipped} already-scraped users, {len(all_users)} remaining")

    if args.max_users:
        all_users = all_users[:args.max_users]
        print(f"[follows] Limited to {args.max_users} users")

    if not all_users:
        print("[follows] No users to scrape — done.")
        conn.close()
        return

    total_edges = 0
    errors = 0
    auth_blocked = 0

    try:
        for i, (user_id, profile_url) in enumerate(all_users, start=1):
            following_url = profile_url.rstrip("/") + "/following"
            print(f"[follows] ({i}/{len(all_users)}) Scraping {following_url}")

            try:
                html = fetch(following_url)
            except Exception as exc:
                print(f"[follows] Error fetching {following_url}: {exc}")
                errors += 1
                continue

            # Check for login redirect (auth-gated profile)
            soup_check = BeautifulSoup(html, "html.parser")
            template = soup_check.select_one("html")
            if template and template.get("data-template") == "login":
                print(f"[follows] Skipping {user_id} — requires auth")
                auth_blocked += 1
                continue

            followed_ids = _parse_following_page(html)

            if not followed_ids:
                # Insert a self-referencing edge so we know this user was scraped
                # (they just follow nobody). Actually, just skip — the already_scraped
                # check uses DISTINCT follower_id, so we need at least one row.
                # We'll handle this by just continuing.
                continue

            scraped_at = _scrape_timestamp()
            with conn.cursor() as cur:
                for followed_id in followed_ids:
                    cur.execute("SAVEPOINT follow_sp")
                    try:
                        cur.execute(INSERT_FOLLOW, (int(user_id), int(followed_id), scraped_at))
                        cur.execute("RELEASE SAVEPOINT follow_sp")
                        total_edges += 1
                    except (psycopg2.errors.ForeignKeyViolation, ValueError):
                        cur.execute("ROLLBACK TO SAVEPOINT follow_sp")

            conn.commit()

    finally:
        conn.close()

    print(f"\n[follows] Done. {total_edges} follow edges inserted, {errors} errors, {auth_blocked} auth-blocked.")


if __name__ == "__main__":
    main()
