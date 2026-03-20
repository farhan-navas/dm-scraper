"""
Scrape the /following page and bio for every user in the DB.

- Inserts follow edges into the follows table.
- Updates the user's bio in the users table (if present on the about page).

Skips users whose follows have already been scraped (follower_id exists
in follows table). Gracefully handles auth-gated profiles (303 redirects).

Usage:
    uv run scrape_user_follows.py
    uv run scrape_user_follows.py --max-users 100    # limit for testing
    uv run scrape_user_follows.py --no-skip           # re-scrape all users
"""

import argparse
import os
from datetime import datetime

import psycopg2
import psycopg2.errors
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from scraper.rate_limiter import fetch

load_dotenv()

FOLLOWS_DDL = """
    CREATE TABLE IF NOT EXISTS follows (
        follower_id BIGINT REFERENCES users(user_id),
        followed_id BIGINT REFERENCES users(user_id),
        scraped_at TIMESTAMPTZ,
        PRIMARY KEY (follower_id, followed_id)
    );
"""

# Add bio column if it doesn't exist
ADD_BIO_COLUMN = """
    ALTER TABLE users ADD COLUMN IF NOT EXISTS bio TEXT;
"""

INSERT_FOLLOW = """
    INSERT INTO follows (follower_id, followed_id, scraped_at)
    VALUES (%s, %s, %s)
    ON CONFLICT (follower_id, followed_id) DO NOTHING
"""

UPDATE_BIO = """
    UPDATE users SET bio = %s WHERE user_id = %s AND (bio IS NULL OR bio = '')
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


def _parse_bio(html: str) -> str | None:
    """Extract the 'About Me' bio text from an /about page."""
    soup = BeautifulSoup(html, "html.parser")
    about_row = soup.select_one(".about-me-row")
    if about_row:
        wrapper = about_row.select_one(".bbWrapper")
        if wrapper:
            text = wrapper.get_text("\n", strip=True)
            return text if text else None
    return None


def _is_login_page(html: str) -> bool:
    """Check if the response is a login redirect page."""
    soup = BeautifulSoup(html, "html.parser")
    html_tag = soup.select_one("html")
    return bool(html_tag and html_tag.get("data-template") == "login")


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
    parser = argparse.ArgumentParser(description="Scrape /following and bio for all users in the DB")
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

    # Ensure schema is up to date
    with conn.cursor() as cur:
        cur.execute(FOLLOWS_DDL)
        cur.execute(ADD_BIO_COLUMN)
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
    total_bios = 0
    errors = 0
    auth_blocked = 0

    try:
        for i, (user_id, profile_url) in enumerate(all_users, start=1):
            following_url = profile_url.rstrip("/") + "/following"
            about_url = profile_url.rstrip("/") + "/about"

            # --- Scrape /about for bio ---
            try:
                about_html = fetch(about_url)
                if not _is_login_page(about_html):
                    bio = _parse_bio(about_html)
                    if bio:
                        with conn.cursor() as cur:
                            cur.execute(UPDATE_BIO, (bio, int(user_id)))
                        total_bios += 1
            except Exception as exc:
                print(f"[follows] ({i}/{len(all_users)}) Error fetching about for {user_id}: {exc}")

            # --- Scrape /following ---
            print(f"[follows] ({i}/{len(all_users)}) Scraping {following_url}")

            try:
                html = fetch(following_url)
            except Exception as exc:
                print(f"[follows] Error fetching {following_url}: {exc}")
                errors += 1
                continue

            if _is_login_page(html):
                print(f"[follows] Skipping {user_id} — requires auth")
                auth_blocked += 1
                continue

            followed_ids = _parse_following_page(html)

            if not followed_ids:
                conn.commit()
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

    print(
        f"\n[follows] Done. {total_edges} follow edges, {total_bios} bios updated, "
        f"{errors} errors, {auth_blocked} auth-blocked."
    )


if __name__ == "__main__":
    main()
