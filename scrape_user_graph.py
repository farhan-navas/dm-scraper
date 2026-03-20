"""
Scrape the /following page and bio for every user in the DB.

- Inserts follow edges into the follows table.
- Updates the user's bio in the users table (if present on the about page).
- Discovers new users from follow lists and scrapes their profiles.

Each run expands the graph by one level. Re-run until no new users are
found to build the complete follow graph.

Skips users whose follows have already been scraped (follower_id exists
in follows table). Gracefully handles auth-gated profiles (303 redirects).

Usage:
    uv run scrape_user_graph.py
    uv run scrape_user_graph.py --max-users 100    # limit for testing
    uv run scrape_user_graph.py --no-skip           # re-scrape all users
"""

import argparse
import os
from datetime import datetime

import psycopg2
import psycopg2.errors
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from scraper.rate_limiter import fetch
from scraper.user_scraper import fetch_user_profile

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

INSERT_USER = """
    INSERT INTO users (user_id, username, profile_url, join_date, role,
        gender, country_of_birth, location, mbti_type, enneagram_type,
        socionics, occupation, replies, discussions_created, reaction_score,
        points, media_count, showcase_count, scraped_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (user_id) DO NOTHING
"""

from scraper.data_model import USERS_FIELDNAMES


def _scrape_timestamp() -> str:
    return datetime.now().isoformat(timespec="seconds") + "Z"


def _parse_following_page(html: str) -> list[tuple[str, str]]:
    """Extract (user_id, profile_url) pairs from a /following page."""
    soup = BeautifulSoup(html, "html.parser")
    results: list[tuple[str, str]] = []
    seen: set[str] = set()

    for el in soup.select(".block-row a[data-user-id]"):
        uid = el.get("data-user-id")
        href = el.get("href")
        if uid and uid not in seen and href:
            seen.add(uid)
            profile_url = href.rstrip("/") + "/"
            if not profile_url.startswith("http"):
                profile_url = BASE_URL + profile_url
            results.append((str(uid), profile_url))

    return results


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


def _get_existing_user_ids(conn) -> set[str]:
    """Return all user_ids currently in the DB."""
    with conn.cursor() as cur:
        cur.execute("SELECT user_id FROM users")
        return {str(row[0]) for row in cur.fetchall()}


def _get_already_scraped_follower_ids(conn) -> set[str]:
    """Return set of user IDs that already have follow edges in the DB."""
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT follower_id FROM follows")
        return {str(row[0]) for row in cur.fetchall()}


def _insert_user_to_db(conn, user: dict) -> None:
    """Insert a user dict into the users table."""
    vals = [user.get(col) for col in USERS_FIELDNAMES]
    # Cast user_id to int
    if vals[0] is not None:
        try:
            vals[0] = int(vals[0])
        except (ValueError, TypeError):
            return
    with conn.cursor() as cur:
        cur.execute(INSERT_USER, vals)


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

    # Collect follow edges that failed FK so we can retry after discovering new users
    pending_edges: list[tuple[int, int, str]] = []
    # Map discovered user_id -> profile_url for new user scraping
    discovered_users: dict[str, str] = {}

    try:
        # --- Phase 1: Scrape /about + /following for existing users ---
        print(f"\n[follows] Phase 1: Scraping follows and bios for {len(all_users)} users...")

        for i, (user_id, profile_url) in enumerate(all_users, start=1):
            following_url = profile_url.rstrip("/") + "/following"
            about_url = profile_url.rstrip("/") + "/about"

            # Scrape /about for bio
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

            # Scrape /following
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

            followed_pairs = _parse_following_page(html)

            if not followed_pairs:
                conn.commit()
                continue

            for fid, furl in followed_pairs:
                discovered_users[fid] = furl

            scraped_at = _scrape_timestamp()

            with conn.cursor() as cur:
                for followed_id, _ in followed_pairs:
                    cur.execute("SAVEPOINT follow_sp")
                    try:
                        cur.execute(INSERT_FOLLOW, (int(user_id), int(followed_id), scraped_at))
                        cur.execute("RELEASE SAVEPOINT follow_sp")
                        total_edges += 1
                    except psycopg2.errors.ForeignKeyViolation:
                        cur.execute("ROLLBACK TO SAVEPOINT follow_sp")
                        pending_edges.append((int(user_id), int(followed_id), scraped_at))
                    except ValueError:
                        cur.execute("ROLLBACK TO SAVEPOINT follow_sp")

            conn.commit()

        # --- Phase 2: Discover and scrape new users ---
        existing_ids = _get_existing_user_ids(conn)
        new_users = {uid: url for uid, url in discovered_users.items() if uid not in existing_ids}

        if new_users:
            print(f"\n[follows] Phase 2: Discovered {len(new_users)} new users — scraping profiles...")
            new_users_scraped = 0

            for i, (uid, profile_url) in enumerate(new_users.items(), start=1):
                print(f"[follows] ({i}/{len(new_users)}) Scraping profile for new user {uid}")

                try:
                    profile = fetch_user_profile(profile_url)
                except Exception as exc:
                    print(f"[follows] Error fetching profile for {uid}: {exc}")
                    continue

                if not profile:
                    continue

                _insert_user_to_db(conn, profile)
                new_users_scraped += 1

                # Also grab bio
                about_url = profile_url.rstrip("/") + "/about"
                try:
                    about_html = fetch(about_url)
                    if not _is_login_page(about_html):
                        bio = _parse_bio(about_html)
                        if bio:
                            with conn.cursor() as cur:
                                cur.execute(UPDATE_BIO, (bio, int(uid)))
                            total_bios += 1
                except Exception:
                    pass

                conn.commit()

            print(f"[follows] Phase 2 complete: {new_users_scraped} new users added to DB.")
        else:
            print(f"\n[follows] No new users discovered.")

        # --- Phase 3: Retry pending follow edges ---
        if pending_edges:
            print(f"\n[follows] Phase 3: Retrying {len(pending_edges)} pending follow edges...")
            recovered = 0
            still_failing = 0

            with conn.cursor() as cur:
                for follower_id, followed_id, scraped_at in pending_edges:
                    cur.execute("SAVEPOINT retry_sp")
                    try:
                        cur.execute(INSERT_FOLLOW, (follower_id, followed_id, scraped_at))
                        cur.execute("RELEASE SAVEPOINT retry_sp")
                        recovered += 1
                        total_edges += 1
                    except psycopg2.errors.ForeignKeyViolation:
                        cur.execute("ROLLBACK TO SAVEPOINT retry_sp")
                        still_failing += 1

            conn.commit()
            print(f"[follows] Retry: {recovered} recovered, {still_failing} still failing (user not on site)")

    finally:
        conn.close()

    print(
        f"\n[follows] Done. {total_edges} follow edges, {total_bios} bios updated, "
        f"{errors} errors, {auth_blocked} auth-blocked, "
        f"{len(new_users) if 'new_users' in dir() else 0} new users discovered."
    )


if __name__ == "__main__":
    main()
