import argparse
import csv
import os
import re
from pathlib import Path
from urllib.parse import urlparse

from dotenv import load_dotenv

from scraper.data_model import (
    INTERACTIONS_FIELDNAMES,
    POSTS_FIELDNAMES,
    THREADS_FIELDNAMES,
    USERS_FIELDNAMES,
)
from scraper.post_scraper import (
    absolute_url,
    get_thread_list,
    scrape_thread,
    _thread_id_from_url,
)

load_dotenv()

FORUMS_CSV_PATH = Path("forums.csv")


def _slug_from_url(url: str) -> str:
    path = urlparse(url).path.strip("/")
    if not path:
        return "forums"
    tail = path.split("/")[-1]
    cleaned = re.sub(r"\.\d+$", "", tail)
    return cleaned or "forums"


def load_forums(csv_path: Path) -> list[dict[str, str]]:
    if not csv_path.exists():
        raise FileNotFoundError(f"Forums CSV not found at {csv_path}")

    forums: list[dict[str, str]] = []
    with open(csv_path, newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            href = row.get("forum_href")
            name = row.get("forum_name") or href or "unknown"
            if not href:
                continue
            forums.append({"forum_name": name, "forum_href": absolute_url(str(href))})
    return forums


def _open_csv_append(path: str, fieldnames: list[str]):
    """Open a CSV for appending. Writes header only if the file is new/empty."""
    p = Path(path)
    write_header = not p.exists() or p.stat().st_size == 0
    f = open(p, "a", newline="", encoding="utf-8")
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    if write_header:
        writer.writeheader()
    return f, writer


def _write_thread_to_db(db_writer, *, user_cache, written_user_ids, posts, interactions, thread_row):
    """Insert one thread's data into Postgres in FK-safe order."""
    # 1. Users first (posts.user_id FK → users)
    for user_id, user in user_cache.items():
        if not user_id or user_id in written_user_ids:
            continue
        db_writer.insert_user(user)
        written_user_ids.add(user_id)

    # 2. Thread (posts.thread_id FK → threads)
    db_writer.insert_thread(thread_row)

    # 3. Posts (interactions FK → posts)
    for row in posts:
        db_writer.insert_post(row)

    # 4. Interactions last
    for interaction in interactions:
        db_writer.insert_interaction(interaction)

    # Commit per thread — safe resume point
    db_writer.commit()


def _write_thread_to_csv(*, user_cache, written_user_ids, posts, interactions, thread_row,
                          posts_writer, interactions_writer, threads_writer, users_writer):
    """Write one thread's data to CSV files."""
    for row in posts:
        posts_writer.writerow(row)

    for interaction in interactions:
        interactions_writer.writerow(interaction)

    threads_writer.writerow(thread_row)

    for user_id, user in user_cache.items():
        if not user_id or user_id in written_user_ids:
            continue
        users_writer.writerow(user)
        written_user_ids.add(user_id)


def scrape_single_forum(
    *,
    forum_name: str,
    forum_url: str,
    max_forum_pages: int | None,
    thread_limit: int | None,
    thread_page_limit: int | None,
    skip_scraped: bool = True,
    db_writer=None,
    csv_mode: bool = False,
):
    slug = _slug_from_url(forum_url)

    print(f"[main] Scraping forum '{forum_name}' ({forum_url})")

    # Load already-scraped thread IDs from DB
    scraped_ids: set[str] = set()
    if skip_scraped and db_writer:
        try:
            scraped_ids = db_writer.get_scraped_thread_ids()
            if scraped_ids:
                print(f"[main] Found {len(scraped_ids)} already-scraped threads in DB")
        except Exception as exc:
            print(f"[main] Could not load scraped thread IDs: {exc}")

    thread_urls = get_thread_list(
        forum_url,
        max_pages=max_forum_pages,
        thread_limit=thread_limit,
    )
    print(f"[main] Fetched {len(thread_urls)} thread URLs for {forum_name}")

    # Filter out threads we've already scraped
    if scraped_ids:
        new_urls = [u for u in thread_urls if _thread_id_from_url(u) not in scraped_ids]
        skipped = len(thread_urls) - len(new_urls)
        if skipped:
            print(f"[main] Skipping {skipped} already-scraped threads, {len(new_urls)} new to scrape")
        thread_urls = new_urls

    if not thread_urls:
        print("[main] No new threads to scrape — done.")
        return

    user_cache: dict[str, dict] = {}
    written_user_ids: set[str] = set()

    # Pre-populate caches with users already in DB to avoid re-fetching profiles
    if db_writer:
        try:
            existing_user_ids = db_writer.get_scraped_user_ids()
            if existing_user_ids:
                for uid in existing_user_ids:
                    user_cache[uid] = {"user_id": uid}
                written_user_ids = set(existing_user_ids)
                print(f"[main] Loaded {len(existing_user_ids)} existing users from DB — will skip profile fetches")
        except Exception as exc:
            print(f"[main] Could not load existing user IDs: {exc}")

    # CSV file handles (only opened in csv mode)
    csv_handles = []
    posts_writer = interactions_writer = threads_writer = users_writer = None

    if csv_mode:
        threads_csv_path = f"data/threads-{slug}.csv"
        posts_csv_path = f"data/posts-{slug}.csv"
        users_csv_path = f"data/users-{slug}.csv"
        interactions_csv_path = f"data/interactions-{slug}.csv"

        posts_f, posts_writer = _open_csv_append(posts_csv_path, POSTS_FIELDNAMES)
        interactions_f, interactions_writer = _open_csv_append(interactions_csv_path, INTERACTIONS_FIELDNAMES)
        threads_f, threads_writer = _open_csv_append(threads_csv_path, THREADS_FIELDNAMES)
        users_f, users_writer = _open_csv_append(users_csv_path, USERS_FIELDNAMES)
        csv_handles = [posts_f, interactions_f, threads_f, users_f]

    try:
        for i, t_url in enumerate(thread_urls, start=1):
            print(f"[main] ({i}/{len(thread_urls)}) Scraping thread: {t_url}")
            try:
                posts, interactions, thread_row = scrape_thread(
                    t_url,
                    user_cache,
                    max_pages=thread_page_limit,
                    forum_url=forum_url,
                )
            except Exception as exc:
                print(f"[main] Error scraping {t_url}: {exc}")
                continue

            if csv_mode:
                _write_thread_to_csv(
                    user_cache=user_cache,
                    written_user_ids=written_user_ids,
                    posts=posts,
                    interactions=interactions,
                    thread_row=thread_row,
                    posts_writer=posts_writer,
                    interactions_writer=interactions_writer,
                    threads_writer=threads_writer,
                    users_writer=users_writer,
                )
            else:
                _write_thread_to_db(
                    db_writer,
                    user_cache=user_cache,
                    written_user_ids=written_user_ids,
                    posts=posts,
                    interactions=interactions,
                    thread_row=thread_row,
                )
    finally:
        for fh in csv_handles:
            fh.close()

    print(f"[main] Finished forum '{forum_name}'.")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape a single forum by index from forums.csv")
    parser.add_argument(
        "--forum-index",
        type=int,
        default=0,
        help="Zero-based index inside forums.csv to scrape",
    )
    parser.add_argument(
        "--no-skip",
        action="store_true",
        help="Scrape all threads even if they already exist in the DB",
    )
    parser.add_argument(
        "--csv",
        action="store_true",
        help="Write to CSV files instead of Postgres (debug mode)",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    forums = load_forums(FORUMS_CSV_PATH)
    if not forums:
        raise RuntimeError("forums.csv is empty, run get_forums_scrape.py first")

    if args.forum_index < 0 or args.forum_index >= len(forums):
        raise IndexError(
            f"forum-index {args.forum_index} out of range (0-{len(forums) - 1})"
        )

    forum = forums[args.forum_index]
    print(
        f"[main] Loaded {len(forums)} forums from {FORUMS_CSV_PATH}; "
        f"scraping index {args.forum_index}: {forum['forum_name']}"
    )

    db_writer = None
    if not args.csv:
        db_url = os.getenv("DATABASE_URL")
        if not db_url:
            raise RuntimeError("DATABASE_URL not set. Use --csv for CSV-only mode.")
        from db.writer import DbWriter
        db_writer = DbWriter(db_url)

    try:
        scrape_single_forum(
            forum_name=forum["forum_name"],
            forum_url=forum["forum_href"],
            max_forum_pages=None,
            thread_limit=None,
            thread_page_limit=None,
            skip_scraped=not args.no_skip,
            db_writer=db_writer,
            csv_mode=args.csv,
        )
    finally:
        if db_writer:
            db_writer.close()


if __name__ == "__main__":
    main()
