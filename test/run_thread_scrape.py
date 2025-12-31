import csv

from scraper.data_model import (
    INTERACTIONS_FIELDNAMES,
    POSTS_FIELDNAMES,
    THREADS_FIELDNAMES,
    USERS_FIELDNAMES,
)
from scraper.post_scraper import scrape_thread


def main() -> None:
    thread_url = "https://www.personalitycafe.com/threads/are-you-a-nerd.1382974/"
    thread_page_limit = 1
    posts_csv_path = "data/posts.csv"
    users_csv_path = "data/users.csv"
    interactions_csv_path = "data/interactions.csv"
    threads_csv_path = "data/threads.csv"

    user_cache: dict[str, dict] = {}

    print(f"[thread-runner] Scraping {thread_url} (max_pages={thread_page_limit})")
    posts, interactions, thread_row = scrape_thread(
        thread_url,
        user_cache,
        max_pages=thread_page_limit,
    )

    with open(posts_csv_path, "w", newline="", encoding="utf-8") as posts_f:
        writer = csv.DictWriter(posts_f, fieldnames=POSTS_FIELDNAMES)
        writer.writeheader()
        for row in posts:
            writer.writerow(row)

    with open(interactions_csv_path, "w", newline="", encoding="utf-8") as interactions_f:
        writer = csv.DictWriter(interactions_f, fieldnames=INTERACTIONS_FIELDNAMES)
        writer.writeheader()
        for row in interactions:
            writer.writerow(row)

    with open(threads_csv_path, "w", newline="", encoding="utf-8") as threads_f:
        writer = csv.DictWriter(threads_f, fieldnames=THREADS_FIELDNAMES)
        writer.writeheader()
        writer.writerow(thread_row)

    with open(users_csv_path, "w", newline="", encoding="utf-8") as users_f:
        writer = csv.DictWriter(users_f, fieldnames=USERS_FIELDNAMES)
        writer.writeheader()
        for user in user_cache.values():
            writer.writerow(user)

    print(
        f"[thread-runner] Done. Wrote {posts_csv_path}, {users_csv_path}, {interactions_csv_path}, and {threads_csv_path}"
    )

if __name__ == "__main__":
    main()
