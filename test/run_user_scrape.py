import csv

from scraper.data_model import USERS_FIELDNAMES
from scraper.user_scraper import fetch_user_profile

def main() -> None:
    user_urls = [
        "https://www.personalitycafe.com/members/daleks_exterminate.39616/"
    ]
    users_csv_path = "users.csv"

    users: dict[str, dict] = {}

    for url in user_urls:
        print(f"[user-runner] Scraping profile {url}")
        try:
            profile = fetch_user_profile(url)
        except Exception as exc:  # individualized fetch failures should not crash the script
            print(f"[user-runner] Error fetching {url}: {exc}")
            continue

        if not profile:
            print(f"[user-runner] No data returned for {url}; skipping")
            continue

        key = profile.get("user_id") or url
        users[key] = profile

    with open(users_csv_path, "w", newline="", encoding="utf-8") as users_f:
        writer = csv.DictWriter(users_f, fieldnames=USERS_FIELDNAMES)
        writer.writeheader()
        for row in users.values():
            writer.writerow(row)

    print(f"[user-runner] Done. Wrote {len(users)} user rows to {users_csv_path}")

if __name__ == "__main__":
    main()
