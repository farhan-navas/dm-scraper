from pathlib import Path

from scraper.user_activity_scraper import scrape_user_activity


def main() -> None:
    users_glob = "data/users-*.csv"
    output_csv = Path("data/interactions.csv")
    max_users = None  # set an int to limit for testing
    max_calls = 1
    period = 2.0
    cookie = None  # optionally set auth cookie string if required

    scrape_user_activity(
        users_glob=users_glob,
        output_csv=output_csv,
        max_users=max_users,
        max_calls=max_calls,
        period=period,
        cookie=cookie,
    )


if __name__ == "__main__":
    main()
