import os
from pathlib import Path

import psycopg2
from psycopg2 import sql

DATA_DIR = Path("data").resolve()
DDL = {
    "users": """
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            username TEXT,
            profile_url TEXT,
            join_date TIMESTAMPTZ,
            role TEXT,
            gender TEXT,
            country_of_birth TEXT,
            location TEXT,
            mbti_type TEXT,
            enneagram_type TEXT,
            socionics TEXT,
            occupation TEXT,
            replies INT,
            discussions_created INT,
            reaction_score INT,
            points INT,
            media_count INT,
            showcase_count INT,
            scraped_at TIMESTAMPTZ
        );
    """,
    "threads": """
        CREATE TABLE IF NOT EXISTS threads (
            thread_id BIGINT PRIMARY KEY,
            thread_url TEXT,
            forum_url TEXT,
            first_seen TIMESTAMPTZ,
            last_seen TIMESTAMPTZ,
            scraped_at TIMESTAMPTZ
        );
    """,
    "posts": """
        CREATE TABLE IF NOT EXISTS posts (
            post_id TEXT PRIMARY KEY,
            thread_id BIGINT REFERENCES threads(thread_id),
            thread_url TEXT,
            page_url TEXT,
            user_id BIGINT REFERENCES users(user_id),
            username TEXT,
            timestamp TIMESTAMPTZ,
            text TEXT,
            scraped_at TIMESTAMPTZ
        );
    """,
    "interactions": """
        CREATE TABLE IF NOT EXISTS interactions (
            interaction_id UUID PRIMARY KEY,
            replying_post_id TEXT REFERENCES posts(post_id),
            target_post_id TEXT REFERENCES posts(post_id),
            source_user_id BIGINT REFERENCES users(user_id),
            target_user_id BIGINT REFERENCES users(user_id),
            thread_id BIGINT REFERENCES threads(thread_id),
            interaction_type TEXT,
            scraped_at TIMESTAMPTZ
        );
    """,
}

TABLE_PREFIXES = ["users", "threads", "posts", "interactions"]


def _csv_files_for(prefix: str) -> list[Path]:
    """Return CSVs matching prefix-*.csv or fallback to prefix.csv."""
    matches = sorted(DATA_DIR.glob(f"{prefix}-*.csv"))
    if not matches:
        fallback = DATA_DIR / f"{prefix}.csv"
        if fallback.exists():
            matches = [fallback]
    if not matches:
        raise FileNotFoundError(f"No CSV files found for prefix '{prefix}' in {DATA_DIR}")
    return matches

def main() -> None:
    db_str = os.environ["DATABASE_URL"]
    
    with psycopg2.connect(db_str) as conn:
        with conn.cursor() as cur:
            for ddl in DDL.values():
                cur.execute(ddl)

            for table in TABLE_PREFIXES:
                files = _csv_files_for(table)

                cur.execute(
                    sql.SQL("TRUNCATE {} RESTART IDENTITY CASCADE").format(
                        sql.Identifier(table)
                    )
                )

                staging = f"tmp_{table}"
                cur.execute(
                    sql.SQL(
                        "CREATE TEMP TABLE {} (LIKE {} INCLUDING DEFAULTS INCLUDING GENERATED)"
                    ).format(sql.Identifier(staging), sql.Identifier(table))
                )

                for path in files:
                    with path.open("r", encoding="utf-8") as f:
                        cur.copy_expert(
                            sql.SQL(
                                "COPY {} FROM STDIN WITH (FORMAT csv, HEADER true, NULL '')"
                            ).format(sql.Identifier(staging)),
                            f,
                        )

                cur.execute(
                    sql.SQL(
                        "INSERT INTO {} SELECT * FROM {} ON CONFLICT DO NOTHING"
                    ).format(sql.Identifier(table), sql.Identifier(staging))
                )
        conn.commit()

if __name__ == "__main__":
    main()
