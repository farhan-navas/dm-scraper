import os
import csv
import logging
from dotenv import load_dotenv
from pathlib import Path

import psycopg2
from psycopg2 import sql

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

load_dotenv()
DB_STR = os.getenv("DATABASE_URL")
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

class NullBytesWrapper:
    """Wrap a binary file handle and replace NULs with spaces for COPY."""

    def __init__(self, f, replace_with: bytes = b" "):
        self._f = f
        self._replace_with = replace_with

    def read(self, size: int = -1) -> bytes:
        b = self._f.read(size)
        return b.replace(b"\x00", self._replace_with) if b else b

    def readline(self, size: int = -1) -> bytes:
        b = self._f.readline(size)
        return b.replace(b"\x00", self._replace_with) if b else b

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
    if not DB_STR:
        raise RuntimeError("DATABASE_URL is not set (load_dotenv() couldn't find it either)")

    logger.info("Connecting to database…")
    try:
        conn = psycopg2.connect(DB_STR, connect_timeout=10)
    except Exception as exc:
        logger.error("Database connect failed: %s", exc)
        raise

    logger.info("Connected.")
    with conn:
        with conn.cursor() as cur:
            logger.info("Ensuring tables exist…")
            for ddl in DDL.values():
                cur.execute(ddl)

            for table in TABLE_PREFIXES:
                files = _csv_files_for(table)

                logger.info("Processing table %s with %d file(s)", table, len(files))
                cur.execute(
                    sql.SQL("TRUNCATE {} RESTART IDENTITY CASCADE").format(
                        sql.Identifier(table)
                    )
                )

                logger.info("Truncated %s", table)
                staging = f"tmp_{table}"
                cur.execute(
                    sql.SQL(
                        "CREATE TEMP TABLE {} (LIKE {} INCLUDING DEFAULTS INCLUDING GENERATED)"
                    ).format(sql.Identifier(staging), sql.Identifier(table))
                )

                logger.info("Created staging table %s", staging)
                for path in files:
                    logger.info("Copying from %s", path.name)
                    with path.open("r", encoding="utf-8", newline="") as tf:
                        reader = csv.reader(tf)
                        header = next(reader)

                    with path.open("rb") as bf:
                        cur.copy_expert(
                            sql.SQL(
                                "COPY {} ({}) FROM STDIN WITH (FORMAT csv, HEADER true, NULL '')"
                            ).format(
                                sql.Identifier(staging),
                                sql.SQL(", ").join(sql.Identifier(col) for col in header),
                            ),
                            NullBytesWrapper(bf),
                        )

                cur.execute(
                    sql.SQL(
                        "INSERT INTO {} SELECT * FROM {} ON CONFLICT DO NOTHING"
                    ).format(sql.Identifier(table), sql.Identifier(staging))
                )
                logger.info("Inserted into %s (rowcount=%s)", table, cur.rowcount)
        conn.commit()
    logger.info("Done!")

if __name__ == "__main__":
    main()
