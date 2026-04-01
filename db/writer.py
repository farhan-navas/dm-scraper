"""Direct Postgres writer for scraped data.

Inserts rows with ON CONFLICT DO NOTHING so re-runs skip duplicates.
Interactions use savepoints to gracefully handle FK violations (e.g.
target posts/users from unscraped threads).
"""

import csv
import logging
from pathlib import Path

import psycopg2
import psycopg2.errors

from scraper.data_model import (
    INTERACTIONS_FIELDNAMES,
    POSTS_FIELDNAMES,
    THREADS_FIELDNAMES,
    USERS_FIELDNAMES,
)

logger = logging.getLogger(__name__)

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
            thread_title TEXT,
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
    "follows": """
        CREATE TABLE IF NOT EXISTS follows (
            follower_id BIGINT REFERENCES users(user_id),
            followed_id BIGINT REFERENCES users(user_id),
            scraped_at TIMESTAMPTZ,
            PRIMARY KEY (follower_id, followed_id)
        );
    """,
}

# Pre-built SQL for each table
_INSERT_USER = (
    "INSERT INTO users ({cols}) VALUES ({phs}) ON CONFLICT (user_id) DO NOTHING"
).format(
    cols=", ".join(USERS_FIELDNAMES),
    phs=", ".join(["%s"] * len(USERS_FIELDNAMES)),
)

_INSERT_THREAD = (
    "INSERT INTO threads ({cols}) VALUES ({phs}) ON CONFLICT (thread_id) DO NOTHING"
).format(
    cols=", ".join(THREADS_FIELDNAMES),
    phs=", ".join(["%s"] * len(THREADS_FIELDNAMES)),
)

_INSERT_POST = (
    "INSERT INTO posts ({cols}) VALUES ({phs}) ON CONFLICT (post_id) DO NOTHING"
).format(
    cols=", ".join(POSTS_FIELDNAMES),
    phs=", ".join(["%s"] * len(POSTS_FIELDNAMES)),
)

_INSERT_INTERACTION = (
    "INSERT INTO interactions ({cols}) VALUES ({phs}) ON CONFLICT (interaction_id) DO NOTHING"
).format(
    cols=", ".join(INTERACTIONS_FIELDNAMES),
    phs=", ".join(["%s"] * len(INTERACTIONS_FIELDNAMES)),
)

# Columns that must be cast to int for BIGINT DB columns
_BIGINT_COLS = {"user_id", "thread_id", "source_user_id", "target_user_id"}


def _safe_bigint(val) -> int | None:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _clean_text(val):
    """Strip null bytes that Postgres TEXT columns reject."""
    if isinstance(val, str):
        return val.replace("\x00", "")
    return val


def _row_values(row: dict, fieldnames: list[str]) -> list:
    """Extract values from a dict in fieldname order, casting as needed."""
    vals = []
    for col in fieldnames:
        v = row.get(col)
        if col in _BIGINT_COLS:
            v = _safe_bigint(v)
        else:
            v = _clean_text(v)
        vals.append(v)
    return vals


class DbWriter:
    """Wraps a single Postgres connection with per-table insert helpers."""

    def __init__(self, db_url: str, ensure_schema: bool = True):
        logger.info("Connecting to database…")
        self.conn = psycopg2.connect(db_url, connect_timeout=10)
        if ensure_schema:
            self._ensure_tables()
        self._fk_failures = 0
        self._failed_interactions: list[dict] = []
        logger.info("DbWriter ready.")

    def _ensure_tables(self) -> None:
        with self.conn.cursor() as cur:
            cur.execute("SET lock_timeout = '3s'")
            for name, ddl in DDL.items():
                cur.execute("SAVEPOINT ddl_sp")
                try:
                    cur.execute(ddl)
                    cur.execute("RELEASE SAVEPOINT ddl_sp")
                except Exception:
                    cur.execute("ROLLBACK TO SAVEPOINT ddl_sp")
            # Schema migrations for existing tables
            for alter in [
                "ALTER TABLE threads ADD COLUMN IF NOT EXISTS thread_title TEXT",
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS bio TEXT",
            ]:
                cur.execute("SAVEPOINT alter_sp")
                try:
                    cur.execute(alter)
                    cur.execute("RELEASE SAVEPOINT alter_sp")
                except Exception:
                    cur.execute("ROLLBACK TO SAVEPOINT alter_sp")
            cur.execute("SET lock_timeout = '0'")
        self.conn.commit()

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def get_scraped_thread_ids(self) -> set[str]:
        with self.conn.cursor() as cur:
            cur.execute("SELECT thread_id FROM threads")
            return {str(row[0]) for row in cur.fetchall()}

    def get_scraped_user_ids(self) -> set[str]:
        with self.conn.cursor() as cur:
            cur.execute("SELECT user_id FROM users")
            return {str(row[0]) for row in cur.fetchall()}

    # ------------------------------------------------------------------
    # Inserts
    # ------------------------------------------------------------------

    def insert_user(self, user: dict) -> None:
        if _safe_bigint(user.get("user_id")) is None:
            return
        vals = _row_values(user, USERS_FIELDNAMES)
        with self.conn.cursor() as cur:
            cur.execute(_INSERT_USER, vals)

    def insert_thread(self, thread_row: dict) -> None:
        if _safe_bigint(thread_row.get("thread_id")) is None:
            return
        vals = _row_values(thread_row, THREADS_FIELDNAMES)
        with self.conn.cursor() as cur:
            cur.execute(_INSERT_THREAD, vals)

    def insert_post(self, post: dict) -> None:
        if not post.get("post_id"):
            return
        vals = _row_values(post, POSTS_FIELDNAMES)
        with self.conn.cursor() as cur:
            cur.execute(_INSERT_POST, vals)

    def insert_interaction(self, interaction: dict) -> None:
        vals = _row_values(interaction, INTERACTIONS_FIELDNAMES)
        with self.conn.cursor() as cur:
            cur.execute("SAVEPOINT interaction_sp")
            try:
                cur.execute(_INSERT_INTERACTION, vals)
                cur.execute("RELEASE SAVEPOINT interaction_sp")
            except psycopg2.errors.ForeignKeyViolation as exc:
                cur.execute("ROLLBACK TO SAVEPOINT interaction_sp")
                self._fk_failures += 1
                self._failed_interactions.append(interaction)

    # ------------------------------------------------------------------
    # Transaction helpers
    # ------------------------------------------------------------------

    def commit(self) -> None:
        self.conn.commit()

    def retry_failed_interactions(self, forum_slug: str = "unknown") -> None:
        """Retry interactions that failed due to FK violations.

        Call this after all forums are scraped — referenced posts/users
        from other forums may now exist in the DB. Any that still fail
        are saved to db_logs/failed_interactions-<forum_slug>.csv for
        future retries.
        """
        if not self._failed_interactions:
            return

        total = len(self._failed_interactions)
        logger.info("Retrying %d failed interaction(s)…", total)

        still_failing: list[dict] = []
        with self.conn.cursor() as cur:
            for interaction in self._failed_interactions:
                vals = _row_values(interaction, INTERACTIONS_FIELDNAMES)
                cur.execute("SAVEPOINT retry_sp")
                try:
                    cur.execute(_INSERT_INTERACTION, vals)
                    cur.execute("RELEASE SAVEPOINT retry_sp")
                except psycopg2.errors.ForeignKeyViolation:
                    cur.execute("ROLLBACK TO SAVEPOINT retry_sp")
                    still_failing.append(interaction)

        self.conn.commit()
        recovered = total - len(still_failing)
        logger.info(
            "Retry complete: %d recovered, %d still failing", recovered, len(still_failing)
        )

        if still_failing:
            log_dir = Path("db_logs")
            log_dir.mkdir(exist_ok=True)
            csv_path = log_dir / f"failed_interactions-{forum_slug}.csv"
            with csv_path.open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=INTERACTIONS_FIELDNAMES)
                writer.writeheader()
                writer.writerows(still_failing)
            logger.info("Saved %d failed interactions to %s", len(still_failing), csv_path)

        self._failed_interactions.clear()
        self._fk_failures = len(still_failing)

    def close(self) -> None:
        if self._fk_failures:
            logger.warning(
                "Skipped %d interaction(s) due to FK violations", self._fk_failures
            )
        try:
            self.conn.commit()
        except Exception:
            pass
        self.conn.close()
