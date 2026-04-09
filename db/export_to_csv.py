"""
Export all DB tables to CSV files in data/.

Usage:
    uv run db/export_to_csv.py                  # export all tables
    uv run db/export_to_csv.py --tables users   # export specific table(s)
    uv run db/export_to_csv.py --output-dir out # custom output directory
"""

import argparse
import os
from pathlib import Path

import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

load_dotenv()

TABLES = ["users", "threads", "posts", "interactions", "follows"]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export DB tables to CSV")
    parser.add_argument(
        "--tables",
        nargs="+",
        default=TABLES,
        help=f"Tables to export (default: all). Options: {', '.join(TABLES)}",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="data",
        help="Output directory (default: data/)",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL not set")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    conn = psycopg2.connect(db_url, connect_timeout=10)

    try:
        with conn.cursor() as cur:
            for table in args.tables:
                if table not in TABLES:
                    print(f"[export] Unknown table: {table}, skipping")
                    continue

                output_path = output_dir / f"{table}.csv"
                print(f"[export] Exporting {table}...", end=" ", flush=True)

                with open(output_path, "wb") as f:
                    cur.copy_expert(
                        sql.SQL("COPY {} TO STDOUT WITH (FORMAT csv, HEADER true)").format(
                            sql.Identifier(table)
                        ),
                        f,
                    )

                # Count rows
                cur.execute(sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(table)))
                count = cur.fetchone()[0]
                size = output_path.stat().st_size
                print(f"{count:,} rows, {size / 1024 / 1024:.1f} MB -> {output_path}")

    finally:
        conn.close()

    print("[export] Done.")


if __name__ == "__main__":
    main()
