"""
Normalize bare-digit post IDs in existing CSVs to the 'post-<digits>' format.

Scans data/posts-*.csv and data/interactions-*.csv, rewrites any bare-digit
post_id / replying_post_id / target_post_id values to 'post-<digits>'.
Files are updated in place.

Usage:
    uv run db/normalize_post_ids.py          # dry-run (default)
    uv run db/normalize_post_ids.py --apply  # overwrite files
"""

import argparse
import csv
import re
import tempfile
import shutil
from pathlib import Path

DATA_DIR = Path("data")

DIGIT_ONLY = re.compile(r"^\d+$")

# Which columns to normalize in each CSV type
RULES: list[tuple[str, list[str]]] = [
    ("posts-*.csv",         ["post_id"]),
    ("interactions-*.csv",  ["replying_post_id", "target_post_id"]),
]


def _normalize(value: str) -> str:
    """Prefix bare-digit values with 'post-'; leave everything else alone."""
    if value and DIGIT_ONLY.match(value):
        return f"post-{value}"
    return value


def normalize_csv(path: Path, columns: list[str], *, apply: bool) -> int:
    """Normalize the given columns in a CSV file. Returns the number of cells changed."""
    changed = 0

    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return 0
        fieldnames = list(reader.fieldnames)
        rows: list[dict] = []
        for row in reader:
            for col in columns:
                old = row.get(col, "")
                new = _normalize(old)
                if new != old:
                    row[col] = new
                    changed += 1
            rows.append(row)

    if changed == 0:
        return 0

    if apply:
        # Write to a temp file then atomically replace
        tmp_fd, tmp_path = tempfile.mkstemp(
            dir=path.parent, suffix=".csv.tmp", prefix=path.stem
        )
        try:
            with open(tmp_fd, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
            shutil.move(tmp_path, path)
        except BaseException:
            Path(tmp_path).unlink(missing_ok=True)
            raise

    return changed


def main() -> None:
    parser = argparse.ArgumentParser(description="Normalize bare-digit post IDs in CSVs")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Overwrite files in place (default is dry-run)",
    )
    args = parser.parse_args()

    if not DATA_DIR.is_dir():
        print(f"Data directory {DATA_DIR} not found — nothing to do.")
        return

    total_files = 0
    total_changed = 0

    for glob_pattern, columns in RULES:
        files = sorted(DATA_DIR.glob(glob_pattern))
        if not files:
            print(f"  No files matching {glob_pattern}")
            continue

        for path in files:
            changed = normalize_csv(path, columns, apply=args.apply)
            total_files += 1
            total_changed += changed
            status = "updated" if (args.apply and changed) else "would update" if changed else "ok"
            print(f"  {path.name}: {changed} cells {status}")

    mode = "APPLIED" if args.apply else "DRY-RUN"
    print(f"\n[{mode}] Scanned {total_files} file(s), {total_changed} cell(s) to normalize.")
    if total_changed and not args.apply:
        print("Re-run with --apply to overwrite files.")


if __name__ == "__main__":
    main()
