import csv
csv.field_size_limit(2147483647)

from collections import Counter
from pathlib import Path


def summarize_posts(path: Path) -> dict:
    totals = {
        "rows": 0,
        "unique_threads": set(),
        "unique_users": set(),
        "missing_user_id": 0,
        "empty_text": 0,
    }
    posts_per_thread: Counter[str] = Counter()
    thread_user_map: dict[str, set[str]] = {}

    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            totals["rows"] += 1
            tid = row.get("thread_id")
            uid = row.get("user_id")
            text = row.get("text")
            if tid:
                totals["unique_threads"].add(tid)
                posts_per_thread[tid] += 1
                thread_user_map.setdefault(tid, set())
                if uid:
                    thread_user_map[tid].add(uid)
            if uid:
                totals["unique_users"].add(uid)
            else:
                totals["missing_user_id"] += 1
            if not text:
                totals["empty_text"] += 1
    return {
        "rows": totals["rows"],
        "unique_threads": len(totals["unique_threads"]),
        "unique_users": len(totals["unique_users"]),
        "missing_user_id": totals["missing_user_id"],
        "empty_text": totals["empty_text"],
        "posts_per_thread": posts_per_thread,
        "thread_user_map": thread_user_map,
    }