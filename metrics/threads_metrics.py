import csv
from collections import Counter
from pathlib import Path


def summarize_threads(path: Path) -> dict:
    totals = {"rows": 0, "unique_thread_ids": set()}
    threads_per_forum: Counter[str] = Counter()
    thread_forum_map: dict[str, str | None] = {}

    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            totals["rows"] += 1
            tid = row.get("thread_id")
            forum_url = row.get("forum_url") or "unknown"
            if tid:
                totals["unique_thread_ids"].add(tid)
                threads_per_forum[forum_url] += 1
                thread_forum_map[tid] = forum_url
    return {
        "rows": totals["rows"],
        "unique_thread_ids": len(totals["unique_thread_ids"]),
        "threads_per_forum": threads_per_forum,
        "thread_forum_map": thread_forum_map,
    }