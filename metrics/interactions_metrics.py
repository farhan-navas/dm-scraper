import csv
from pathlib import Path
from collections import Counter


def summarize_interactions(path: Path) -> dict:
    counts = Counter()
    thread_ids = set()
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            counts["rows"] += 1
            itype = row.get("interaction_type") or "unknown"
            counts[f"type_{itype}"] += 1
            tid = row.get("thread_id")
            if tid:
                thread_ids.add(tid)
    counts["unique_threads"] = len(thread_ids)
    counts["thread_ids"] = len(thread_ids)
    return counts
