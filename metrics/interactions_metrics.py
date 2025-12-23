import csv
from pathlib import Path
from collections import Counter

def summarize_interactions(path: Path) -> dict:
    rows = 0
    type_counts: Counter[str] = Counter()
    thread_ids: set[str] = set()

    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows += 1
            itype = row.get("interaction_type") or "unknown"
            type_counts[f"type_{itype}"] += 1
            tid = row.get("thread_id")
            if tid:
                thread_ids.add(tid)

    return {
        "rows": rows,
        "unique_threads": len(thread_ids),
        "thread_ids": thread_ids,
        **type_counts,
    }
