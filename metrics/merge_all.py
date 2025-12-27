import json
from pathlib import Path
from collections import Counter, defaultdict

def load_summary(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def load_user_ids(path: Path) -> set[str]:
    data = json.loads(path.read_text(encoding="utf-8"))
    ids = data.get("user_ids", [])
    return set(ids)

def merge_summaries(paths, user_id_paths=None):
    merged = {
        "posts": {"files": [], "totals": {"rows": 0, "missing_user_id": 0, "empty_text": 0, "unique_threads": 0, "unique_users": 0}},
        "interactions": {"files": [], "totals": {"rows": 0, "unique_threads": 0, "type_counts": {}}},
        "users": {"files": [], "totals": {"rows": 0, "missing_username": 0, "missing_join_date": 0, "unique_user_ids": 0},
                  "profile_richness": {"fill_counts": defaultdict(int), "fill_rates": {}},
                  "gender_distribution_overall": Counter(), "role_distribution_overall": Counter()},
        "threads": {"files": [], "totals": {"rows": 0, "unique_thread_ids": 0},
                    "threads_per_forum": {"counts": Counter(), "stats": None}},
        "forums": {"gender_distribution": defaultdict(Counter), "role_distribution": defaultdict(Counter)},
        "overall_unique_users": 0,
    }

    unique_users = set()
    unique_threads_posts = set()
    unique_threads_interactions = set()
    unique_thread_ids = set()

    # Preload explicit user IDs if provided
    if user_id_paths:
        for p in user_id_paths:
            unique_users.update(load_user_ids(p))

    for path in paths:
        s = load_summary(path)

        # Posts
        merged["posts"]["files"].extend(s["posts"]["files"])
        merged["posts"]["totals"]["rows"] += s["posts"]["totals"]["rows"]
        merged["posts"]["totals"]["missing_user_id"] += s["posts"]["totals"]["missing_user_id"]
        merged["posts"]["totals"]["empty_text"] += s["posts"]["totals"]["empty_text"]
        # These are lengths in the per-file summaries; here we sum and dedupe via sets below when IDs are present
        unique_threads_posts.update(s["posts"]["totals"].get("unique_threads", []) if isinstance(s["posts"]["totals"].get("unique_threads"), list) else [])
        unique_users.update(s["posts"]["totals"].get("unique_users", []) if isinstance(s["posts"]["totals"].get("unique_users"), list) else [])

        # Interactions
        merged["interactions"]["files"].extend(s["interactions"]["files"])
        merged["interactions"]["totals"]["rows"] += s["interactions"]["totals"]["rows"]
        for k, v in s["interactions"]["totals"]["type_counts"].items():
            merged["interactions"]["totals"]["type_counts"][k] = merged["interactions"]["totals"]["type_counts"].get(k, 0) + v
        unique_threads_interactions.update(s["interactions"]["totals"].get("unique_threads", []) if isinstance(s["interactions"]["totals"].get("unique_threads"), list) else [])

        # Users
        merged["users"]["files"].extend(s["users"]["files"])
        merged["users"]["totals"]["rows"] += s["users"]["totals"]["rows"]
        merged["users"]["totals"]["missing_username"] += s["users"]["totals"]["missing_username"]
        merged["users"]["totals"]["missing_join_date"] += s["users"]["totals"]["missing_join_date"]
        merged["users"]["profile_richness"]["fill_counts"] = {
            k: merged["users"]["profile_richness"]["fill_counts"].get(k, 0) + s["users"]["profile_richness"]["fill_counts"].get(k, 0)
            for k in set(merged["users"]["profile_richness"]["fill_counts"]) | set(s["users"]["profile_richness"]["fill_counts"])
        }
        merged["users"]["gender_distribution_overall"].update(s["users"]["gender_distribution_overall"])
        merged["users"]["role_distribution_overall"].update(s["users"]["role_distribution_overall"])
        # Only fall back to count-based placeholders when no explicit IDs were provided
        if not user_id_paths:
            unique_users.update(range(s["users"]["totals"].get("unique_user_ids", 0)))

        # Threads
        merged["threads"]["files"].extend(s["threads"]["files"])
        merged["threads"]["totals"]["rows"] += s["threads"]["totals"]["rows"]
        unique_thread_ids.update(range(s["threads"]["totals"].get("unique_thread_ids", 0)))
        merged["threads"]["threads_per_forum"]["counts"].update(s["threads"]["threads_per_forum"].get("counts", {}))

        # Forums
        for forum, counts in s.get("forums", {}).get("gender_distribution", {}).items():
            merged["forums"]["gender_distribution"][forum].update(counts)
        for forum, counts in s.get("forums", {}).get("role_distribution", {}).items():
            merged["forums"]["role_distribution"][forum].update(counts)

    merged["posts"]["totals"]["unique_threads"] = len(unique_threads_posts)
    merged["posts"]["totals"]["unique_users"] = len(unique_users)
    merged["interactions"]["totals"]["unique_threads"] = len(unique_threads_interactions)
    merged["threads"]["totals"]["unique_thread_ids"] = len(unique_thread_ids)
    merged["threads"]["threads_per_forum"]["counts"] = dict(merged["threads"]["threads_per_forum"]["counts"])
    merged["forums"]["gender_distribution"] = {k: dict(v) for k, v in merged["forums"]["gender_distribution"].items()}
    merged["forums"]["role_distribution"] = {k: dict(v) for k, v in merged["forums"]["role_distribution"].items()}
    merged["users"]["profile_richness"]["fill_counts"] = dict(merged["users"]["profile_richness"]["fill_counts"])
    merged["overall_unique_users"] = len(unique_users)
    return merged

if __name__ == "__main__":
    summaries = sorted(Path("metrics").glob("summary*.json"))
    user_id_files = sorted(Path("metrics").glob("userids*.json"))
    merged = merge_summaries(summaries, user_id_files)
    Path("overall-metrics.json").write_text(json.dumps(merged, indent=2), encoding="utf-8")
    print("Merged", len(summaries), "files -> overall-metrics.json")
    