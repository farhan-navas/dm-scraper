import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from posts_metrics import summarize_posts
from interactions_metrics import summarize_interactions
from users_metrics import summarize_users
from threads_metrics import summarize_threads

def main():
    data_dir = Path("data")
    paths = sorted(data_dir.glob("*.csv"))
    posts_paths = [p for p in paths if p.name.startswith("posts-")]
    interactions_paths = [p for p in paths if p.name.startswith("interactions-")]
    users_paths = [p for p in paths if p.name.startswith("users-")]
    threads_paths = [p for p in paths if p.name.startswith("threads-")]

    posts_data = {"files": [], "totals": {"rows": 0, "missing_user_id": 0, "empty_text": 0}, "unique_threads": set(), "unique_users": set()}
    interactions_data = {"files": [], "totals": {"rows": 0}, "unique_threads": set(), "type_counts": {}}
    users_data = {"files": [], "totals": {"rows": 0, "missing_username": 0, "missing_join_date": 0}, "unique_user_ids": set()}
    threads_data = {"files": [], "totals": {"rows": 0}, "unique_thread_ids": set()}

    thread_forum_map: dict[str, str | None] = {}
    threads_per_forum: Counter[str] = Counter()
    posts_per_thread: Counter[str] = Counter()
    user_info: dict[str, dict[str, Any]] = {}

    forum_gender_counts: dict[str, Counter[str]] = defaultdict(Counter)
    forum_role_counts: dict[str, Counter[str]] = defaultdict(Counter)
    forum_user_seen: dict[str, set[str]] = defaultdict(set)
    overall_gender_counts: Counter[str] = Counter()
    overall_role_counts: Counter[str] = Counter()

    profile_fill_counts = {"mbti_type": 0, "enneagram_type": 0, "socionics": 0, "location": 0}

    # Pass 1: threads
    for path in threads_paths:
        name = path.name
        print(f"[threads] processing {name}")
        res = summarize_threads(path)
        threads_data["files"].append({
            "file": name,
            "rows": res["rows"],
            "unique_thread_ids": res["unique_thread_ids"],
            "threads_per_forum": dict(res["threads_per_forum"]),
        })
        threads_data["totals"]["rows"] += res["rows"]
        threads_data["unique_thread_ids"].update(res["thread_forum_map"].keys())
        thread_forum_map.update(res["thread_forum_map"])
        threads_per_forum.update(res["threads_per_forum"])

    # Pass 2: users
    for path in users_paths:
        name = path.name
        print(f"[users] processing {name}")
        res = summarize_users(path)
        users_data["files"].append({
            "file": name,
            "rows": res["rows"],
            "missing_username": res["missing_username"],
            "missing_join_date": res["missing_join_date"],
        })
        users_data["totals"]["rows"] += res["rows"]
        users_data["totals"]["missing_username"] += res.get("missing_username", 0)
        users_data["totals"]["missing_join_date"] += res.get("missing_join_date", 0)
        users_data["unique_user_ids"].update(res["unique_user_ids"])
        user_info.update(res["user_info"])
        profile_fill_counts = {
            k: profile_fill_counts.get(k, 0) + res["profile_fill_counts"].get(k, 0)
            for k in profile_fill_counts
        }
        overall_gender_counts.update(res["gender_counts"])
        overall_role_counts.update(res["role_counts"])

    # Pass 3: posts
    for path in posts_paths:
        name = path.name
        print(f"[posts] processing {name}")
        res = summarize_posts(path)
        posts_data["files"].append({
            "file": name,
            "rows": res["rows"],
            "unique_threads": res["unique_threads"],
            "unique_users": res["unique_users"],
            "missing_user_id": res["missing_user_id"],
            "empty_text": res["empty_text"],
        })
        posts_data["totals"]["rows"] += res["rows"]
        posts_data["totals"]["missing_user_id"] += res["missing_user_id"]
        posts_data["totals"]["empty_text"] += res["empty_text"]
        posts_data["unique_threads"].update(res["thread_user_map"].keys())

        # ADD IF USERS DOES NOT HAVE ALL USER INFO!
        # for uids in res["thread_user_map"].values():
        #     posts_data["unique_users"].update(uids)
        
        posts_per_thread.update(res["posts_per_thread"])
        for tid, uids in res["thread_user_map"].items():
            forum = thread_forum_map.get(tid)
            if not forum:
                continue
            for uid in uids:
                if uid in forum_user_seen[forum]:
                    continue
                forum_user_seen[forum].add(uid)
                info = user_info.get(uid)
                if not info:
                    raise ValueError(f"Missing user_info for user_id={uid} (thread_id={tid}, forum={forum})")
                if info.get("gender"):
                    forum_gender_counts[forum][info["gender"]] += 1
                if info.get("role"):
                    forum_role_counts[forum][info["role"]] += 1

    # Pass 4: interactions
    for path in interactions_paths:
        name = path.name
        print(f"[interactions] processing {name}")
        res = summarize_interactions(path)
        interactions_data["files"].append({"file": name, **{k: v for k, v in res.items() if k != "thread_ids"}})
        interactions_data["totals"]["rows"] += res.get("rows", 0)
        for key, val in res.items():
            if key.startswith("type_"):
                interactions_data["type_counts"][key] = interactions_data["type_counts"].get(key, 0) + val
        interactions_data["unique_threads"].update(res.get("thread_ids", set()))

    posts_per_thread_stats = None
    if posts_per_thread:
        counts = list(posts_per_thread.values())
        posts_per_thread_stats = {
            "min": min(counts),
            "max": max(counts),
            "avg": sum(counts) / len(counts),
        }

    threads_per_forum_stats = None
    if threads_per_forum:
        counts = list(threads_per_forum.values())
        threads_per_forum_stats = {
            "min": min(counts),
            "max": max(counts),
            "avg": sum(counts) / len(counts),
        }

    profile_fill_rates = {}
    total_users = users_data["totals"]["rows"] or 1
    for field, filled in profile_fill_counts.items():
        profile_fill_rates[field] = filled / total_users

    # Use user IDs from users CSVs only (ignore post-derived users)
    posts_data["unique_users"] = set(users_data["unique_user_ids"])

    output: dict[str, Any] = {
        "posts": {
            "files": posts_data["files"],
            "totals": {
                **posts_data["totals"],
                "unique_threads": len(posts_data["unique_threads"]),
                "unique_users": len(posts_data["unique_users"]),
            },
            "per_thread_counts": posts_per_thread_stats,
        },
        "interactions": {
            "files": interactions_data["files"],
            "totals": {
                "rows": interactions_data["totals"]["rows"],
                "unique_threads": len(interactions_data["unique_threads"]),
                "type_counts": interactions_data["type_counts"],
            },
        },
        "users": {
            "files": users_data["files"],
            "totals": {
                **users_data["totals"],
                "unique_user_ids": len(users_data["unique_user_ids"]),
            },
            "profile_richness": {
                "fill_counts": profile_fill_counts,
                "fill_rates": profile_fill_rates,
            },
            "gender_distribution_overall": dict(overall_gender_counts),
            "role_distribution_overall": dict(overall_role_counts),
        },
        "threads": {
            "files": threads_data["files"],
            "totals": {
                "rows": threads_data["totals"]["rows"],
                "unique_thread_ids": len(threads_data["unique_thread_ids"]),
            },
            "threads_per_forum": {
                "counts": dict(threads_per_forum),
                "stats": threads_per_forum_stats,
            },
        },
        "forums": {
            "gender_distribution": {forum: dict(counts) for forum, counts in forum_gender_counts.items()},
            "role_distribution": {forum: dict(counts) for forum, counts in forum_role_counts.items()},
        },
        "overall_unique_users": len(users_data["unique_user_ids"]),
    }

    out_path = Path("metrics/summary.json")
    out_path.write_text(json.dumps(output, indent=2), encoding="utf-8")

if __name__ == "__main__":
    main()
