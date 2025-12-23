import csv
from collections import Counter
from pathlib import Path


def summarize_users(path: Path) -> dict:
    totals = {"rows": 0, "missing_username": 0, "missing_join_date": 0}
    unique_user_ids: set[str] = set()
    gender_counts: Counter[str] = Counter()
    role_counts: Counter[str] = Counter()
    profile_fill_counts = {"mbti_type": 0, "enneagram_type": 0, "socionics": 0, "location": 0}
    user_info: dict[str, dict] = {}

    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            totals["rows"] += 1
            uid = row.get("user_id")
            username = row.get("username")
            if not username:
                totals["missing_username"] += 1
            if not row.get("join_date"):
                totals["missing_join_date"] += 1
            if uid:
                unique_user_ids.add(uid)
                info = {
                    "gender": row.get("gender"),
                    "role": row.get("role"),
                    "mbti_type": row.get("mbti_type"),
                    "enneagram_type": row.get("enneagram_type"),
                    "socionics": row.get("socionics"),
                    "location": row.get("location"),
                }
                user_info[uid] = info
                if info["gender"]:
                    gender_counts[info["gender"]] += 1
                if info["role"]:
                    role_counts[info["role"]] += 1
                for field in profile_fill_counts:
                    if info.get(field):
                        profile_fill_counts[field] += 1

    return {
        "rows": totals["rows"],
        "missing_username": totals["missing_username"],
        "missing_join_date": totals["missing_join_date"],
        "unique_user_ids": unique_user_ids,
        "gender_counts": gender_counts,
        "role_counts": role_counts,
        "profile_fill_counts": profile_fill_counts,
        "user_info": user_info,
    }