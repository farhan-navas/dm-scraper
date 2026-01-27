import csv
import re
from datetime import datetime
from pathlib import Path
from typing import Iterator
from urllib.parse import urljoin
from uuid import uuid4

from bs4 import BeautifulSoup

from scraper.data_model import INTERACTIONS_FIELDNAMES
from scraper.post_scraper import _normalize_reaction_name
from scraper.rate_limiter import configure_rate_limiter, fetch
from scraper.user_scraper import extract_user_id_from_profile_url

POST_HREF_PATTERN = re.compile(r"/posts/(\d+)/?")

def _current_scrape_timestamp() -> str:
    """Generate an ISO8601 timestamp (UTC) for scrape bookkeeping."""
    return datetime.now().isoformat(timespec="seconds") + "Z"

def _post_id_from_href(href: str | None) -> str | None:
    if not href:
        return None
    match = POST_HREF_PATTERN.search(href)
    if not match:
        return None
    return f"post-{match.group(1)}"

def _activity_url(profile_url: str) -> str:
    base = profile_url.rstrip("/") + "/"
    return urljoin(base, "activity")

def _iter_user_rows(users_glob: str) -> Iterator[dict]:
    for path in sorted(Path().glob(users_glob)):
        if not path.is_file():
            continue
        with path.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                yield row

def _parse_reaction_name(activity_el) -> str | None:
    reaction_span = activity_el.select_one(".reaction")
    if not reaction_span:
        return None

    name = reaction_span.get("title")
    if not name:
        text_el = reaction_span.select_one(".reaction-text")
        name = text_el.get_text(" ", strip=True) if text_el else None

    normalized = _normalize_reaction_name(name or "")
    if normalized:
        return normalized

    reaction_id = reaction_span.get("data-reaction-id")
    return reaction_id if reaction_id else None

def _parse_activity_item(activity_el, *, source_user_id: str | None, scraped_at: str) -> list[dict]:
    title_el = activity_el.select_one(".contentRow-title")
    if not title_el:
        return []

    post_link = title_el.find("a", href=POST_HREF_PATTERN)
    replying_post_id = _post_id_from_href(post_link["href"]) if post_link else None

    reaction_name = _parse_reaction_name(activity_el)

    interaction_type = None
    if reaction_name:
        interaction_type = f"reaction-{reaction_name}"
    elif "commented on the thread" in title_el.get_text(" ", strip=True):
        interaction_type = "reply"

    # Fallback classification
    if not interaction_type:
        interaction_type = "reply"

    # Activity feed does not expose thread ids reliably; leave blank rather than guessing.
    thread_id = None

    interaction = {
        "interaction_id": str(uuid4()),
        "replying_post_id": replying_post_id,
        "target_post_id": replying_post_id if interaction_type.startswith("reaction-") else None,
        "source_user_id": source_user_id,
        "target_user_id": None,
        "thread_id": thread_id,
        "interaction_type": interaction_type,
        "scraped_at": scraped_at,
    }
    return [interaction]

def parse_activity_html(html: str, *, source_user_id: str | None) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    scraped_at = _current_scrape_timestamp()

    interactions: list[dict] = []
    for activity_el in soup.select(".activity-item"):
        interactions.extend(
            _parse_activity_item(activity_el, source_user_id=source_user_id, scraped_at=scraped_at)
        )
    return interactions

def _ensure_output_path(path: Path) -> None:
    if path.parent and not path.parent.exists():
        path.parent.mkdir(parents=True, exist_ok=True)

def scrape_user_activity(
    *,
    users_glob: str,
    output_csv: Path,
    max_users: int | None = None,
    max_calls: int = 1,
    period: float = 2.0,
    cookie: str | None = None,
) -> None:
    configure_rate_limiter(max_calls=max_calls, period=period)
    _ensure_output_path(output_csv)

    cookie_dict = None
    if cookie:
        parts = [p.strip() for p in cookie.split(";") if p.strip()]
        cookie_dict = dict(part.split("=", 1) for part in parts if "=" in part)

    with output_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=INTERACTIONS_FIELDNAMES)
        writer.writeheader()

        for idx, user_row in enumerate(_iter_user_rows(users_glob), start=1):
            if max_users is not None and idx > max_users:
                break

            profile_url = user_row.get("profile_url")
            user_id = user_row.get("user_id") or extract_user_id_from_profile_url(profile_url or "")
            if not profile_url or not user_id:
                print(f"[activity] Skipping row without profile_url/user_id: {user_row}")
                continue

            activity_url = _activity_url(profile_url)
            print(f"[activity] ({idx}) Fetching activity for user {user_id} -> {activity_url}")
            try:
                html = fetch(activity_url, cookies=cookie_dict)
            except Exception as exc:
                print(f"[activity] Error fetching {activity_url}: {exc}")
                continue

            interactions = parse_activity_html(html, source_user_id=user_id)
            for interaction in interactions:
                writer.writerow(interaction)
