"""
Scrape a user's /activity page to extract reactions, comments, and thread
creation events. Handles "Load More" pagination to get full history.

Returns structured dicts ready for insertion into the interactions table.
"""

import re
from datetime import datetime
from uuid import uuid4

from bs4 import BeautifulSoup

from scraper.rate_limiter import fetch

BASE_URL = "https://www.personalitycafe.com"

POST_HREF_PATTERN = re.compile(r"/posts/(\d+)/?")

ALLOWED_REACTIONS = {
    "like", "helpful", "love", "smile", "hug",
    "haha", "wow", "face palm", "sad",
}


def _scrape_timestamp() -> str:
    return datetime.now().isoformat(timespec="seconds") + "Z"


def _post_id_from_href(href: str | None) -> str | None:
    if not href:
        return None
    match = POST_HREF_PATTERN.search(href)
    return f"post-{match.group(1)}" if match else None


def _normalize_reaction(raw: str | None) -> str | None:
    if not raw:
        return None
    text = raw.strip().lower()
    return text if text in ALLOWED_REACTIONS else None


def _is_login_page(soup: BeautifulSoup) -> bool:
    html_tag = soup.select_one("html")
    return bool(html_tag and html_tag.get("data-template") == "login")


def _parse_activity_items(soup: BeautifulSoup, source_user_id: str) -> list[dict]:
    """Parse all activity items from a single page of results."""
    scraped_at = _scrape_timestamp()
    results: list[dict] = []

    for item in soup.select(".activity-item"):
        title = item.select_one(".contentRow-title")
        if not title:
            continue

        title_text = title.get_text(" ", strip=True)

        # Extract post link
        post_link = title.find("a", href=POST_HREF_PATTERN)
        post_id = _post_id_from_href(post_link["href"]) if post_link else None

        # Extract timestamp
        time_el = item.select_one("time[datetime]")
        timestamp = time_el.get("datetime") if time_el else None

        # Determine activity type and extract fields
        if "reacted to" in title_text:
            # Reaction: "X reacted to Y's post in thread Z with Like"
            reaction_el = item.select_one(".reaction")
            reaction_name = None
            reaction_id = None
            if reaction_el:
                img = reaction_el.select_one("img")
                reaction_name = _normalize_reaction(
                    img.get("alt") if img else reaction_el.get_text(strip=True)
                )
                reaction_id = reaction_el.get("data-reaction-id")

            if not reaction_name:
                reaction_name = "like"  # fallback

            # Extract target username from title text
            target_username = None
            match = re.search(r"reacted to (.+?)'s post", title_text)
            if match:
                target_username = match.group(1).strip()

            results.append({
                "interaction_id": str(uuid4()),
                "replying_post_id": post_id,
                "target_post_id": post_id,
                "source_user_id": source_user_id,
                "target_user_id": None,  # we only have username, not ID
                "thread_id": None,
                "interaction_type": f"reaction-{reaction_name}",
                "scraped_at": scraped_at,
                # Extra fields not in interactions table but useful for enrichment
                "_target_username": target_username,
                "_reaction_id": reaction_id,
                "_timestamp": timestamp,
            })

        elif "commented on" in title_text:
            results.append({
                "interaction_id": str(uuid4()),
                "replying_post_id": post_id,
                "target_post_id": None,
                "source_user_id": source_user_id,
                "target_user_id": None,
                "thread_id": None,
                "interaction_type": "comment",
                "scraped_at": scraped_at,
                "_timestamp": timestamp,
            })

        elif "posted the thread" in title_text:
            results.append({
                "interaction_id": str(uuid4()),
                "replying_post_id": None,
                "target_post_id": None,
                "source_user_id": source_user_id,
                "target_user_id": None,
                "thread_id": None,
                "interaction_type": "thread_create",
                "scraped_at": scraped_at,
                "_timestamp": timestamp,
            })

    return results


def _get_load_more_url(soup: BeautifulSoup) -> str | None:
    """Extract the 'Load More' pagination URL if present."""
    link = soup.select_one('a[href*="before_id"]')
    if not link:
        return None
    href = link.get("href", "")
    if not href:
        return None
    if not href.startswith("http"):
        return BASE_URL + href
    return href


def scrape_user_activity(
    profile_url: str,
    user_id: str,
    *,
    max_pages: int | None = None,
) -> list[dict]:
    """
    Scrape all activity for a user by following Load More pagination.

    Returns a list of interaction dicts. Extra fields prefixed with '_'
    (like _target_username) are included for enrichment but should be
    stripped before DB insertion.

    Args:
        profile_url: The user's profile URL.
        user_id: The user's numeric ID (string).
        max_pages: Stop after this many pages (None = all pages).
    """
    activity_url = profile_url.rstrip("/") + "/activity"
    all_items: list[dict] = []
    page = 0

    while True:
        page += 1
        try:
            html = fetch(activity_url)
        except Exception as exc:
            print(f"[activity] Error fetching {activity_url}: {exc}")
            break

        soup = BeautifulSoup(html, "html.parser")

        if _is_login_page(soup):
            print(f"[activity] User {user_id} requires auth — skipping activity")
            break

        items = _parse_activity_items(soup, user_id)
        all_items.extend(items)

        if max_pages is not None and page >= max_pages:
            break

        # Follow Load More
        next_url = _get_load_more_url(soup)
        if not next_url:
            break

        activity_url = next_url

    return all_items


def strip_extra_fields(interaction: dict) -> dict:
    """Remove underscore-prefixed extra fields before DB insertion."""
    return {k: v for k, v in interaction.items() if not k.startswith("_")}
