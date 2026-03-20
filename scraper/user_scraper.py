import re

from datetime import datetime
from urllib.parse import urlparse
from bs4 import BeautifulSoup

from scraper.rate_limiter import fetch

def _safe_text(element) -> str | None:
    if not element:
        return None
    return element.get_text(" ", strip=True)

def extract_user_id_from_profile_url(profile_url: str) -> str | None:
    """
    Extract a stable user_id from a XenForo-style member URL.
    Examples:
      /members/some-user.12345/
      https://.../members/some-user.12345/
    Returns the numeric ID as string, or None if not found.
    """
    if not profile_url:
        return None

    path = urlparse(profile_url).path

    # Typical XenForo: /members/username.12345/
    m = re.search(r'\.(\d+)/?$', path)
    if m:
        return m.group(1)

    # Fallback: last group of digits in path
    m = re.search(r'/(\d+)/?$', path)
    if m:
        return m.group(1)

    return None

def _clean_int(value: str | None) -> int | None:
    if not value:
        return None
    digits = re.sub(r"[^0-9]", "", value)
    return int(digits) if digits else None

def _as_string(value):
    """Convert BeautifulSoup attribute values (which may be lists) to strings."""
    if value is None:
        return None
    if isinstance(value, (list, tuple)):
        if not value:
            return None
        value = value[0]
    return str(value)

def _collect_stats(pairs: list) -> dict[str, str]:
    stats: dict[str, str] = {}
    for dl in pairs:
        dt = dl.find("dt")
        dd = dl.find("dd")
        if not dt or not dd:
            continue
        label = dt.get_text(strip=True).lower()
        value = dd.get_text(" ", strip=True)
        stats[label] = value
    return stats

def _fallback_username(profile_url: str) -> str | None:
    path = urlparse(profile_url).path.rstrip("/")
    if "." in path:
        return path.split("/")[-1].split(".")[0]
    return path.split("/")[-1] or None

def _extract_location_from_header(soup: BeautifulSoup) -> str | None:
    link = soup.select_one(".memberHeader-blurb a[href*='location-info']")
    if link:
        return link.get_text(strip=True)

    blurb = soup.select_one(".memberHeader-blurb")
    if not blurb:
        return None

    text = blurb.get_text(" ", strip=True)
    match = re.search(r"from\s+(.*)", text, re.IGNORECASE)
    return match.group(1).strip(" .") if match else None

def _build_user_record(
    *,
    user_id: str | None,
    profile_url: str,
    username: str | None,
    join_date: str | None,
    role: str | None,
    stats: dict[str, str],
    gender: str | None = None,
    country_of_birth: str | None = None,
    location: str | None = None,
    mbti_type: str | None = None,
    enneagram_type: str | None = None,
    socionics: str | None = None,
    occupation: str | None = None,
) -> dict:
    def stat_value(label: str) -> int | None:
        return _clean_int(stats.get(label))

    scraped_at = datetime.now().isoformat(timespec="seconds") + "Z"

    return {
        "user_id": user_id,
        "username": username or _fallback_username(profile_url),
        "profile_url": profile_url,
        "join_date": join_date,
        "role": role,
        "gender": gender,
        "country_of_birth": country_of_birth,
        "location": location,
        "mbti_type": mbti_type,
        "enneagram_type": enneagram_type,
        "socionics": socionics,
        "occupation": occupation,
        "replies": stat_value("replies"),
        "discussions_created": stat_value("discussions created"),
        "reaction_score": stat_value("reaction score"),
        "points": stat_value("points"),
        "media_count": stat_value("media"),
        "showcase_count": stat_value("showcase"),
        "scraped_at": scraped_at,
    }

def _has_meaningful_profile_data(user: dict) -> bool:
    metric_keys = [
        "replies",
        "discussions_created",
        "reaction_score",
        "points",
        "media_count",
        "showcase_count",
    ]
    if any(user.get(k) is not None for k in metric_keys):
        return True
    return bool(user.get("join_date") or user.get("role"))

def parse_user_about_page(html: str) -> dict[str, str | None]:
    """Extract optional demographic/profile fields from the About tab."""
    soup = BeautifulSoup(html, "html.parser")
    details: dict[str, str | None] = {}

    def _label_key(raw: str) -> str:
        cleaned = re.sub(r"[:\s]+", " ", raw.lower()).strip()
        return re.sub(r"[^a-z0-9 ]", "", cleaned)

    for row in soup.select(".flex-row"):
        label_el = row.select_one(".about-identifier")
        if not label_el:
            continue
        raw_label = _safe_text(label_el)
        if not raw_label:
            continue
        key_hint = _label_key(raw_label)

        value_el = row.select_one(".about-content") or row.select_one(".about-custom-content")
        value = _safe_text(value_el)
        if not value:
            continue

        if key_hint.startswith("location"):
            details["location"] = value
        elif key_hint.startswith("gender"):
            details["gender"] = value
        elif "myers briggs" in key_hint or key_hint == "mbti" or "type indicator" in key_hint:
            details["mbti_type"] = value
        elif "enneagram" in key_hint:
            details["enneagram_type"] = value
        elif "country of birth" in key_hint:
            details["country_of_birth"] = value
        elif "socionics" in key_hint:
            details["socionics"] = value
        elif "occupation" in key_hint:
            details["occupation"] = value

    return details

def _merge_user_details(user: dict, extra: dict[str, str | None]) -> None:
    for key, value in extra.items():
        if value:
            user[key] = value


def parse_user_profile_page(html: str, profile_url: str, user_id: str | None) -> dict | None:
    """Attempt to extract user data from the full profile page."""
    soup = BeautifulSoup(html, "html.parser")

    username = None
    header = soup.select_one("h1.p-title-value") or soup.select_one(".memberHeader-title")
    if header:
        username = header.get_text(strip=True)
    if not username:
        name_el = soup.select_one(".memberHeader-content .username")
        if name_el:
            username = name_el.get_text(strip=True)

    role = None
    role_el = soup.select_one(".memberHeader-content .userTitle") or soup.select_one(".userTitle")
    if role_el:
        role = role_el.get_text(strip=True)

    join_date = None
    time_el = soup.select_one(".memberHeader-content time") or soup.find("time", attrs={"itemprop": "dateCreated"})
    if time_el:
        join_date = _as_string(time_el.get("datetime")) or time_el.get_text(strip=True)

    location = _extract_location_from_header(soup)

    stats = _collect_stats(soup.select("dl.pairs"))
    if not join_date:
        join_date = stats.get("joined")

    user = _build_user_record(
        user_id=user_id,
        profile_url=profile_url,
        username=username,
        join_date=join_date,
        role=role,
        stats=stats,
        location=location,
    )

    return user if _has_meaningful_profile_data(user) else None


def parse_user_tooltip(html: str, profile_url: str, user_id: str | None) -> dict:
    """Parse tooltip HTML for a member into a structured dict."""
    soup = BeautifulSoup(html, "html.parser")
    tooltip = soup.select_one(".memberTooltip")

    username = None
    if tooltip:
        name_el = tooltip.select_one(".memberTooltip-name a.username")
        if name_el:
            username = name_el.get_text(strip=True)

    role = None
    if tooltip:
        role_el = tooltip.select_one(".userTitle")
        if role_el:
            role = role_el.get_text(strip=True)

    join_date = None
    if tooltip:
        time_el = tooltip.select_one(".memberTooltip-blurb time")
        if time_el:
            join_date = _as_string(time_el.get("datetime")) or time_el.get_text(strip=True)

    stats = _collect_stats(tooltip.select(".memberTooltip-stats dl") if tooltip else [])

    return _build_user_record(
        user_id=user_id,
        profile_url=profile_url,
        username=username,
        join_date=join_date,
        role=role,
        stats=stats,
    )


def fetch_user_profile(profile_url: str) -> dict | None:
    """Prefer full profile page; fallback to tooltip when blocked."""
    user_id = extract_user_id_from_profile_url(profile_url)
    if not user_id:
        print(f"[user] Could not parse user_id from {profile_url}")
        return None

    about_data: dict[str, str | None] = {}
    about_url = profile_url.rstrip("/") + "/about"
    try:
        about_html = fetch(about_url)
        about_data = parse_user_about_page(about_html)
    except Exception as exc:  # noqa: BLE001 - enrichment is optional
        print(f"[user] Error fetching about tab {about_url}: {exc}")

    try:
        profile_html = fetch(profile_url)
    except Exception as exc:  # noqa: BLE001 - fallback to tooltip on any failure
        print(f"[user] Error fetching profile page {profile_url}: {exc}")
        profile_html = None

    if profile_html:
        profile = parse_user_profile_page(profile_html, profile_url, user_id)
        if profile:
            if about_data:
                _merge_user_details(profile, about_data)
            return profile
        print(f"[user] Profile page lacked data for {profile_url}, falling back to tooltip")

    tooltip_url = profile_url.rstrip("/") + "/tooltip"
    print(f"[user] Fetching tooltip {tooltip_url} (user_id={user_id})")
    tooltip_html = fetch(tooltip_url)
    tooltip_user = parse_user_tooltip(tooltip_html, profile_url, user_id)
    if about_data and tooltip_user:
        _merge_user_details(tooltip_user, about_data)
    return tooltip_user


def get_or_fetch_user(profile_url: str, user_cache: dict[str, dict]) -> dict | None:
    """
    Returns a user dict. Uses cache to avoid refetching profiles.
    user_cache: {user_id: user_dict}
    """
    if not profile_url:
        return None

    user_id = extract_user_id_from_profile_url(profile_url)
    if not user_id:
        return None

    if user_id in user_cache:
        return user_cache[user_id]

    profile = fetch_user_profile(profile_url)
    if profile:
        user_cache[user_id] = profile
    return profile
