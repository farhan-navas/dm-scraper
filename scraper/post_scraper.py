import os
import hashlib
import json
import re
from datetime import datetime
from urllib.parse import urlparse, urlencode
from uuid import uuid4

from bs4 import BeautifulSoup

from scraper.rate_limiter import fetch
from scraper.user_scraper import get_or_fetch_user, extract_user_id_from_profile_url

BASE_URL = "https://www.personalitycafe.com"

# Endpoint for loading hidden nested replies.
LOAD_MORE_POSTS_PATH = "/threads/load-more-posts/"

# Thread index selectors
THREAD_CARD_SELECTOR = "div.structItem--thread"
THREAD_LINK_SELECTOR = "h3.structItem-title a"
NEXT_PAGE_SELECTOR = "a.pageNav-jump--next"

# Post selectors
POST_SELECTOR = "article.js-post, div.MessageCard.js-post"
TOGGLE_REPLIES_SELECTOR = ".toggle-replies-button"
NESTED_REPLY_LABEL_SELECTOR = ".js-nested-reply-label:not(.hidden)"
USERNAME_SELECTOR = ".MessageCard__user-info__name"
BODY_SELECTOR = ".message-body .bbWrapper"
QUOTE_BLOCK_SELECTOR = "blockquote.bbCodeBlock--quote"
QUOTE_SOURCE_LINK_SELECTOR = ".bbCodeBlock-sourceJump"

# Reaction selectors
REACTION_BAR_SELECTOR = ".california-reaction-bar"
ALLOWED_REACTIONS = {
    "like",
    "helpful",
    "love",
    "smile",
    "hug",
    "haha",
    "wow",
    "face palm",
    "sad",
}

def absolute_url(href: str) -> str:
    if href.startswith("http"):
        return href
    if href.startswith("/"):
        return BASE_URL + href
    return BASE_URL + "/" + href.lstrip("/")

def _is_member_link(href: str | None) -> bool:
    """Return True when href looks like a member profile link."""
    return bool(href and "/members/" in href)

def _current_scrape_timestamp() -> str:
    """Return an ISO8601 timestamp used for row-level bookkeeping."""
    return datetime.now().isoformat(timespec="seconds") + "Z"

def _thread_id_from_url(thread_url: str) -> str:
    """Derive a stable thread identifier from the thread URL."""
    path = urlparse(thread_url).path.rstrip("/")
    match = re.search(r"\.(\d+)$", path)
    if match:
        return match.group(1)
    match = re.search(r"(\d+)$", path)
    if match:
        return match.group(1)
    return hashlib.sha1(thread_url.encode("utf-8", "ignore")).hexdigest()[:16]


def _normalize_reaction_name(raw) -> str | None:
    """Lowercase a reaction label and drop anything outside the allowed set."""
    if raw is None:
        return None
    text = str(raw).strip().lower()
    if not text:
        return None
    if text in ALLOWED_REACTIONS:
        return text
    return None

def _load_nested_replies_for_label(label, request_uri: str) -> list[str]:
    """Fetch hidden replies for a single nested reply toggle label.

    Returns a list of HTML fragments (strings) for child replies; empty list on failure.
    """
    parent_post_id = label.get("parent-post")
    parent_level = label.get("parent-level")
    thread_id = label.get("thread-id")

    if not parent_post_id or not thread_id:
        return []

    # Keep request lean;
    params = {
        "parent_post_id": parent_post_id,
        "parent_post_level": parent_level,
        "thread_id": thread_id,
        "_xfRequestUri": request_uri,
        "_xfWithData": 1,
        "_xfResponseType": "json",
    }

    url = BASE_URL + LOAD_MORE_POSTS_PATH + "?" + urlencode(params, doseq=True)

    try:
        resp_text = fetch(url)
        data = json.loads(resp_text)
    except Exception as exc:
        print(f"[nested] Failed to load replies for parent {parent_post_id}: {exc}")
        return []

    # XenForo returns {'html': {'content': '<div>...</div>'}}
    html_fragments: list[str] = []
    if isinstance(data, dict):
        html_block = data.get("html") or {}
        if isinstance(html_block, dict):
            content = html_block.get("content")
            if content:
                html_fragments.append(content)
        # Some variants return 'messages'
        messages = data.get("messages")
        if isinstance(messages, list):
            html_fragments.extend([m for m in messages if isinstance(m, str)])
    return html_fragments

def _inject_nested_replies(soup: BeautifulSoup, page_url: str) -> None:
    """Find visible nested-reply toggles, fetch their children, and append to DOM."""
    parsed = urlparse(page_url)
    request_uri = parsed.path
    if parsed.query:
        request_uri += "?" + parsed.query

    # Keep fetching until no new visible reply toggles remain, so nested "show more"
    # controls that appear inside loaded fragments are also expanded.
    num_f = 0
    while True:
        labels = [
            label for label in soup.select(NESTED_REPLY_LABEL_SELECTOR)
        ]
        if not labels:
            break

        for label in labels:
            print(f"[replies] the current label is for post-id: {label['parent-post']}")

            fragments = _load_nested_replies_for_label(label, request_uri=request_uri)
            if not fragments:
                continue

            container = label.find_previous("div", class_="js-nested-children-container")
            if not container:
                continue

            for fragment in fragments:
                num_f += 1
                frag_soup = BeautifulSoup(fragment, "html.parser")
                print(f"[fragments] frags are {frag_soup}")
                for child in frag_soup.contents:
                    container.append(child)

            # Mark as hidden so we don't request the same label again on later passes.
            label_classes = label.get("class") or []
            if "hidden" not in label_classes:
                label["class"] = label_classes + ["hidden"] # type: ignore
            print(f"[labels] {label.get('class')}")
    print(f"[FRAGS] the number of frags in total is {num_f}")

def _parse_post_id_from_quote_link(link) -> str | None:
    if not link:
        return None
    selector = link.get("data-content-selector")
    if selector:
        match = re.search(r"post-(\d+)", selector)
        if match:
            return match.group(1)
    href = link.get("href")
    if href:
        match = re.search(r"(?:id=|post-)(\d+)", href)
        if match:
            return match.group(1)
    return None

def _clean_quote_username(raw: str | None) -> str | None:
    if not raw:
        return None
    cleaned = raw.strip()
    cleaned = re.sub(r"\s*said:?$", "", cleaned, flags=re.IGNORECASE)
    return cleaned or None

def _extract_quote_targets(post_div) -> list[dict]:
    quotes: list[dict] = []
    for block in post_div.select(QUOTE_BLOCK_SELECTOR):
        link = block.select_one(QUOTE_SOURCE_LINK_SELECTOR)
        target_post_id = _parse_post_id_from_quote_link(link)
        username = _clean_quote_username(link.get_text(" ", strip=True) if link else None)
        if not target_post_id and not username:
            continue
        quotes.append({
            "target_post_id": target_post_id,
            "target_username": username,
        })
    return quotes

def _extract_mentions(body_el) -> list[dict]:
    mentions: list[dict] = []
    if not body_el:
        return mentions

    seen: set[tuple[str | None, str | None]] = set()
    for link in body_el.select("a"):
        href = link.get("href")
        if not href or "/members/" not in href:
            continue
        classes = link.get("class") or []
        if not link.get("data-user-id") and not any(cls.startswith("username") for cls in classes):
            continue
        profile_url = absolute_url(str(href))
        username = link.get_text(strip=True) or None
        key = (profile_url, username)
        if key in seen:
            continue
        seen.add(key)
        mentions.append({
            "profile_url": profile_url,
            "username": username,
            "user_id": extract_user_id_from_profile_url(profile_url),
        })
    return mentions

def _extract_reaction_bar(post_div) -> dict | None:
    """try to extract a reaction bar from a post"""
    bar = post_div.select_one(REACTION_BAR_SELECTOR)
    if not bar:
        return None

    reaction_types: list[dict] = []
    for span in bar.select("ul.reactionSummary span.reaction"):
        rid = span.get("data-reaction-id") or span.get("data-reactionid")
        name = None
        img = span.find("img", class_="reaction-sprite")
        if img:
            name = img.get("alt") or img.get("title")
        if not name:
            name = span.get("title")
        reaction_types.append({
            "reaction_id": str(rid) if rid else None,
            "reaction": _normalize_reaction_name(name),
        })

    link = bar.select_one("a.reactionsBar-link")
    overlay_url = absolute_url(str(link.get("href"))) if link and link.get("href") else None
    preview_names = [el.get_text(strip=True) for el in (link.select("bdi") if link else []) if el.get_text(strip=True)]
    others_count = 0
    if link:
        text = link.get_text(" ", strip=True)
        match = re.search(r"and\s+(\d+)\s+others", text)
        if match:
            others_count = int(match.group(1))

    return {
        "overlay_url": overlay_url,
        "reaction_types": reaction_types,
        "preview_names": preview_names,
        "others_count": others_count,
    }

def _parse_reaction_overlay(url: str) -> list[dict]:
    cookie_val = os.environ.get("CDNCSRF")
    cookies = {"cdncsrf": cookie_val}
    try:
        html = fetch(url, cookies=cookies)
    except Exception as exc:
        print(f"[reactions] Failed to fetch overlay {url}: {exc}")
        return []

    soup = BeautifulSoup(html, "html.parser")
    entries: list[dict] = []
    for row in soup.select(".block-row"):
        reaction_span = row.select_one(".reaction")
        rid = reaction_span.get("data-reaction-id") if reaction_span else None
        reaction_name = None
        if reaction_span:
            img = reaction_span.find("img")
            if img:
                reaction_name = img.get("alt") or img.get("title")
            if not reaction_name:
                reaction_name = reaction_span.get("title")

        user_link = row.select_one("a[href*='/members/']")
        profile_url = absolute_url(str(user_link.get("href"))) if user_link and user_link.get("href") else None
        username = user_link.get_text(" ", strip=True) if user_link else None
        entries.append({
            "reaction_id": str(rid) if rid else None,
            "reaction": _normalize_reaction_name(reaction_name),
            "profile_url": profile_url,
            "username": username,
            "user_id": extract_user_id_from_profile_url(profile_url) if profile_url else None,
        })

    return entries

def get_thread_list(
    forum_url: str,
    max_pages: int | None,
    thread_limit: int | None = 5,
):
    """
    Scrape a forum section index to collect thread URLs.
    `max_pages` prevents infinite crawling; increase carefully.
    `thread_limit` stops once N unique threads are gathered (default 5).
    """
    seen = set()
    ordered_threads: list[str] = []
    page_url = forum_url
    page = 1

    while True:
        print(f"[threads] Fetching forum index page {page}: {page_url}")
        html = fetch(page_url)
        soup = BeautifulSoup(html, "html.parser")

        for card in soup.select(THREAD_CARD_SELECTOR):
            link = card.select_one(THREAD_LINK_SELECTOR)
            if not link:
                continue
            href = link.get("href")
            if isinstance(href, list):
                href = href[0]
            if not href:
                continue
            url = absolute_url(str(href))
            if url in seen:
                continue
            seen.add(url)
            ordered_threads.append(url)
            if thread_limit is not None and len(ordered_threads) >= thread_limit:
                break

        if thread_limit is not None and len(ordered_threads) >= thread_limit:
            break
            
        if max_pages is not None and page >= max_pages:
            break

        # Find "next page" (if any)
        next_link = soup.select_one(NEXT_PAGE_SELECTOR)
        if not next_link:
            break

        next_href = next_link.get("href")
        if not next_href:
            break

        page_url = absolute_url(str(next_href))
        page += 1

    print(f"[threads] Collected {len(ordered_threads)} thread URLs.")
    return ordered_threads

def _extract_post_id(post_div) -> str | None:
    """
    Try to extract a stable post_id from attributes.
    Common patterns: data-content, id="js-post-123", id="post-123", etc.
    """
    # 1) data-content
    pid = post_div.get("data-content")
    if pid:
        return str(pid)

    # 2) id with digits
    elem_id = post_div.get("id")
    if elem_id:
        m = re.search(r"(\d+)$", elem_id)
        if m:
            return m.group(1)

    return None

def parse_posts_from_page(soup: BeautifulSoup):
    """
    Parse posts from one thread page soup.
    Returns a list of dicts:
        {
            "post_id",
            "username",
            "profile_url",
            "timestamp",
            "text",
        }
    """
    posts = []

    for post_div in soup.select(POST_SELECTOR):
        # Username
        user_el = post_div.select_one(USERNAME_SELECTOR)
        username = user_el.get_text(strip=True) if user_el else post_div.get("data-author")

        # Profile URL
        profile_url = None
        if user_el:
            link_el = user_el.find("a", href=True)
            if link_el:
                profile_url = absolute_url(str(link_el["href"]))
        if not profile_url:
            # Fallback: any link to /members/ inside post card
            link_el = post_div.find("a", href=_is_member_link)
            if link_el and link_el.get("href"):
                profile_url = absolute_url(str(link_el["href"]))

        # Timestamp
        time_el = post_div.find("time", attrs={"datetime": True}) or post_div.find("time")
        timestamp = time_el.get("datetime") if time_el else None

        # Body text
        body_el = post_div.select_one(BODY_SELECTOR)
        text = body_el.get_text("\n", strip=True) if body_el else None
        quotes = _extract_quote_targets(post_div)
        mentions = _extract_mentions(body_el)
        reactions = _extract_reaction_bar(post_div)

        # Post ID
        post_id = _extract_post_id(post_div)

        posts.append({
            "post_id": post_id,
            "username": username,
            "profile_url": profile_url,
            "timestamp": timestamp,
            "text": text,
            "quotes": quotes,
            "mentions": mentions,
            "reactions": reactions,
        })     

    return posts

def _build_interactions_for_post(
    *,
    thread_id: str,
    post_row: dict,
    quotes: list[dict],
    mentions: list[dict],
    reactions: dict | None,
    post_author_index: dict[str, dict],
    starter_post_id: str | None,
    starter_user_id: str | None,
    prev_post_id: str | None,
    prev_user_id: str | None,
) -> list[dict]:
    interactions: list[dict] = []
    replying_post_id = post_row.get("post_id")
    if not replying_post_id:
        return interactions

    source_user_id = post_row.get("user_id")
    scraped_at = post_row.get("scraped_at")

    reaction_entries: list[dict] = []
    if reactions:
        overlay_url = reactions.get("overlay_url")
        if overlay_url:
            reaction_entries = _parse_reaction_overlay(overlay_url)
        if not reaction_entries:
            reaction_types = reactions.get("reaction_types") or []
            fallback = reaction_types[0] if reaction_types else {}
            fallback_reaction = _normalize_reaction_name(fallback.get("reaction"))
            for name in reactions.get("preview_names", []):
                reaction_entries.append({
                    "reaction_id": fallback.get("reaction_id"),
                    "reaction": fallback_reaction,
                    "username": name,
                    "profile_url": None,
                    "user_id": None,
                })

    for reaction in reaction_entries:
        if not reaction.get("user_id") and not reaction.get("username"):
            continue
        interaction_type = _normalize_reaction_name(reaction.get("reaction"))
        if not interaction_type:
            continue
        interaction_type = f"reaction-{interaction_type}"
        interactions.append({
            "interaction_id": str(uuid4()),
            "replying_post_id": replying_post_id,
            "target_post_id": replying_post_id,
            "source_user_id": reaction.get("user_id"),
            "target_user_id": source_user_id,
            "thread_id": thread_id,
            "interaction_type": interaction_type,
            "scraped_at": scraped_at,
        })

    for quote in quotes:
        target_post_id = quote.get("target_post_id")
        target_user_id = None
        if target_post_id and target_post_id in post_author_index:
            target_user_id = post_author_index[target_post_id].get("user_id")
        interactions.append({
            "interaction_id": str(uuid4()),
            "replying_post_id": replying_post_id,
            "target_post_id": target_post_id,
            "source_user_id": source_user_id,
            "target_user_id": target_user_id,
            "thread_id": thread_id,
            "interaction_type": "quote",
            "scraped_at": scraped_at,
        })

    for mention in mentions:
        profile_url = mention.get("profile_url")
        target_user_id = mention.get("user_id")
        if not target_user_id and profile_url:
            target_user_id = extract_user_id_from_profile_url(profile_url)
        if not target_user_id and not mention.get("username"):
            continue
        interactions.append({
            "interaction_id": str(uuid4()),
            "replying_post_id": replying_post_id,
            "target_post_id": None,
            "source_user_id": source_user_id,
            "target_user_id": target_user_id,
            "thread_id": thread_id,
            "interaction_type": "mention",
            "scraped_at": scraped_at,
        })

    # Default interaction: treat every post as a reply; aim at previous post when available, otherwise thread starter.
    target_post_id = prev_post_id or starter_post_id
    target_user_id = prev_user_id or starter_user_id
    interactions.append({
        "interaction_id": str(uuid4()),
        "replying_post_id": replying_post_id,
        "target_post_id": target_post_id,
        "source_user_id": source_user_id,
        "target_user_id": target_user_id,
        "thread_id": thread_id,
        "interaction_type": "reply",
        "scraped_at": scraped_at,
    })

    return interactions

def scrape_thread(
    thread_url: str,
    user_cache: dict[str, dict],
    max_pages: int | None,
    forum_url: str | None = None,
):
    """
    Scrape all posts in a thread, enrich with user metadata, and derive interactions.
    Returns (posts, interactions, thread_row).
    """
    all_posts: list[dict] = []
    interactions: list[dict] = []
    post_author_index: dict[str, dict] = {}
    thread_id = _thread_id_from_url(thread_url)
    thread_scrape_ts = _current_scrape_timestamp()

    starter_post_id: str | None = None
    starter_user_id: str | None = None
    prev_post_id: str | None = None
    prev_user_id: str | None = None

    page_url = thread_url
    page = 1

    while True:
        print(f"[scrape-thread] Fetching page {page_url}")
        html = fetch(page_url)
        soup = BeautifulSoup(html, "html.parser")
        _inject_nested_replies(soup, page_url)
        page_posts = parse_posts_from_page(soup)

        for p in page_posts:
            profile_url = p.get("profile_url")
            user_id = None
            username = p.get("username")
            quotes = p.get("quotes") or []
            mentions = p.get("mentions") or []

            if profile_url:
                user = get_or_fetch_user(profile_url, user_cache)
                if user:
                    user_id = user["user_id"]
                    # Prefer canonical username from profile if present
                    if user.get("username"):
                        username = user["username"]
                else:
                    # fallback: derive from URL
                    user_id = extract_user_id_from_profile_url(profile_url)

            scraped_at = _current_scrape_timestamp()
            post_id = p.get("post_id")
            post_row = {
                "thread_id": thread_id,
                "thread_url": thread_url,
                "page_url": page_url,
                "post_id": post_id,
                "user_id": user_id,
                "username": username,
                "timestamp": p.get("timestamp"),
                "text": p.get("text"),
                "scraped_at": scraped_at,
            }
            all_posts.append(post_row)
            if post_id:
                post_author_index[post_id] = {
                    "user_id": user_id,
                    "username": username,
                }
                if starter_post_id is None:
                    starter_post_id = post_id
                    starter_user_id = user_id

            interactions.extend(
                _build_interactions_for_post(
                    thread_id=thread_id,
                    post_row=post_row,
                    quotes=quotes,
                    mentions=mentions,
                    reactions=p.get("reactions"),
                    post_author_index=post_author_index,
                    starter_post_id=starter_post_id,
                    starter_user_id=starter_user_id,
                    prev_post_id=prev_post_id,
                    prev_user_id=prev_user_id,
                )
            )

            if post_id:
                prev_post_id = post_id
                prev_user_id = user_id

        if max_pages is not None and page >= max_pages:
            break

        next_link = soup.select_one(NEXT_PAGE_SELECTOR)
        if not next_link:
            break
        next_href = next_link.get("href")
        if not next_href:
            break

        page_url = absolute_url(str(next_href))
        page += 1

    thread_row = {
        "thread_id": thread_id,
        "thread_url": thread_url,
        "forum_url": forum_url,
        "first_seen": thread_scrape_ts,
        "last_seen": thread_scrape_ts,
        "scraped_at": thread_scrape_ts,
    }
    return all_posts, interactions, thread_row
