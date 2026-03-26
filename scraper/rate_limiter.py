import os
import random
import time
import requests

from collections import deque
from typing import Optional

from dotenv import load_dotenv
load_dotenv()

# Pool of current Chrome UAs — one is picked randomly per session.
_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
]

class RateLimiter:
    """
    Allow up to `max_calls` every `period` seconds.
    Example: max_calls=10, period=60 => 10 requests/minute.
    """
    def __init__(self, max_calls: int, period: float):
        self.max_calls = max_calls
        self.period = period
        self.calls = deque()

    def wait(self):
        now = time.time()
        while self.calls and self.calls[0] <= now - self.period:
            self.calls.popleft()

        if len(self.calls) >= self.max_calls:
            sleep_for = self.period - (now - self.calls[0])
            if sleep_for > 0:
                print(f"[rate-limiter] Sleeping {sleep_for:.2f}s to respect rate limit...")
                time.sleep(sleep_for)
        self.calls.append(time.time())

class BlockedResponseError(RuntimeError):
    """Raised when the server returns 200 but the body indicates a login wall, CAPTCHA, or permission block."""

_BLOCK_SIGNALS = [
    "you must be logged in",
    "you must be registered",
    "you do not have permission",
    "please log in",
    "cf-browser-verification",
    "just a moment...",           # Cloudflare challenge
    "checking your browser",     # Cloudflare challenge
]

def _check_for_blocked_response(text: str, url: str) -> None:
    """Raise BlockedResponseError if the response body looks like a block page."""
    lower = text[:4000].lower()  # only scan the head of the document
    for signal in _BLOCK_SIGNALS:
        if signal in lower:
            raise BlockedResponseError(
                f"[fetch] Response from {url} looks blocked (matched: {signal!r}). "
                "Cookie may be expired or the page requires authentication."
            )

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": random.choice(_USER_AGENTS),
})

# Load auth cookies from environment if available.
# XF_USER is the persistent login cookie (lasts 30 days).
# CDNCSRF is the CDN-level CSRF token.
# Domain must match www.personalitycafe.com (not .personalitycafe.com)
# to avoid duplicate cookie conflicts with server-set values.
_xf_user = os.environ.get("XF_USER")
_cdncsrf = os.environ.get("CDNCSRF")
if _xf_user:
    SESSION.cookies.set("xf_user", _xf_user, domain="www.personalitycafe.com")
if _cdncsrf:
    SESSION.cookies.set("cdncsrf", _cdncsrf, domain="www.personalitycafe.com")

DEFAULT_MAX_CALLS = 1
DEFAULT_PERIOD = 2.0
_limiter: Optional[RateLimiter] = None

def configure_rate_limiter(max_calls: int = DEFAULT_MAX_CALLS, period: float = DEFAULT_PERIOD) -> None:
    """Set global rate limiter configuration before issuing requests."""
    global _limiter
    _limiter = RateLimiter(max_calls=max_calls, period=period)

def _get_limiter() -> RateLimiter:
    global _limiter
    if _limiter is None:
        configure_rate_limiter()
    assert _limiter is not None
    return _limiter

def fetch(url: str, max_retries: int = 3, max_429_retries: int = 5, cookies=None) -> str:
    """
    Rate-limited GET with basic retry + 429/5xx backoff.
    Returns HTML text or raises the last exception.
    Can pass in cookie if necessary.

    429 responses are retried separately (up to `max_429_retries` times)
    without consuming the normal retry budget.
    """
    attempt = 0
    consecutive_429s = 0

    while attempt < max_retries:
        attempt += 1
        _get_limiter().wait()
        try:
            resp = SESSION.get(url, timeout=15, cookies=cookies)
        except requests.RequestException as e:
            print(f"[fetch] Error on {url}: {e} (attempt {attempt}/{max_retries})")
            if attempt == max_retries:
                raise
            time.sleep(5 * attempt)
            continue

        # Respect 429 (Too Many Requests) — does not consume a retry attempt
        if resp.status_code == 429:
            consecutive_429s += 1
            if consecutive_429s > max_429_retries:
                raise RuntimeError(
                    f"[fetch] Hit 429 on {url} {consecutive_429s} times in a row, giving up."
                )
            retry_after = resp.headers.get("Retry-After")
            if retry_after and retry_after.isdigit():
                delay = int(retry_after)
            else:
                delay = 60  # fallback
            print(f"[fetch] 429 on {url}, sleeping {delay}s then retrying (429 #{consecutive_429s})...")
            time.sleep(delay)
            attempt -= 1  # don't consume a retry
            continue

        consecutive_429s = 0

        # Handle 400 cookie/session errors — refresh session cookies and retry
        if resp.status_code == 400:
            try:
                body = resp.json()
                errors = body.get("errors", [])
            except Exception:
                errors = []
            errors_lower = " ".join(str(e).lower() for e in errors)
            if "cookies are required" in errors_lower or "security error" in errors_lower:
                if attempt < max_retries:
                    _get_limiter().wait()
                    try:
                        SESSION.get("https://www.personalitycafe.com/", timeout=15)
                    except Exception:
                        pass
                    print(f"[fetch] 400 session expired on {url}, refreshed — retrying...")
                    continue

        # Handle 5xx with backoff
        if 500 <= resp.status_code < 600:
            print(f"[fetch] {resp.status_code} on {url} (attempt {attempt}/{max_retries})")
            if attempt == max_retries:
                resp.raise_for_status()
            time.sleep(10 * attempt)
            continue

        resp.raise_for_status()
        _check_for_blocked_response(resp.text, url)
        return resp.text

    raise RuntimeError(f"Failed to fetch {url} after {max_retries} attempts.")
