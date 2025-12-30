import time
import requests

from collections import deque
from typing import Optional

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

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Farhan-ResearchBot/0.1 (+https://github.com/farhan-navas; contact: farhanmnavas@gmail.com)",
})

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

def fetch(url: str, max_retries: int = 3, cookies=None) -> str:
    """
    Rate-limited GET with basic retry + 429/5xx backoff.
    Returns HTML text or raises the last exception.
    Can pass in cookie if necessary
    """
    for attempt in range(1, max_retries + 1):
        _get_limiter().wait()
        try:
            resp = SESSION.get(url, timeout=15, cookies=cookies)
        except requests.RequestException as e:
            print(f"[fetch] Error on {url}: {e} (attempt {attempt}/{max_retries})")
            if attempt == max_retries:
                raise
            time.sleep(5 * attempt)
            continue

        # Respect 429 (Too Many Requests)
        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After")
            if retry_after and retry_after.isdigit():
                delay = int(retry_after)
            else:
                delay = 60  # fallback
            print(f"[fetch] 429 on {url}, sleeping {delay}s then retrying...")
            time.sleep(delay)
            continue

        # Handle 5xx with backoff
        if 500 <= resp.status_code < 600:
            print(f"[fetch] {resp.status_code} on {url} (attempt {attempt}/{max_retries})")
            if attempt == max_retries:
                resp.raise_for_status()
            time.sleep(10 * attempt)
            continue

        resp.raise_for_status()
        return resp.text

    raise RuntimeError(f"Failed to fetch {url} after {max_retries} attempts.")
