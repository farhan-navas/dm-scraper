import os
import requests

URL = "https://www.personalitycafe.com/posts/44739864/reactions"

session = requests.Session()
session.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "X-Requested-With": "XMLHttpRequest",
    "Referer": "https://www.personalitycafe.com/",
})

cdncsrf = os.environ.get("CDNCSRF") or ""

session.cookies.update({
    "cdncsrf": cdncsrf,
})

resp = session.get(URL, timeout=10)

print("Status:", resp.status_code)
print("Content-Type:", resp.headers.get("Content-Type"))
print("Length:", len(resp.text))

with open("reactions.html", "w", encoding="utf-8") as f:
    f.write(resp.text)
