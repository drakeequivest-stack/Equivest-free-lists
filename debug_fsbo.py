import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

urls = [
    "https://www.forsalebyowner.com/real-estate/arizona/phoenix",
    "https://www.forsalebyowner.com/real-estate/arizona",
    "https://www.forsalebyowner.com/search?state=AZ&city=Phoenix",
]

for url in urls:
    r = requests.get(url, headers=HEADERS, timeout=12)
    soup = BeautifulSoup(r.text, "html.parser")
    title = soup.title.string if soup.title else "None"
    print(f"{r.status_code}  {url}")
    print(f"  Title: {title[:80]}")
    # Check for listing elements
    for sel in [".listing", ".property-card", "[class*='listing']", "[class*='property']"]:
        found = soup.select(sel)
        if found:
            print(f"  Selector '{sel}': {len(found)} results")
    print()
