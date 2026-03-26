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

url = "https://phoenix.craigslist.org/search/rea?purveyor=owner&sort=date"
r = requests.get(url, headers=HEADERS, timeout=12)
print("Status:", r.status_code)

soup = BeautifulSoup(r.text, "html.parser")

# Try each selector
print("\nli.cl-search-result:", len(soup.select("li.cl-search-result")))
print("li[data-pid]:",        len(soup.select("li[data-pid]")))
print(".result-row:",         len(soup.select(".result-row")))
print("li.cl-static-search-result:", len(soup.select("li.cl-static-search-result")))

# Print first 3000 chars of body to see structure
# Print first item HTML to see structure
items = soup.select("li.cl-static-search-result")
if items:
    print("\n--- First item HTML ---")
    print(items[0].prettify()[:2000])
