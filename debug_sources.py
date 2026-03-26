import requests
from bs4 import BeautifulSoup
import re

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

session = requests.Session()
session.headers.update(HEADERS)

url = "https://fsbo.com/listings/listings/show/id/546219/"
r = session.get(url, timeout=12)
soup = BeautifulSoup(r.text, "html.parser")

# Find the Phone: label and print its parent element
for strong in soup.find_all("strong"):
    if strong.get_text(strip=True) == "Phone:":
        print("=== Phone label found ===")
        parent = strong.parent
        print(f"Parent tag: {parent.name}")
        print(f"Parent HTML:\n{parent.prettify()[:500]}")
        # Also print grandparent
        print(f"\nGrandparent HTML:\n{parent.parent.prettify()[:800]}")

# Find actual listing price (not dropdown)
print("\n=== Looking for listing price ===")
for strong in soup.find_all("strong"):
    if strong.get_text(strip=True) == "Contact:":
        print(f"Contact label parent:\n{strong.parent.prettify()[:400]}")

# Print a section of the page body
print("\n=== Body section (5000-8000 chars) ===")
print(r.text[5000:8000])
