"""
FSBO Scraper — Craigslist + fsbo.com
Run manually or on a schedule: python3 scraper.py
Run for one state:              python3 scraper.py --state Arizona
"""
import re
import sys
import time
import random
import argparse
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

PHONE_RE  = re.compile(r'(\(?\d{3}\)?[\s\-\.]\d{3}[\s\-\.]\d{4})')
PRICE_RE  = re.compile(r'\$[\d]{2,3},\d{3}')   # matches $85,000 – $999,999

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

MARKETS = {
    "Arizona":      [("Phoenix", "phoenix"), ("Tucson", "tucson")],
    "Texas":        [("Dallas", "dallas"), ("Houston", "houston"), ("Austin", "austin")],
    "Tennessee":    [("Nashville", "nashville"), ("Memphis", "memphis")],
    "Florida":      [("Tampa", "tampa"), ("Orlando", "orlando"), ("Jacksonville", "jacksonville")],
    "Georgia":      [("Atlanta", "atlanta")],
    "Nevada":       [("Las Vegas", "lasvegas")],
    "California":   [("Los Angeles", "losangeles"), ("San Diego", "sandiego")],
    "Ohio":         [("Columbus", "columbus"), ("Cleveland", "cleveland")],
    "Indiana":      [("Indianapolis", "indianapolis")],
    "Alabama":      [("Birmingham", "birmingham")],
    "Missouri":     [("St. Louis", "stlouis"), ("Kansas City", "kansascity")],
}

STATE_ABBR = {
    "Arizona": "AZ", "Texas": "TX", "Tennessee": "TN", "Florida": "FL",
    "Georgia": "GA", "Nevada": "NV", "California": "CA",
    "Ohio": "OH", "Indiana": "IN", "Alabama": "AL", "Missouri": "MO",
}

MAX_PER_CITY = 40
LISTING_DAYS = 30


def _get(url: str, session=None, timeout: int = 12) -> requests.Response | None:
    try:
        s = session or requests
        r = s.get(url, headers=HEADERS, timeout=timeout)
        return r if r.ok else None
    except Exception:
        return None


# ── Craigslist ─────────────────────────────────────────────────────────────────

def _scrape_cl_detail(url: str) -> tuple[str, str, str]:
    """Returns (phone, address, description) from a Craigslist listing."""
    r = _get(url)
    if not r:
        return "", "", ""
    soup = BeautifulSoup(r.text, "html.parser")
    body_el = soup.select_one("#postingbody")
    description = body_el.get_text(" ", strip=True) if body_el else ""
    m = PHONE_RE.search(description)
    phone = m.group(1).strip() if m else ""
    addr_el = soup.select_one(".mapaddress")
    address = addr_el.get_text(strip=True) if addr_el else ""
    return phone, address, description[:600]


def scrape_city(city: str, subdomain: str, state: str) -> list[dict]:
    url = f"https://{subdomain}.craigslist.org/search/rea?purveyor=owner&sort=date"
    print(f"    [CL] {city}... ", end="", flush=True)

    r = _get(url)
    if not r:
        print("blocked/failed")
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    items = (soup.select("li.cl-static-search-result") or
             soup.select("li.cl-search-result") or
             soup.select("li[data-pid]") or
             soup.select(".result-row"))

    if not items:
        print("no listings found")
        return []

    results = []
    expires  = (datetime.now(timezone.utc) + timedelta(days=LISTING_DAYS)).isoformat()
    now_iso  = datetime.now(timezone.utc).isoformat()

    for item in items[:MAX_PER_CITY]:
        try:
            link_el = item.select_one("a")
            if not link_el:
                continue
            href = link_el.get("href", "")
            if not href.startswith("http"):
                href = f"https://{subdomain}.craigslist.org{href}"
            title_el = item.select_one(".title")
            title = title_el.get_text(strip=True) if title_el else item.get("title", "")
            if not href or not title:
                continue
            price_el = item.select_one(".price")
            price = price_el.get_text(strip=True) if price_el else ""
            hood_el = item.select_one(".location")
            neighborhood = hood_el.get_text(strip=True) if hood_el else city

            time.sleep(random.uniform(1.0, 2.0))
            phone, address, description = _scrape_cl_detail(href)

            results.append({
                "state":       state,
                "city":        city,
                "title":       title[:200],
                "price":       price,
                "address":     address or neighborhood,
                "phone":       phone,
                "description": description,
                "url":         href,
                "source":      "craigslist",
                "posted_at":   now_iso,
                "scraped_at":  now_iso,
                "expires_at":  expires,
            })
        except Exception:
            continue

    phones_found = sum(1 for r in results if r["phone"])
    print(f"{len(results)} listings ({phones_found} with phone)")
    return results


# ── fsbo.com ───────────────────────────────────────────────────────────────────

def _fsbo_com_session() -> tuple[requests.Session, str]:
    """Returns a session with cookies + the form_key CSRF token."""
    session = requests.Session()
    session.headers.update(HEADERS)
    r = session.get("https://fsbo.com/", timeout=12)
    soup = BeautifulSoup(r.text, "html.parser")
    el = soup.select_one("input[name='form_key']")
    form_key = el["value"] if el else ""
    return session, form_key


def _fsbo_com_listing(session: requests.Session, url: str) -> dict | None:
    """Fetch one fsbo.com listing page. Returns dict or None if no phone."""
    r = _get(url, session=session)
    if not r:
        return None
    soup = BeautifulSoup(r.text, "html.parser")

    # Phone: find <strong>Phone:</strong> then its sibling div
    phone = ""
    seller_name = ""
    for strong in soup.find_all("strong"):
        text = strong.get_text(strip=True)
        if text == "Phone:":
            sib = strong.parent.find_next_sibling("div")
            if sib:
                candidate = sib.get_text(strip=True)
                if PHONE_RE.match(candidate) or re.match(r'\d{3}', candidate):
                    phone = candidate
        elif text == "Contact:":
            sib = strong.parent.find_next_sibling("div")
            if sib:
                seller_name = sib.get_text(strip=True)

    if not phone:
        return None   # skip listings without a phone

    # Address + beds/baths from og:title
    og_title = ""
    og = soup.find("meta", property="og:title")
    if og:
        og_title = og.get("content", "")

    # Price — look for first $XXX,XXX in page body
    price = ""
    body_text = soup.get_text(" ")
    m = PRICE_RE.search(body_text)
    if m:
        price = m.group(0)

    return {
        "title":       og_title[:200],
        "price":       price,
        "address":     og_title[:200],
        "phone":       phone,
        "description": seller_name,   # reuse description field for seller name
        "url":         url,
    }


def scrape_fsbo_com(state: str) -> list[dict]:
    abbr = STATE_ABBR.get(state, "")
    cities = MARKETS.get(state, [])
    print(f"    [fsbo.com] {state}... ", end="", flush=True)

    session, form_key = _fsbo_com_session()
    if not form_key:
        print("could not get form key")
        return []

    seen_urls = set()
    results   = []
    expires   = (datetime.now(timezone.utc) + timedelta(days=LISTING_DAYS)).isoformat()
    now_iso   = datetime.now(timezone.utc).isoformat()

    for city, _ in cities:
        query = f"{city}, {abbr}"
        try:
            resp = session.post(
                "https://fsbo.com/listings/search/search/",
                data={"form_key": form_key, "searchQuery": query, "radius": "50"},
                timeout=12,
                allow_redirects=True,
            )
        except Exception:
            continue

        soup = BeautifulSoup(resp.text, "html.parser")
        links = list(dict.fromkeys(  # dedupe while preserving order
            a["href"] for a in soup.select("a[href*='/listings/show/']")
            if a.get("href")
        ))

        for link_url in links[:20]:
            if link_url in seen_urls:
                continue
            seen_urls.add(link_url)
            time.sleep(random.uniform(1.5, 2.5))
            listing = _fsbo_com_listing(session, link_url)
            if not listing:
                continue
            listing.update({
                "state":      state,
                "city":       city,
                "source":     "fsbo.com",
                "posted_at":  now_iso,
                "scraped_at": now_iso,
                "expires_at": expires,
            })
            results.append(listing)

        time.sleep(random.uniform(2.0, 3.0))

    print(f"{len(results)} listings with phone")
    return results


# ── State runner ───────────────────────────────────────────────────────────────

def scrape_state(state: str) -> list[dict]:
    cities = MARKETS.get(state, [])
    all_leads = []

    # Craigslist (volume)
    for city, subdomain in cities:
        leads = scrape_city(city, subdomain, state)
        all_leads.extend(leads)
        time.sleep(random.uniform(2.0, 4.0))

    # fsbo.com (guaranteed phones)
    fsbo_leads = scrape_fsbo_com(state)
    all_leads.extend(fsbo_leads)

    return all_leads


# ── Main ───────────────────────────────────────────────────────────────────────

def run(states: list[str] | None = None):
    import os
    from supabase import create_client

    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_SERVICE_KEY", "")
    if not url or not key:
        print("ERROR: Set SUPABASE_URL and SUPABASE_SERVICE_KEY environment variables.")
        return

    db = create_client(url, key)
    targets = states or list(MARKETS.keys())

    print(f"\n{'='*55}")
    print(f"  FSBO Scraper — {len(targets)} state(s)")
    print(f"{'='*55}\n")

    total_new = 0
    for state in targets:
        print(f"\n[{state}]")
        leads = scrape_state(state)
        if leads:
            try:
                resp = db.table("fsbo_leads").upsert(leads, on_conflict="url").execute()
                n = len(resp.data) if resp.data else 0
                total_new += n
                print(f"  → {n} leads saved to database")
            except Exception as e:
                print(f"  → DB error: {e}")
        else:
            print(f"  → No leads found")

    print(f"\n{'='*55}")
    print(f"  Done. Total saved: {total_new}")
    print(f"{'='*55}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", type=str, default=None, help="Scrape one state only")
    args = parser.parse_args()
    run([args.state] if args.state else None)
