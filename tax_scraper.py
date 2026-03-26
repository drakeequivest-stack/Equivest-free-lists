"""
Tax Delinquent Scraper — multi-state (free/public sources only)
Run all states:      python3 tax_scraper.py
Run one state:       python3 tax_scraper.py --state Texas
Run one county:      python3 tax_scraper.py --county "Travis"

Sources (free, no subscription):
  FL - Orange County:      CSV direct download (octaxcol.com)
  OH - Hamilton County:    Excel direct download (hamiltoncountyauditor.org)
  TN - Shelby County:      Excel direct download (shelbycountytrustee.com)
  NV - Clark County:       Annual Delinquency PDF (~12,600 records)
  TX - Travis County:      CSV direct download (traviscountytx.gov, ~residential/multifamily)
"""

import os
import sys
import io
import csv
import time
import random
import argparse
import requests
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

# How long until leads expire (refresh monthly)
LISTING_DAYS = 60

# Skip corporate/entity owners — wholesalers want individual motivated sellers only
ENTITY_KEYWORDS = [
    " LLC", " L.L.C", " INC", " CORP", " LTD", " L.P.",
    " LP ", " LP,", " LP", "TRUST", "PROPERTIES", "HOLDINGS",
    "LIMITED LIABILITY", " PARTNE", " SERIES",  # handles truncated PDF names
    "INVESTMENTS", "REALTY", "MANAGEMENT", "PARTNERS",
    "PARTNERSHIP", "ASSOCIATION", "FOUNDATION", "CHURCH",
    "COUNTY", "CITY OF", "STATE OF", "USA ", "U.S.A",
    "BANK ", "MORTGAGE", "FINANCIAL", "CAPITAL ",
    "REAL ESTATE", "ENTERPRISES", "VENTURES", "DEVELOPMENT",
    "CONSTRUCTION", "GROUP ", "SERVICES ", "SOLUTIONS",
]

def _is_entity(owner_name: str) -> bool:
    """Returns True if the owner looks like a corporate entity, not an individual."""
    if not owner_name:
        return False
    upper = owner_name.upper()
    return any(kw in upper for kw in ENTITY_KEYWORDS)


def _get(url: str, timeout: int = 30, stream: bool = False) -> requests.Response | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout, stream=stream)
        return r if r.ok else None
    except Exception as e:
        print(f"  GET failed: {e}")
        return None


def _get_with_session(session_url: str, file_url: str, timeout: int = 60) -> requests.Response | None:
    """Establish a session cookie at session_url, then download file_url."""
    try:
        s = requests.Session()
        s.headers.update(HEADERS)
        s.get(session_url, timeout=30)   # seed the session cookie
        r = s.get(file_url, timeout=timeout)
        return r if r.ok else None
    except Exception as e:
        print(f"  GET (session) failed: {e}")
        return None


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _expires_iso():
    return (datetime.now(timezone.utc) + timedelta(days=LISTING_DAYS)).isoformat()


def _clean(val) -> str:
    if val is None:
        return ""
    return str(val).strip()


# ─── FLORIDA COUNTY SCRAPERS ──────────────────────────────────────────────────

def scrape_orange_fl() -> list[dict]:
    """Orange County FL — direct CSV download from octaxcol.com"""
    print("  [Orange County, FL] Downloading CSV...", end="", flush=True)
    url = "https://www.octaxcol.com/assets/uploads/2020/02/DelinquentRealEstateTaxData.zip"
    r = _get(url, timeout=60)
    if not r:
        # Try alternate URL format
        url2 = "https://www.octaxcol.com/tax-certificate-sale/"
        print(f" primary URL failed, trying page scrape...")
        return _scrape_orange_fl_page()

    import zipfile
    results = []
    try:
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            for fname in z.namelist():
                if fname.lower().endswith(".csv"):
                    with z.open(fname) as f:
                        reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig", errors="replace"))
                        for row in reader:
                            # Map common column names
                            lead = _orange_row_to_lead(row)
                            if lead:
                                results.append(lead)
                    break  # only first CSV
    except Exception as e:
        print(f" zip error: {e}")
        return []

    print(f" {len(results)} records")
    return results


def _orange_row_to_lead(row: dict) -> dict | None:
    """Map Orange County CSV columns to our schema.
    Actual columns from DelinquentRealEstateTaxData.csv:
    Parcel No, Owner1-5, Situs Street Number/Direction/Name/Type/Suite/City/ZipCode,
    Tax Year, Payoff Amount Due, Total Value, Taxable Value
    """
    parcel = _clean(row.get("Parcel No", ""))
    if not parcel:
        return None

    # Combine owner name fields
    owner_parts = [_clean(row.get(f"Owner{i}", "")) for i in range(1, 4)]
    owner = " / ".join(p for p in owner_parts if p)

    # Build situs (property) address
    situs_parts = [
        _clean(row.get("Situs Street Number", "")),
        _clean(row.get("Situs Street Direction", "")),
        _clean(row.get("Situs Street Name", "")),
        _clean(row.get("Situs Street Type", "")),
        _clean(row.get("Situs Suite", "")),
        _clean(row.get("Situs City", "")),
        _clean(row.get("Situs ZipCode", "")),
    ]
    address = " ".join(p for p in situs_parts if p)

    assessed = _clean(row.get("Total Value", row.get("Taxable Value", "")))
    owed     = _clean(row.get("Payoff Amount Due", row.get("Gross Taxes", "")))
    tax_year = _clean(row.get("Tax Year", row.get("Cert Year", "")))

    # Only keep Unpaid or Sellable certificates (active delinquent)
    status = _clean(row.get("Status Code", ""))
    if status and status not in ("Unpaid", "Sellable"):
        return None

    if not owner and not address:
        return None

    # Skip corporate entities
    if _is_entity(owner):
        return None

    return {
        "state":            "Florida",
        "county":           "Orange",
        "parcel_id":        parcel,
        "owner_name":       owner,
        "property_address": address,
        "assessed_value":   assessed,
        "amount_owed":      owed,
        "tax_year":         tax_year,
        "source_url":       "https://www.octaxcol.com/",
        "scraped_at":       _now_iso(),
        "expires_at":       _expires_iso(),
    }


def _scrape_orange_fl_page() -> list[dict]:
    """Fallback: scrape the Orange County tax sale page for a download link."""
    from bs4 import BeautifulSoup
    r = _get("https://www.octaxcol.com/tax-certificate-sale/")
    if not r:
        print("  [Orange] page scrape failed")
        return []
    soup = BeautifulSoup(r.text, "html.parser")
    # Look for any CSV/zip download links
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if any(ext in href.lower() for ext in [".csv", ".zip", ".xlsx"]):
            print(f"  Found download link: {href}")
            return scrape_csv_url("Orange", "Florida", href, "https://www.octaxcol.com/")
    print("  No download link found on page")
    return []


def scrape_csv_url(county: str, state: str, url: str, source_url: str,
                   col_map: dict | None = None,
                   session_url: str | None = None) -> list[dict]:
    """Generic: download a CSV (or zip containing CSV) and parse it."""
    print(f"  [{county} County, {state}] Fetching {url.split('/')[-1]}...", end="", flush=True)
    r = _get_with_session(session_url, url) if session_url else _get(url, timeout=60)
    if not r:
        print(" failed")
        return []

    content_type = r.headers.get("Content-Type", "")
    content = r.content

    # Handle zip files
    if url.lower().endswith(".zip") or "zip" in content_type:
        import zipfile
        try:
            with zipfile.ZipFile(io.BytesIO(content)) as z:
                for fname in z.namelist():
                    if fname.lower().endswith(".csv"):
                        content = z.read(fname)
                        break
                    elif fname.lower().endswith(".xlsx"):
                        return _parse_xlsx(county, state, source_url, z.read(fname), col_map)
        except Exception as e:
            print(f" zip error: {e}")
            return []

    # Handle Excel files
    if url.lower().endswith(".xlsx") or url.lower().endswith(".xls") or "spreadsheet" in content_type:
        return _parse_xlsx(county, state, source_url, content, col_map)

    # Parse CSV
    results = []
    try:
        text = content.decode("utf-8-sig", errors="replace")
        reader = csv.DictReader(io.StringIO(text))
        for row in reader:
            lead = _generic_row_to_lead(row, county, state, source_url, col_map)
            if lead:
                results.append(lead)
    except Exception as e:
        print(f" CSV parse error: {e}")
        return []

    print(f" {len(results)} records")
    return results


def _parse_xlsx(county: str, state: str, source_url: str,
                content: bytes, col_map: dict | None) -> list[dict]:
    """Parse an Excel file into leads."""
    try:
        import openpyxl
        wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True, data_only=True)
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))
        if not rows:
            return []
        headers = [str(h).strip() if h else "" for h in rows[0]]
        results = []
        for row_data in rows[1:]:
            row = {headers[i]: (row_data[i] if i < len(row_data) else "") for i in range(len(headers))}
            lead = _generic_row_to_lead(row, county, state, source_url, col_map)
            if lead:
                results.append(lead)
        print(f" {len(results)} records")
        return results
    except ImportError:
        print(" openpyxl not installed — run: pip3 install openpyxl --break-system-packages")
        return []
    except Exception as e:
        print(f" xlsx error: {e}")
        return []


def _generic_row_to_lead(row: dict, county: str, state: str,
                          source_url: str, col_map: dict | None = None) -> dict | None:
    """Map CSV row to lead using col_map or auto-detect common column names."""
    keys = {k.upper().strip() if k else "": v for k, v in row.items()}

    def get(*candidates):
        for c in candidates:
            v = keys.get(c.upper())
            if v is not None and str(v).strip():
                return str(v).strip()
        return ""

    if col_map:
        parcel  = get(col_map.get("parcel_id", ""))
        owner   = get(col_map.get("owner_name", ""))
        address = get(col_map.get("property_address", ""))
        assessed = get(col_map.get("assessed_value", ""))
        owed    = get(col_map.get("amount_owed", ""))
        year    = get(col_map.get("tax_year", ""))
    else:
        parcel  = get("PARCEL_ID", "PARCEL", "FOLIO", "ACCOUNT_NO", "ACCOUNT", "PARCELID", "PARCEL NO")
        owner   = get("OWNER_NAME", "OWNER", "TAXPAYER", "TAXPAYER_NAME", "NAME")
        address = get("SITUS_ADDRESS", "PROPERTY_ADDRESS", "ADDRESS", "SITE_ADDRESS",
                      "SITUS", "PROP_ADDR", "PROPERTY ADDRESS")
        assessed = get("ASSESSED_VALUE", "JUST_VALUE", "ASSESSED", "MARKET_VALUE", "APPRAISED_VALUE")
        owed    = get("TOTAL_DELINQUENT", "DELINQUENT_AMOUNT", "AMOUNT_DUE", "TAXES_DUE",
                      "TOTAL_DUE", "BALANCE", "AMOUNT OWED", "TAX_DUE")
        year    = get("TAX_YEAR", "YEAR", "FISCAL_YEAR")

    if not parcel and not owner:
        return None

    # Skip corporate entities
    if _is_entity(owner):
        return None

    return {
        "state":            state,
        "county":           county,
        "parcel_id":        parcel,
        "owner_name":       owner,
        "property_address": address,
        "assessed_value":   assessed,
        "amount_owed":      owed,
        "tax_year":         year,
        "source_url":       source_url,
        "scraped_at":       _now_iso(),
        "expires_at":       _expires_iso(),
    }


# ─── TEXAS ────────────────────────────────────────────────────────────────────

def scrape_travis_tx() -> list[dict]:
    """
    Travis County TX (Austin area) — Open Tax Delinquent CSV (free, direct download).
    URL: https://tax-office.traviscountytx.gov/voterdata/TaxDelqOpenData.csv
    ~30 columns including Owner Name, mailing address, property street/zip,
    Total Due, 1st Year Delinquent, Property Type Code (A=SFR, B=multifamily).
    """
    print("  [Travis County, TX] Downloading tax delinquent CSV...", end="", flush=True)
    url = "https://tax-office.traviscountytx.gov/voterdata/TaxDelqOpenData.csv"
    r = _get(url, timeout=60)
    if not r:
        print(" failed")
        return []

    try:
        text = r.content.decode("utf-8-sig", errors="replace")
        reader = csv.DictReader(io.StringIO(text))
    except Exception as e:
        print(f" parse error: {e}")
        return []

    results = []
    skipped = 0
    for row in reader:
        # Property Type Code: A = residential SFR, B = multifamily
        prop_type = _clean(row.get("Property Type Code", "")).upper()
        if prop_type and not prop_type.startswith(("A", "B")):
            skipped += 1
            continue

        owner = _clean(row.get("Owner Name", ""))
        if not owner or _is_entity(owner):
            skipped += 1
            continue

        # Build property address from Street Number + Street Name + Property Zip
        street_num  = _clean(row.get("Street Number", ""))
        street_name = _clean(row.get("Street Name", ""))
        prop_zip    = _clean(row.get("Property Zip", ""))
        prop_addr   = " ".join(filter(None, [street_num, street_name]))
        if prop_zip:
            prop_addr = f"{prop_addr}, TX {prop_zip}"

        amount = _clean(row.get("Total Due", "") or row.get("Delinquent Total", ""))
        year   = _clean(row.get("1st Year Delinquent", "") or row.get("Last Tax Roll Year", ""))

        results.append({
            "state":            "Texas",
            "county":           "Travis",
            "parcel_id":        _clean(row.get("Account #", "")),
            "owner_name":       owner,
            "property_address": prop_addr,
            "assessed_value":   _clean(row.get("Assessed Value", "")),
            "amount_owed":      amount,
            "tax_year":         year,
            "source_url":       "https://tax-office.traviscountytx.gov/",
            "scraped_at":       _now_iso(),
            "expires_at":       _expires_iso(),
        })

    print(f" {len(results):,} records (skipped {skipped:,} commercial/entities)")
    return results


# ─── NEVADA ───────────────────────────────────────────────────────────────────

def scrape_clark_nv() -> list[dict]:
    """Clark County NV (Las Vegas) — Annual Delinquency Publication PDF.
    ~12,600 records. Columns: Parcel | Name of Owner | Taxes
    URL updated each year; the treasurer page always points to latest.
    """
    import re
    try:
        import pdfplumber
    except ImportError:
        print("  [Clark County, NV] pdfplumber not installed — run: pip3 install pdfplumber --break-system-packages")
        return []

    src_url = "https://www.clarkcountynv.gov/government/elected_officials/county_treasurer/notice-of-delinquent-taxes-nrs-361-565"
    dl_url  = "https://www.clarkcountynv.gov/adobe/assets/urn:aaid:aem:9779802d-60e6-46f7-a3ee-577665a9b13e/original/as/annual-delinquency-publication.pdf"

    print("  [Clark County, NV] Downloading delinquency PDF...", end="", flush=True)
    r = _get(dl_url, timeout=120)
    if not r:
        print(" failed")
        return []

    PARCEL_PAT = re.compile(r'^\d{3}-\d{2}-\d{3}-\d{3}$')

    results = []
    try:
        with pdfplumber.open(io.BytesIO(r.content)) as pdf:
            for page in pdf.pages:
                words = page.extract_words()
                if not words:
                    continue

                # Group words into rows by top (round to nearest 2px)
                row_map: dict[int, list] = {}
                for w in words:
                    y_key = round(w['top'] / 2) * 2
                    row_map.setdefault(y_key, []).append(w)

                for y_key in sorted(row_map.keys()):
                    row_words = sorted(row_map[y_key], key=lambda w: w['x0'])

                    # Column layout (from bounding-box analysis):
                    #   Parcel:  x0 <  130  (matches NNN-NN-NNN-NNN)
                    #   Name:    130 <= x0 < 430
                    #   Amount:  x0 >= 430  (join tokens without space)
                    parcel_words = [w for w in row_words
                                    if w['x0'] < 130 and PARCEL_PAT.match(w['text'])]
                    if not parcel_words:
                        continue  # header, footer, or subtotal row

                    parcel = parcel_words[0]['text']
                    name   = ' '.join(w['text'] for w in row_words if 130 <= w['x0'] < 430)
                    amount = ''.join(w['text'] for w in row_words if w['x0'] >= 430)

                    if not name:
                        continue
                    # Clark County PDF writes abbreviations with spaces — normalize for entity check
                    name_norm = (name
                                 .replace(" L L C", " LLC")
                                 .replace(" L L P", " LLP")
                                 .replace(" L.L.C.", " LLC")
                                 .replace(" L T D", " LTD")
                                 .replace(" L P ", " LP ")
                                 .replace(" L P", " LP"))
                    if _is_entity(name_norm):
                        continue

                    results.append({
                        "state":            "Nevada",
                        "county":           "Clark",
                        "parcel_id":        parcel,
                        "owner_name":       name,
                        "property_address": "",
                        "assessed_value":   "",
                        "amount_owed":      amount,
                        "tax_year":         "",
                        "source_url":       src_url,
                        "scraped_at":       _now_iso(),
                        "expires_at":       _expires_iso(),
                    })
    except Exception as e:
        print(f" PDF parse error: {e}")
        return []

    print(f" {len(results):,} records")
    return results


# ─── CONFIRMED COUNTY SOURCES (verified working) ──────────────────────────────
# Format: (state, county, url, source_url, col_map)
# Only includes URLs that have been verified to return actual data.

CONFIRMED_COUNTIES = [
    # ── OHIO ──────────────────────────────────────────────────────────────────
    # Hamilton County (Cincinnati) — Current Unpaid Accounts Excel
    # 16,141 records | Columns: parcel_number, owner_name_1, owner_address_1, unpaid_amount
    ("Ohio", "Hamilton",
     "https://www.hamiltoncountyauditor.org/download/Delinquent/unpaid.xlsx",
     "https://www.hamiltoncountyauditor.org/tax_delinquent.asp",
     {
         "parcel_id":        "parcel_number",
         "owner_name":       "owner_name_1",
         "property_address": "owner_address_1",   # mailing addr doubles as property addr
         "amount_owed":      "unpaid_amount",
         "tax_year":         "",
     }),

    # ── TENNESSEE ─────────────────────────────────────────────────────────────
    # Shelby County (Memphis) — 2023 Delinquent Realty Lawsuit List Excel
    # 21,836 records | Columns: Owner Name, Parcel NO, Year, Property Location, Amount Sued
    ("Tennessee", "Shelby",
     "https://www.shelbycountytrustee.com/DocumentCenter/View/1492/ExhibitA",
     "https://www.shelbycountytrustee.com/259/Delinquent-Realty-Lawsuit-List",
     {
         "parcel_id":        "Parcel NO",
         "owner_name":       "Owner Name",
         "property_address": "Property Location",
         "amount_owed":      "Amount Sued",
         "tax_year":         "Year",
     }),
]


def run_confirmed(state: str | None, counties_filter: list[str] | None, db) -> int:
    """Run all confirmed county scrapers, optionally filtered by state/county."""
    total = 0
    for st, county, dl_url, src_url, col_map in CONFIRMED_COUNTIES:
        if state and st.lower() != state.lower():
            continue
        if counties_filter and county.lower() not in [c.lower() for c in counties_filter]:
            continue
        # Shelby County TN requires a session cookie before the file download
        if county == "Shelby" and st == "Tennessee":
            leads = scrape_csv_url(county, st, dl_url, src_url, col_map,
                                   session_url=src_url)
        else:
            leads = scrape_csv_url(county, st, dl_url, src_url, col_map)
        total += _save_leads(db, leads, county)
        time.sleep(random.uniform(2, 4))
    return total


# ─── MAIN RUNNER ──────────────────────────────────────────────────────────────

def run_florida(counties: list[str] | None, db) -> int:
    """Scrape Florida counties. Only Orange County has a confirmed working source."""
    total = 0

    if not counties or "orange" in [c.lower() for c in counties]:
        leads = scrape_orange_fl()
        total += _save_leads(db, leads, "Orange")
        time.sleep(random.uniform(2, 4))

    return total


def _save_leads(db, leads: list[dict], county: str) -> int:
    if not leads:
        print(f"  → [{county}] No leads to save")
        return 0

    # Deduplicate by (state, county, parcel_id) — keep last occurrence
    seen = {}
    for lead in leads:
        key = (lead["state"], lead["county"], lead.get("parcel_id", ""))
        seen[key] = lead
    unique = list(seen.values())
    if len(unique) < len(leads):
        print(f"  (deduped {len(leads) - len(unique)} duplicate parcels)")

    # Upsert in chunks to avoid request size limits
    CHUNK = 500
    total = 0
    for i in range(0, len(unique), CHUNK):
        chunk = unique[i:i + CHUNK]
        try:
            resp = db.table("tax_delinquent_leads").upsert(
                chunk,
                on_conflict="state,county,parcel_id"
            ).execute()
            total += len(resp.data) if resp.data else len(chunk)
        except Exception as e:
            print(f"  → [{county}] DB error on chunk {i//CHUNK + 1}: {e}")

    print(f"  → [{county}] {total} leads saved")
    return total


def run(state: str | None = None, counties: list[str] | None = None):
    import os
    from supabase import create_client

    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_SERVICE_KEY", "")
    if not url or not key:
        print("ERROR: Set SUPABASE_URL and SUPABASE_SERVICE_KEY environment variables.")
        return

    db = create_client(url, key)

    # Determine which states to run
    if state:
        states = [state]
    else:
        # All states with confirmed sources
        states = ["Florida", "Ohio", "Tennessee", "Nevada", "Texas"]

    print(f"\n{'='*55}")
    print(f"  Tax Delinquent Scraper — {', '.join(states)}")
    print(f"{'='*55}\n")

    total = 0
    for st in states:
        print(f"\n[{st}]")
        if st.lower() == "florida":
            total += run_florida(counties, db)
        elif st.lower() == "texas":
            if not counties or "travis" in [c.lower() for c in counties]:
                leads = scrape_travis_tx()
                total += _save_leads(db, leads, "Travis")
                time.sleep(random.uniform(2, 4))
        elif st.lower() == "nevada":
            if not counties or "clark" in [c.lower() for c in counties]:
                leads = scrape_clark_nv()
                total += _save_leads(db, leads, "Clark")
                time.sleep(random.uniform(2, 4))
        else:
            total += run_confirmed(st, counties, db)

    print(f"\n{'='*55}")
    print(f"  Done. Total saved: {total}")
    print(f"{'='*55}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--state",  type=str, default=None, help="State to scrape (default: all)")
    parser.add_argument("--county", type=str, default=None, help="Specific county only")
    args = parser.parse_args()

    counties = [args.county] if args.county else None
    run(state=args.state, counties=counties)
