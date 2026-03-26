"""
Absentee Owner Scraper
Identifies properties where the owner's MAILING address ≠ PROPERTY address.
These are landlords/investors who don't live at the property — often motivated sellers.

Run all:          python3 absentee_scraper.py
Run one county:   python3 absentee_scraper.py --county "Hamilton"
Run one state:    python3 absentee_scraper.py --state Arizona

Confirmed free sources:
  OH - Hamilton County:  hamiltoncountyauditor.org full parcel data
  OH - Cuyahoga County:  TaxMap_Parcels_CAMA_RP_WGS84 ArcGIS FeatureServer (owner + mailing)
  AZ - Maricopa County:  ArcGIS Residential Master bulk ZIP (free, no auth)
  TX - Dallas County:    DCAD bulk ZIP (~162MB, free)
  TN - Davidson County:  Nashville Metro Parcels ArcGIS FeatureServer
  GA - Fulton County:    PropertyProfile.zip bulk export (~26MB, free)
  IN - Marion County:    MapIndyProperty MapServer layer 10 (owner + mailing, 347k parcels)
  AL - Jefferson County: JCCGIS Parcels MapServer (out-of-state owners, free, no auth)
"""

import os
import io
import csv
import sys
import time
import random
import argparse
import requests
from datetime import datetime, timezone, timedelta
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

LISTING_DAYS = 60

# Entity filter — same as tax scraper
ENTITY_KEYWORDS = [
    " LLC", " L.L.C", " INC", " CORP", " LTD", " L.P.",
    " LP ", " LP,", "TRUST", "PROPERTIES", "HOLDINGS",
    "INVESTMENTS", "REALTY", "MANAGEMENT", "PARTNERS",
    "PARTNERSHIP", "ASSOCIATION", "FOUNDATION", "CHURCH",
    "COUNTY", "CITY OF", "STATE OF", "USA ", "U.S.A",
    "BANK ", "MORTGAGE", "FINANCIAL", "CAPITAL ",
    "REAL ESTATE", "ENTERPRISES", "VENTURES", "DEVELOPMENT",
    "CONSTRUCTION", "GROUP ", "SERVICES ", "SOLUTIONS",
]

def _is_entity(name: str) -> bool:
    upper = (name or "").upper()
    return any(kw in upper for kw in ENTITY_KEYWORDS)

def _now_iso():
    return datetime.now(timezone.utc).isoformat()

def _expires_iso():
    return (datetime.now(timezone.utc) + timedelta(days=LISTING_DAYS)).isoformat()

def _clean(v) -> str:
    return str(v).strip() if v is not None else ""


def _is_absentee(owner_addr: str, prop_addr: str, owner_city: str, prop_city: str) -> bool:
    """Returns True if owner's mailing address differs from property address."""
    if not owner_addr or not prop_addr:
        return False
    # Normalize
    oa = owner_addr.upper().strip()
    pa = prop_addr.upper().strip()
    oc = owner_city.upper().strip()
    pc = prop_city.upper().strip()
    # Different street address = likely absentee
    if oa and pa and oa[:15] != pa[:15]:
        return True
    # Same street but different city = definitely absentee
    if oc and pc and oc != pc:
        return True
    return False


def _get(url: str, verify: bool = True, timeout: int = 60) -> requests.Response | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout, verify=verify)
        return r if r.ok else None
    except Exception as e:
        print(f"  GET failed: {e}")
        return None


def _get_arcgis_fs(base_url: str, where: str = "1=1",
                   fields: str = "*", batch: int = 2000) -> list[dict] | None:
    """Paginate through an ArcGIS FeatureServer layer."""
    records = []
    offset = 0
    while True:
        params = {
            "where": where,
            "outFields": fields,
            "f": "json",
            "resultOffset": offset,
            "resultRecordCount": batch,
        }
        try:
            r = requests.get(f"{base_url}/query", params=params,
                             headers=HEADERS, timeout=120)
            if not r.ok:
                print(f"  ArcGIS FS error {r.status_code}")
                return None
            data = r.json()
        except Exception as e:
            print(f"  ArcGIS FS failed: {e}")
            return None
        features = data.get("features", [])
        for feat in features:
            records.append(feat.get("attributes", {}))
        if len(features) < batch:
            break
        offset += batch
    return records


# ─── HAMILTON COUNTY OH ──────────────────────────────────────────────────────

def scrape_hamilton_oh() -> list[dict]:
    """
    Hamilton County OH Auditor — full parcel data.
    Columns: parcel_number, owner_name_1, owner_address_1, owner_address_2,
             property_class, unpaid_amount, tax_district
    Owner address IS the mailing address. We compare to parcel location
    from a second file, or use city/zip mismatch as proxy.
    """
    print("  [Hamilton County, OH] Downloading full parcel data...", end="", flush=True)
    # The unpaid.xlsx has owner mailing address. For the full roll, we need a different file.
    # Hamilton County also publishes a full parcel export via their GIS portal.
    # Confirmed URL: current unpaid accounts (mailing address + parcel)
    url = "https://www.hamiltoncountyauditor.org/download/Delinquent/unpaid.xlsx"

    r = _get(url)
    if not r:
        print(" failed")
        return []

    import openpyxl
    wb = openpyxl.load_workbook(io.BytesIO(r.content), read_only=True, data_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    if not rows:
        print(" empty")
        return []

    headers = [str(h).strip().lower() if h else "" for h in rows[0]]
    leads = []

    for row_data in rows[1:]:
        row = {headers[i]: (row_data[i] if i < len(row_data) else None) for i in range(len(headers))}
        owner = _clean(row.get("owner_name_1", ""))
        if not owner or _is_entity(owner):
            continue

        # owner_address_1 = mailing address (street), owner_address_2 = city/state/zip
        owner_street = _clean(row.get("owner_address_1", ""))
        owner_citystate = _clean(row.get("owner_address_2", ""))
        parcel = _clean(row.get("parcel_number", ""))

        # Extract owner city from "CINCINNATI,OH  45230" pattern
        owner_city = owner_citystate.split(",")[0].strip() if "," in owner_citystate else ""

        # For Hamilton County, we don't have a separate property address column in this file
        # Use city mismatch: if owner doesn't live in OH or lives outside Cincinnati metro = absentee
        owner_state = ""
        if "," in owner_citystate:
            parts = owner_citystate.split(",")
            if len(parts) > 1:
                state_zip = parts[1].strip()
                owner_state = state_zip[:2] if len(state_zip) >= 2 else ""

        # Flag as absentee if owner's mailing address is out-of-state
        # OR if owner city doesn't match Cincinnati/Hamilton County area
        oh_cities = {"CINCINNATI", "NORWOOD", "FOREST PARK", "BLUE ASH", "MASON",
                     "ANDERSON", "DEER PARK", "EVENDALE", "FAIRFAX", "GOLF MANOR",
                     "LOCKLAND", "MADEIRA", "MONTGOMERY", "MT HEALTHY", "READING",
                     "SILVERTON", "SPRINGDALE", "ST BERNARD", "WOODLAWN", "WYOMING"}
        is_absentee = (owner_state and owner_state != "OH") or \
                      (owner_city and owner_city.upper() not in oh_cities)

        if not is_absentee:
            continue

        leads.append({
            "state":          "Ohio",
            "county":         "Hamilton",
            "owner_name":     owner,
            "owner_address":  f"{owner_street}, {owner_citystate}".strip(", "),
            "property_address": parcel,  # parcel # as proxy (no property addr in this file)
            "parcel_id":      parcel,
            "amount_owed":    _clean(row.get("unpaid_amount", "")),
            "source_url":     "https://www.hamiltoncountyauditor.org/",
            "scraped_at":     _now_iso(),
            "expires_at":     _expires_iso(),
        })

    print(f" {len(leads):,} absentee owners")
    return leads


# ─── HILLSBOROUGH COUNTY FL ──────────────────────────────────────────────────

def scrape_hillsborough_fl() -> list[dict]:
    """
    Hillsborough County FL — HCPA_Parcels_All ArcGIS FeatureServer (public, no auth).
    Fields: OWNER, ADDR_1/CITY/STATE/ZIP (mailing) vs SITE_ADDR/SITE_CITY/SITE_ZIP (property).
    ~379k absentee residential parcels.
    """
    print("  [Hillsborough County, FL] Downloading parcel data...", end="", flush=True)

    base_url = "https://services.arcgis.com/apTfC6SUmnNfnxuF/arcgis/rest/services/HCPA_Parcels_All/FeatureServer/0"
    where = "(STATE <> 'FL' OR ZIP <> SITE_ZIP) AND DOR_C LIKE '01%'"
    fields = "FOLIO,OWNER,ADDR_1,CITY,STATE,ZIP,SITE_ADDR,SITE_CITY,SITE_ZIP"

    records = _get_arcgis_fs(base_url, where=where, fields=fields)
    if records is None:
        print(" failed")
        return []

    leads = []
    skipped = 0
    for row in records:
        owner = _clean(row.get("OWNER", ""))
        if not owner or _is_entity(owner):
            skipped += 1
            continue
        site_addr = _clean(row.get("SITE_ADDR", ""))
        if not site_addr:
            continue
        mail_str   = _clean(row.get("ADDR_1", ""))
        mail_city  = _clean(row.get("CITY", ""))
        mail_state = _clean(row.get("STATE", ""))
        mail_zip   = _clean(row.get("ZIP", ""))
        leads.append({
            "state":            "Florida",
            "county":           "Hillsborough",
            "owner_name":       owner,
            "owner_address":    f"{mail_str}, {mail_city}, {mail_state} {mail_zip}".strip(", "),
            "property_address": site_addr,
            "parcel_id":        _clean(row.get("FOLIO", "")),
            "amount_owed":      "",
            "source_url":       "https://hcpafl.org/",
            "scraped_at":       _now_iso(),
            "expires_at":       _expires_iso(),
        })

    print(f" {len(leads):,} absentee owners (skipped {skipped:,} entities)")
    return leads


def _parse_csv_bytes(content: bytes) -> list[dict]:
    text = content.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    return list(reader)


def _fl_rows_to_absentee(rows: list[dict], county: str, source_url: str) -> list[dict]:
    """Filter FL property appraiser rows to absentee owners."""
    leads = []
    for row in rows:
        keys = {k.upper().strip() if k else "": v for k, v in row.items()}

        def get(*candidates):
            for c in candidates:
                v = keys.get(c.upper())
                if v is not None and str(v).strip():
                    return str(v).strip()
            return ""

        owner = get("OWN_NAME", "OWNER_NAME", "OWNER1", "NAME")
        if not owner or _is_entity(owner):
            continue

        mail_addr  = get("MAIL_ADDRESS1", "MAILING_ADDRESS", "MAIL_ADDR1", "OWN_ADDR1")
        mail_city  = get("MAIL_CITY", "MAILING_CITY", "OWN_CITY")
        mail_state = get("MAIL_STATE", "MAILING_STATE", "OWN_STATE")
        prop_addr  = get("SITUS_ADDRESS", "SITE_ADDRESS", "PHYSICAL_ADDRESS", "PROP_ADDRESS")
        prop_city  = get("SITUS_CITY", "SITE_CITY", "PROP_CITY")
        parcel     = get("PARCEL_ID", "PARCEL", "FOLIO", "ACCOUNT")

        if not _is_absentee(mail_addr, prop_addr, mail_city, prop_city):
            continue

        leads.append({
            "state":          "Florida",
            "county":         county,
            "owner_name":     owner,
            "owner_address":  f"{mail_addr}, {mail_city}, {mail_state}".strip(", "),
            "property_address": prop_addr or parcel,
            "parcel_id":      parcel,
            "amount_owed":    "",
            "source_url":     source_url,
            "scraped_at":     _now_iso(),
            "expires_at":     _expires_iso(),
        })

    return leads


# ─── MARICOPA COUNTY AZ ──────────────────────────────────────────────────────

def scrape_maricopa_az() -> list[dict]:
    """
    Maricopa County AZ Assessor — Residential Master bulk ZIP (free, no auth).
    Pipe-delimited TXT, no header row. Column layout confirmed from file spec:
      Col  0: APN
      Col 24: Owner Name
      Col 25: Owner Mailing Street
      Col 26: Owner Mailing Street 2
      Col 27: Owner Mailing City
      Col 28: Owner Mailing State
      Col 29: Owner Mailing Zip
      Col 31: Situs Street Number
      Col 32: Situs Street Direction
      Col 33: Situs Street Name
      Col 34: Situs Street Type
      Col 37: Situs City
      Col 38: Situs Zip
    """
    print("  [Maricopa County, AZ] Downloading Residential Master ZIP...", end="", flush=True)

    url = "https://www.arcgis.com/sharing/rest/content/items/e22983d41d91490d90965544b718a120/data"
    try:
        r = requests.get(url, headers=HEADERS, timeout=180, allow_redirects=True)
        if not r.ok:
            print(f" failed (HTTP {r.status_code})")
            return []
    except Exception as e:
        print(f" failed ({e})")
        return []

    print(f" {len(r.content)//1024//1024}MB downloaded, parsing...", end="", flush=True)

    import zipfile
    try:
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            txt_name = next((n for n in z.namelist() if n.endswith(".txt")), None)
            if not txt_name:
                print(" no TXT file in ZIP")
                return []

            raw = z.read(txt_name).decode("utf-8-sig", errors="replace")
            reader = csv.reader(io.StringIO(raw), delimiter="|")

            leads = []
            skipped_entity = 0
            for row in reader:
                if len(row) < 38:
                    continue

                parcel     = _clean(row[0])
                owner      = _clean(row[24])
                mail_str   = _clean(row[25])
                mail_city  = _clean(row[27])
                mail_state = _clean(row[28])
                mail_zip   = _clean(row[29])

                # Build situs address from parts
                situs_addr = " ".join(filter(None, [
                    _clean(row[31]), _clean(row[32]),
                    _clean(row[33]), _clean(row[34])
                ]))
                situs_city = _clean(row[37])
                situs_zip  = _clean(row[38]) if len(row) > 38 else ""

                if not parcel or not owner:
                    continue
                if _is_entity(owner):
                    skipped_entity += 1
                    continue

                # Absentee: out-of-state owner OR owner city/zip differs from situs
                is_absentee = False
                if mail_state and mail_state.upper() != "AZ":
                    is_absentee = True
                elif mail_zip and situs_zip and mail_zip[:5] != situs_zip[:5]:
                    is_absentee = True
                elif mail_city and situs_city and mail_city.upper() != situs_city.upper():
                    is_absentee = True
                elif mail_str and situs_addr and mail_str.upper()[:10] != situs_addr.upper()[:10]:
                    is_absentee = True

                if not is_absentee:
                    continue

                leads.append({
                    "state":            "Arizona",
                    "county":           "Maricopa",
                    "owner_name":       owner,
                    "owner_address":    f"{mail_str}, {mail_city}, {mail_state} {mail_zip}".strip(", "),
                    "property_address": situs_addr,
                    "parcel_id":        parcel,
                    "amount_owed":      "",
                    "source_url":       "https://mcassessor.maricopa.gov/",
                    "scraped_at":       _now_iso(),
                    "expires_at":       _expires_iso(),
                })

        print(f" {len(leads):,} absentee owners (skipped {skipped_entity:,} entities)")
        return leads

    except zipfile.BadZipFile:
        print(" not a valid ZIP file")
        return []
    except Exception as e:
        print(f" parse error: {e}")
        import traceback; traceback.print_exc()
        return []


# ─── DALLAS COUNTY TX ────────────────────────────────────────────────────────

def scrape_dallas_tx() -> list[dict]:
    """
    Dallas Central Appraisal District — free bulk ZIP download.
    ACCOUNT_INFO.CSV has owner mailing address + situs address.
    URL built dynamically via DCAD's download handler.
    """
    print("  [Dallas County, TX] Downloading DCAD bulk data (~162MB)...", end="", flush=True)

    url = (
        "https://www.dallascad.org/ViewPDFs.aspx?type=3&id="
        "\\\\DCAD.ORG\\WEB\\WEBDATA\\WEBFORMS\\DATA PRODUCTS\\DCAD2026_CURRENT.ZIP"
    )
    try:
        r = requests.get(url, headers=HEADERS, timeout=300)
        if not r.ok:
            print(f" failed (HTTP {r.status_code})")
            return []
    except Exception as e:
        print(f" failed ({e})")
        return []

    print(f" {len(r.content)//1024//1024}MB downloaded, parsing...", end="", flush=True)

    import zipfile
    try:
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            raw = z.read("ACCOUNT_INFO.CSV").decode("utf-8-sig", errors="replace")
    except Exception as e:
        print(f" ZIP error: {e}")
        return []

    reader = csv.DictReader(io.StringIO(raw))
    leads = []
    skipped_entity = 0
    skipped_same   = 0

    for row in reader:
        # Skip non-real-property accounts
        if _clean(row.get("DIVISION_CD", "")).upper() == "BPP":
            continue

        owner = _clean(row.get("OWNER_NAME1", ""))
        if not owner or _is_entity(owner):
            skipped_entity += 1
            continue

        parcel     = _clean(row.get("ACCOUNT_NUM", ""))
        mail_str   = " ".join(filter(None, [
            _clean(row.get("OWNER_ADDRESS_LINE1", "")),
            _clean(row.get("OWNER_ADDRESS_LINE2", "")),
            _clean(row.get("OWNER_ADDRESS_LINE3", "")),
        ]))
        mail_city  = _clean(row.get("OWNER_CITY", ""))
        mail_state = _clean(row.get("OWNER_STATE", ""))
        mail_zip   = _clean(row.get("OWNER_ZIPCODE", ""))

        situs_addr = " ".join(filter(None, [
            _clean(row.get("STREET_NUM", "")),
            _clean(row.get("FULL_STREET_NAME", "")),
        ]))
        situs_city = _clean(row.get("PROPERTY_CITY", ""))
        situs_zip  = _clean(row.get("PROPERTY_ZIPCODE", ""))

        if not parcel or not situs_addr:
            continue

        # Absentee check
        is_absentee = False
        if mail_state and mail_state.upper() not in ("TEXAS", "TX"):
            is_absentee = True
        elif mail_zip and situs_zip and mail_zip[:5] != situs_zip[:5].strip():
            is_absentee = True
        elif mail_city and situs_city and mail_city.upper() != situs_city.upper():
            is_absentee = True
        elif mail_str and situs_addr and mail_str.upper()[:10] != situs_addr.upper()[:10]:
            is_absentee = True

        if not is_absentee:
            skipped_same += 1
            continue

        leads.append({
            "state":            "Texas",
            "county":           "Dallas",
            "owner_name":       owner,
            "owner_address":    f"{mail_str}, {mail_city}, {mail_state} {mail_zip}".strip(", "),
            "property_address": situs_addr,
            "parcel_id":        parcel,
            "amount_owed":      "",
            "source_url":       "https://www.dallascad.org/",
            "scraped_at":       _now_iso(),
            "expires_at":       _expires_iso(),
        })

    print(f" {len(leads):,} absentee owners (skipped {skipped_entity:,} entities, {skipped_same:,} owner-occupied)")
    return leads


# ─── FULTON COUNTY GA ────────────────────────────────────────────────────────

_FULTON_RESIDENTIAL = {
    "Residential 1 family", "Residential 2 family", "Residential 3 family",
    "Residential 4 family", "Residential under construction",
    "Single Family Residential Cond", "Single Family Residential Loft",
    "Single Family Residential Town", "Co Ops Single Family Fee Simpl",
    "Single Family Residential Mobi", "Single Family Residential: Par",
}

def scrape_fulton_ga() -> list[dict]:
    """
    Fulton County GA — PropertyProfile.zip bulk export (~26MB, free no auth).
    Tab-delimited TXT with Owner, MailAddr (mailing), Situs (property), ZipCode.
    ~128k absentee residential owners.
    """
    print("  [Fulton County, GA] Downloading PropertyProfile ZIP...", end="", flush=True)

    url = "https://gis.fultoncountyga.gov/Data/OpenData/Data/PropertyProfile/TXT/PropertyProfile.zip"
    try:
        r = requests.get(url, headers=HEADERS, timeout=180)
        if not r.ok:
            print(f" failed (HTTP {r.status_code})")
            return []
    except Exception as e:
        print(f" failed ({e})")
        return []

    import zipfile
    try:
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            raw = z.read("PropertyProfile.txt").decode("utf-8-sig", errors="replace")
    except Exception as e:
        print(f" ZIP error: {e}")
        return []

    reader = csv.DictReader(io.StringIO(raw), delimiter="\t")
    leads = []
    skipped = 0

    for row in reader:
        lu = _clean(row.get("LandUse", ""))
        if lu not in _FULTON_RESIDENTIAL:
            continue

        owner = _clean(row.get("Owner", ""))
        if not owner or _is_entity(owner):
            skipped += 1
            continue

        situs    = _clean(row.get("Situs", "")).upper()
        mail     = _clean(row.get("MailAddr", "")).upper()
        parcel   = _clean(row.get("ParcelID", ""))
        zip_code = _clean(row.get("ZipCode", ""))

        if not situs or not mail:
            continue

        # Absentee: mailing address doesn't match situs address start
        if mail.startswith(situs[:10]):
            continue

        leads.append({
            "state":            "Georgia",
            "county":           "Fulton",
            "owner_name":       owner,
            "owner_address":    mail,
            "property_address": situs,
            "parcel_id":        parcel,
            "amount_owed":      "",
            "source_url":       "https://www.fultoncountyga.gov/",
            "scraped_at":       _now_iso(),
            "expires_at":       _expires_iso(),
        })

    print(f" {len(leads):,} absentee owners (skipped {skipped:,} entities)")
    return leads


# ─── DAVIDSON COUNTY TN ──────────────────────────────────────────────────────

_DAVIDSON_RESIDENTIAL = (
    "SINGLE FAMILY", "DUPLEX", "TRIPLEX", "QUADPLEX",
    "RESIDENTIAL CONDO", "RESIDENTIAL COMBO/MISC",
    "ZERO LOT LINE", "MOBILE HOME",
)

def scrape_davidson_tn() -> list[dict]:
    """
    Davidson County (Nashville) TN — Nashville Metro Property Assessor parcels.
    ArcGIS FeatureServer with owner mailing address + property address.
    Absentee filter: mailing zip != property zip OR mailing state != TN.
    """
    print("  [Davidson County, TN] Downloading parcel data...", end="", flush=True)

    base_url = "https://services2.arcgis.com/HdTo6HJqh92wn4D8/arcgis/rest/services/Parcels_view/FeatureServer/0"
    # Pull absentee owners (out-of-state OR different zip) server-side to reduce payload
    where = "OwnState <> 'TN' OR OwnZip <> PropZip"
    fields = "ParID,Owner,OwnAddr1,OwnCity,OwnState,OwnZip,PropAddr,PropCity,PropZip,LUDesc,OwnDate"

    records = _get_arcgis_fs(base_url, where=where, fields=fields)
    if records is None:
        print(" failed")
        return []

    def _epoch_to_date(v):
        if not v:
            return ""
        try:
            return datetime.fromtimestamp(int(v) / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
        except Exception:
            return ""

    leads = []
    skipped = 0
    residential_upper = {s.upper() for s in _DAVIDSON_RESIDENTIAL}

    for row in records:
        lu = _clean(row.get("LUDesc", "")).upper()
        if lu not in residential_upper:
            skipped += 1
            continue

        owner = _clean(row.get("Owner", ""))
        if not owner or _is_entity(owner):
            skipped += 1
            continue

        prop_addr = _clean(row.get("PropAddr", ""))
        if not prop_addr:
            continue

        mail_str   = _clean(row.get("OwnAddr1", ""))
        mail_city  = _clean(row.get("OwnCity", ""))
        mail_state = _clean(row.get("OwnState", ""))
        mail_zip   = _clean(row.get("OwnZip", ""))

        leads.append({
            "state":            "Tennessee",
            "county":           "Davidson",
            "owner_name":       owner,
            "owner_address":    f"{mail_str}, {mail_city}, {mail_state} {mail_zip}".strip(", "),
            "property_address": prop_addr,
            "parcel_id":        str(_clean(row.get("ParID", ""))),
            "amount_owed":      "",
            "source_url":       "https://www.padctn.org/",
            "scraped_at":       _now_iso(),
            "expires_at":       _expires_iso(),
        })

    print(f" {len(leads):,} absentee owners (skipped {skipped:,} commercial/entities)")
    return leads


# ─── CUYAHOGA COUNTY OH ──────────────────────────────────────────────────────

def scrape_cuyahoga_oh() -> list[dict]:
    """
    Cuyahoga County OH — TaxMap_Parcels_CAMA_RP_WGS84 ArcGIS FeatureServer.
    Server-side filter: out-of-state owner OR owner zip ≠ parcel zip.
    Residential filter: Ohio land use codes starting with '5' (5xx = residential).
    Fields: parcelpin, mail_name, mail_addr_street/city/state/zip, par_addr_all, tax_luc
    """
    print("  [Cuyahoga County, OH] Downloading parcel data...", end="", flush=True)

    base_url = (
        "https://gis.cuyahogacounty.us/server/rest/services/"
        "CUYAHOGA_BASE/TaxMap_Parcels_CAMA_RP_WGS84/FeatureServer/0"
    )
    where = (
        "mail_name IS NOT NULL AND mail_state IS NOT NULL AND "
        "(mail_state <> 'OH' OR "
        "(mail_zip IS NOT NULL AND par_zip IS NOT NULL AND mail_zip <> par_zip))"
    )
    fields = (
        "parcelpin,mail_name,mail_addr_street,mail_city,mail_state,mail_zip,"
        "par_addr_all,par_city,par_zip,tax_luc"
    )

    records = _get_arcgis_fs(base_url, where=where, fields=fields)
    if records is None:
        print(" failed")
        return []

    leads = []
    skipped = 0
    for row in records:
        # Residential: Ohio LUC codes starting with '5' (510=SFR, 520=duplex, 530=triplex, etc.)
        luc = _clean(row.get("tax_luc", ""))
        if not luc.startswith("5"):
            skipped += 1
            continue

        owner = _clean(row.get("mail_name", ""))
        if not owner or _is_entity(owner):
            skipped += 1
            continue

        prop_addr = _clean(row.get("par_addr_all", ""))
        if not prop_addr:
            continue

        mail_str   = _clean(row.get("mail_addr_street", ""))
        mail_city  = _clean(row.get("mail_city", ""))
        mail_state = _clean(row.get("mail_state", ""))
        mail_zip   = _clean(row.get("mail_zip", ""))

        leads.append({
            "state":            "Ohio",
            "county":           "Cuyahoga",
            "owner_name":       owner,
            "owner_address":    f"{mail_str}, {mail_city}, {mail_state} {mail_zip}".strip(", "),
            "property_address": prop_addr,
            "parcel_id":        _clean(row.get("parcelpin", "")),
            "amount_owed":      "",
            "source_url":       "https://cuyahogacounty.gov/",
            "scraped_at":       _now_iso(),
            "expires_at":       _expires_iso(),
        })

    print(f" {len(leads):,} absentee owners (skipped {skipped:,} commercial/entities)")
    return leads


# ─── MARION COUNTY IN ────────────────────────────────────────────────────────

_MARION_RESIDENTIAL = (
    "RES ONE FAMILY", "RES TWO FAMILY", "CONDO PLATTED", "OTHER RES STRUCTURE",
    "SINGLE FAMILY", "CONDOMINIUM", "DUPLEX", "TWO-FAMILY", "TRIPLEX",
    "THREE-FAMILY", "TOWNHOUSE", "MOBILE HOME", "RESIDENTIAL", "SFR",
    "ROW HOUSE", "ZERO LOT",
)

def scrape_marion_in() -> list[dict]:
    """
    Marion County IN — MapIndyProperty MapServer layer 10 (347k parcels, free, no auth).
    Server-side filter: out-of-state owner OR owner zip ≠ property zip.
    Fields: FULLOWNERNAME, OWNERADDRESS/CITY/STATE/ZIP (mailing),
            STNUMBER/PRE_DIR/STREET_NAME/SUFFIX/CITY/ZIPCODE (property),
            PROPERTY_SUB_CLASS_DESCRIPTION
    """
    print("  [Marion County, IN] Downloading parcel data...", end="", flush=True)

    base_url = (
        "https://gis.indy.gov/server/rest/services/MapIndy/MapIndyProperty/MapServer/10"
    )
    where = (
        "FULLOWNERNAME IS NOT NULL AND "
        "(OWNERSTATE <> 'IN' OR "
        "(OWNERZIP IS NOT NULL AND ZIPCODE IS NOT NULL AND OWNERZIP <> ZIPCODE))"
    )
    fields = (
        "PARCEL_TAG,FULLOWNERNAME,OWNERADDRESS,OWNERCITY,OWNERSTATE,OWNERZIP,"
        "STNUMBER,PRE_DIR,STREET_NAME,SUFFIX,CITY,ZIPCODE,PROPERTY_SUB_CLASS_DESCRIPTION"
    )

    records = _get_arcgis_fs(base_url, where=where, fields=fields)
    if records is None:
        print(" failed")
        return []

    res_upper = {s.upper() for s in _MARION_RESIDENTIAL}
    leads = []
    skipped = 0

    for row in records:
        sub_desc = _clean(row.get("PROPERTY_SUB_CLASS_DESCRIPTION", "")).upper()
        if not any(r in sub_desc for r in res_upper):
            skipped += 1
            continue

        owner = _clean(row.get("FULLOWNERNAME", ""))
        if not owner or _is_entity(owner):
            skipped += 1
            continue

        situs_addr = " ".join(filter(None, [
            _clean(row.get("STNUMBER", "")),
            _clean(row.get("PRE_DIR", "")),
            _clean(row.get("STREET_NAME", "")),
            _clean(row.get("SUFFIX", "")),
        ]))
        situs_city = _clean(row.get("CITY", ""))
        situs_zip  = _clean(row.get("ZIPCODE", ""))
        prop_addr  = f"{situs_addr}, {situs_city}, IN {situs_zip}".strip(", ") if situs_addr else ""
        if not prop_addr:
            continue

        mail_str   = _clean(row.get("OWNERADDRESS", ""))
        mail_city  = _clean(row.get("OWNERCITY", ""))
        mail_state = _clean(row.get("OWNERSTATE", ""))
        mail_zip   = _clean(row.get("OWNERZIP", ""))

        leads.append({
            "state":            "Indiana",
            "county":           "Marion",
            "owner_name":       owner,
            "owner_address":    f"{mail_str}, {mail_city}, {mail_state} {mail_zip}".strip(", "),
            "property_address": prop_addr,
            "parcel_id":        _clean(row.get("PARCEL_TAG", "")),
            "amount_owed":      "",
            "source_url":       "https://www.indy.gov/",
            "scraped_at":       _now_iso(),
            "expires_at":       _expires_iso(),
        })

    print(f" {len(leads):,} absentee owners (skipped {skipped:,} commercial/entities)")
    return leads


# ─── JEFFERSON COUNTY AL ─────────────────────────────────────────────────────

def scrape_jefferson_al() -> list[dict]:
    """
    Jefferson County AL (Birmingham) — JCCGIS Parcels MapServer (public, no auth).
    Server-side filter: out-of-state owner only (STATE_Mail <> 'AL').
    Fields: PID, OWNERNAME, PROP_MAIL (mailing street), CITYMAIL, STATE_Mail, ZIP_MAIL,
            Bldg_Number, Street_Name, Street_Type, Street_Dir, Property_City, ZIP.
    URL: https://jccgis.jccal.org/server/rest/services/Basemap/Parcels/MapServer/0
    """
    print("  [Jefferson County, AL] Downloading parcel data...", end="", flush=True)

    base_url = (
        "https://jccgis.jccal.org/server/rest/services/Basemap/Parcels/MapServer/0"
    )
    # Only pull records where owner mails from outside Alabama
    where = "OWNERNAME IS NOT NULL AND STATE_Mail IS NOT NULL AND STATE_Mail <> 'AL'"
    fields = (
        "PID,OWNERNAME,PROP_MAIL,CITYMAIL,STATE_Mail,ZIP_MAIL,"
        "Bldg_Number,Street_Name,Street_Type,Street_Dir,Property_City,ZIP"
    )

    records = _get_arcgis_fs(base_url, where=where, fields=fields)
    if records is None:
        print(" failed")
        return []

    leads = []
    skipped = 0

    for row in records:
        owner = _clean(row.get("OWNERNAME", ""))
        if not owner or _is_entity(owner):
            skipped += 1
            continue

        # Build property address from street components
        situs_addr = " ".join(filter(None, [
            _clean(row.get("Bldg_Number", "")),
            _clean(row.get("Street_Dir", "")),
            _clean(row.get("Street_Name", "")),
            _clean(row.get("Street_Type", "")),
        ]))
        situs_city = _clean(row.get("Property_City", ""))
        situs_zip  = _clean(row.get("ZIP", ""))
        prop_addr  = f"{situs_addr}, {situs_city}, AL {situs_zip}".strip(", ") if situs_addr else ""
        if not prop_addr:
            continue

        mail_str   = _clean(row.get("PROP_MAIL", ""))
        mail_city  = _clean(row.get("CITYMAIL", ""))
        mail_state = _clean(row.get("STATE_Mail", ""))
        mail_zip   = _clean(row.get("ZIP_MAIL", ""))

        leads.append({
            "state":            "Alabama",
            "county":           "Jefferson",
            "owner_name":       owner,
            "owner_address":    f"{mail_str}, {mail_city}, {mail_state} {mail_zip}".strip(", "),
            "property_address": prop_addr,
            "parcel_id":        _clean(row.get("PID", "")),
            "amount_owed":      "",
            "source_url":       "https://jccgis.jccal.org/",
            "scraped_at":       _now_iso(),
            "expires_at":       _expires_iso(),
        })

    print(f" {len(leads):,} absentee owners (skipped {skipped:,} entities)")
    return leads


# ─── COUNTY CONFIGS ───────────────────────────────────────────────────────────

COUNTY_SCRAPERS = {
    "Hamilton":     ("Ohio",       scrape_hamilton_oh),
    "Cuyahoga":     ("Ohio",       scrape_cuyahoga_oh),
    "Hillsborough": ("Florida",    scrape_hillsborough_fl),
    "Maricopa":     ("Arizona",    scrape_maricopa_az),
    "Dallas":       ("Texas",      scrape_dallas_tx),
    "Davidson":     ("Tennessee",  scrape_davidson_tn),
    "Fulton":       ("Georgia",    scrape_fulton_ga),
    "Marion":       ("Indiana",    scrape_marion_in),
    "Jefferson":    ("Alabama",    scrape_jefferson_al),
}


# ─── DB SAVE ──────────────────────────────────────────────────────────────────

def _save_leads(db, leads: list[dict], county: str) -> int:
    if not leads:
        print(f"  → [{county}] No leads to save")
        return 0

    seen = {}
    for lead in leads:
        key = (lead["state"], lead["county"], lead.get("parcel_id") or lead.get("property_address", ""))
        seen[key] = lead
    unique = list(seen.values())

    CHUNK = 500
    total = 0
    for i in range(0, len(unique), CHUNK):
        chunk = unique[i:i + CHUNK]
        try:
            resp = db.table("absentee_owner_leads").upsert(
                chunk,
                on_conflict="state,county,parcel_id"
            ).execute()
            total += len(resp.data) if resp.data else len(chunk)
        except Exception as e:
            print(f"  → [{county}] DB error chunk {i//CHUNK+1}: {e}")

    print(f"  → [{county}] {total:,} absentee owners saved")
    return total


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def run(state: str | None = None, county: str | None = None):
    from supabase import create_client

    url_env = os.environ.get("SUPABASE_URL", "")
    key_env = os.environ.get("SUPABASE_SERVICE_KEY", "")
    if not url_env or not key_env:
        print("ERROR: Set SUPABASE_URL and SUPABASE_SERVICE_KEY.")
        return

    db = create_client(url_env, key_env)

    targets = {
        name: (st, fn) for name, (st, fn) in COUNTY_SCRAPERS.items()
        if (not state  or st.lower()   == state.lower()) and
           (not county or name.lower() == county.lower())
    }

    if not targets:
        print(f"No confirmed sources for state='{state}' county='{county}'")
        return

    print(f"\n{'='*55}")
    print(f"  Absentee Owner Scraper — {len(targets)} county/counties")
    print(f"{'='*55}\n")

    total = 0
    for name, (st, fn) in targets.items():
        print(f"\n[{st} — {name} County]")
        leads = fn()
        total += _save_leads(db, leads, name)
        time.sleep(random.uniform(2, 4))

    print(f"\n{'='*55}")
    print(f"  Done. Total saved: {total:,}")
    print(f"{'='*55}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--state",  type=str, default=None)
    parser.add_argument("--county", type=str, default=None)
    args = parser.parse_args()
    run(state=args.state, county=args.county)
