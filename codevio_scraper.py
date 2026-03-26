"""
Code Violations Scraper
Run all cities:   python3 codevio_scraper.py
Run one city:     python3 codevio_scraper.py --city "Columbus"
Run one state:    python3 codevio_scraper.py --state Ohio

Confirmed live sources:
  OH - Cleveland:     services3.arcgis.com   (ArcGIS FeatureServer, open violations)
  OH - Columbus:      opendata.columbus.gov  (Socrata CSV, ~280k records)
  IN - Indianapolis:  gis.indy.gov           (ArcGIS MapServer, 910k total, active filter)
  TX - Austin:        data.austintexas.gov   (Socrata CSV, ~2.8k active)
  TX - Houston:       services.arcgis.com    (ArcGIS FeatureServer, ~6.5k active)
  TN - Nashville:     services2.arcgis.com   (ArcGIS FeatureServer, ~2.8k active)
  NV - Las Vegas:     services1.arcgis.com   (ArcGIS FeatureServer, ~9.4k open)
  MO - Kansas City:   data.kcmo.org          (Socrata CSV, open violations)
  CA - Los Angeles:   data.lacity.org        (Socrata CSV, ~29k open)
  CA - San Francisco: data.sfgov.org         (Socrata CSV, ~29k active)

Note: No owner name in city data — address + parcel # used to identify properties.
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

def _now_iso():
    return datetime.now(timezone.utc).isoformat()

def _expires_iso():
    return (datetime.now(timezone.utc) + timedelta(days=LISTING_DAYS)).isoformat()

def _clean(v) -> str:
    return str(v).strip() if v is not None else ""

def _get_csv(url: str, verify_ssl: bool = True) -> list[dict] | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=60, verify=verify_ssl)
        if not r.ok:
            return None
        reader = csv.DictReader(io.StringIO(r.content.decode("utf-8-sig", errors="replace")))
        return list(reader)
    except Exception as e:
        print(f"  GET failed: {e}")
        return None


def _get_arcgis_fs(base_url: str, where: str = "1=1", batch: int = 1000) -> list[dict] | None:
    """Paginate through an ArcGIS FeatureServer layer and return all features as dicts."""
    records = []
    offset = 0
    while True:
        params = {
            "where":         where,
            "outFields":     "*",
            "f":             "json",
            "resultOffset":  offset,
            "resultRecordCount": batch,
        }
        try:
            r = requests.get(
                f"{base_url}/query",
                params=params,
                headers=HEADERS,
                timeout=60,
            )
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

        # Stop when we get fewer records than requested
        if len(features) < batch:
            break
        offset += batch

    return records


# ─── CITY SCRAPER CONFIGS ──────────────────────────────────────────────────────
# Format: (state, city, url, col_map, filter_fn, verify_ssl)
# col_map: maps our fields to dataset column names
# filter_fn: optional function(row) -> bool to keep only relevant rows

def _columbus_filter(row: dict) -> bool:
    """Keep only open/active residential violations."""
    status = _clean(row.get("B1_APPL_STATUS", "")).upper()
    vtype  = _clean(row.get("B1_PER_GROUP", "")).upper()
    # Keep active enforcement cases, skip closed/resolved
    if status in ("CLOSED", "FINALED", "EXPIRED", "CANCELLED"):
        return False
    return True

def _columbus_row(row: dict, state: str, city: str) -> dict | None:
    address = _clean(row.get("SITE_ADDRESS", ""))
    if not address:
        return None
    return {
        "state":          state,
        "city":           city,
        "address":        address,
        "parcel_id":      _clean(row.get("B1_PARCEL_NBR", "")),
        "violation_type": _clean(row.get("B1_PER_TYPE", "")),
        "violation_sub":  _clean(row.get("B1_PER_SUB_TYPE", "")),
        "case_status":    _clean(row.get("B1_APPL_STATUS", "")),
        "filed_date":     _clean(row.get("B1_FILE_DD", "")),
        "last_insp_date": _clean(row.get("INSP_LAST_DATE", "")),
        "source_url":     _clean(row.get("ACA_URL", "")),
        "scraped_at":     _now_iso(),
        "expires_at":     _expires_iso(),
    }


_HILLSBOROUGH_ACTIVE = {
    'Active','Fine Run','In Violation (DOV)','Inspection','New Owner Fine',
    'Not Complied Fines Running','Notice Issued','Open','RECEIVED','WARNING',
    'Hearing Scheduled','In Process','Pending','Pre Hearing','Referred to Hearing',
    'Print Agenda/Email Board','Settlement Offered','Settlement in Process',
}

def _hillsborough_row(row: dict, state: str, city: str) -> dict | None:
    address = _clean(row.get("USER_Property_Address", ""))
    if not address:
        return None
    return {
        "state":          state,
        "city":           city,
        "address":        address,
        "parcel_id":      _clean(row.get("USER_PARCEL_NO_NO", "")).strip(),
        "violation_type": _clean(row.get("USER_B1_APPL_STATUS", "")),
        "violation_sub":  "",
        "case_status":    _clean(row.get("USER_B1_APPL_STATUS", "")),
        "filed_date":     "",
        "last_insp_date": "",
        "source_url":     "",
        "scraped_at":     _now_iso(),
        "expires_at":     _expires_iso(),
    }

def _hillsborough_filter(row: dict) -> bool:
    return _clean(row.get("USER_B1_APPL_STATUS", "")) in _HILLSBOROUGH_ACTIVE


def _houston_row(row: dict, state: str, city: str) -> dict | None:
    address = _clean(row.get("IncidentAddress", ""))
    if not address:
        return None
    def _epoch_to_date(v):
        if v is None:
            return ""
        try:
            return datetime.fromtimestamp(int(v) / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
        except Exception:
            return ""
    return {
        "state":          state,
        "city":           city,
        "address":        address,
        "parcel_id":      _clean(row.get("CaseNumber", "")),
        "violation_type": _clean(row.get("ViolationType", "")),
        "violation_sub":  _clean(row.get("CaseDescription", ""))[:200],
        "case_status":    _clean(row.get("ViolationStatus", "")),
        "filed_date":     _epoch_to_date(row.get("InspectionDate")),
        "last_insp_date": _epoch_to_date(row.get("InspectionDate")),
        "source_url":     "",
        "scraped_at":     _now_iso(),
        "expires_at":     _expires_iso(),
    }


def _nashville_row(row: dict, state: str, city: str) -> dict | None:
    address = _clean(row.get("Property_Address", ""))
    if not address:
        return None
    # Append ZIP if present
    zip_code = _clean(row.get("ZIP", ""))
    if zip_code and zip_code not in address:
        address = f"{address} {zip_code}"
    # Convert epoch ms dates to ISO date strings
    def _epoch_to_date(v):
        if v is None:
            return ""
        try:
            return datetime.fromtimestamp(int(v) / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
        except Exception:
            return ""
    return {
        "state":          state,
        "city":           city,
        "address":        address,
        "parcel_id":      _clean(row.get("Property_APN", "")),
        "violation_type": _clean(row.get("Subtype_Description", "")),
        "violation_sub":  _clean(row.get("Violations_Noted", "")),
        "case_status":    _clean(row.get("Status", "")),
        "filed_date":     _epoch_to_date(row.get("Date_Received")),
        "last_insp_date": _epoch_to_date(row.get("Last_Activity_Date")),
        "source_url":     "",
        "scraped_at":     _now_iso(),
        "expires_at":     _expires_iso(),
    }


def _lasvegas_row(row: dict, state: str, city: str) -> dict | None:
    address = _clean(row.get("ADDRESS", ""))
    if not address:
        return None
    # Skip records where ADDRESS is a parcel number (all digits), not a street address
    if address.isdigit():
        return None
    def _epoch_to_date(v):
        if v is None:
            return ""
        try:
            return datetime.fromtimestamp(int(v) / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
        except Exception:
            return ""
    parcel = row.get("Parcel_Number")
    try:
        parcel_str = str(int(float(parcel))) if parcel is not None else ""
    except Exception:
        parcel_str = _clean(parcel)
    return {
        "state":          state,
        "city":           city,
        "address":        address,
        "parcel_id":      parcel_str,
        "violation_type": _clean(row.get("DESCRIPT", "")),
        "violation_sub":  _clean(row.get("WARD", "")),
        "case_status":    _clean(row.get("STAT", "")),
        "filed_date":     _epoch_to_date(row.get("Event_Date")),
        "last_insp_date": "",
        "source_url":     "",
        "scraped_at":     _now_iso(),
        "expires_at":     _expires_iso(),
    }


def _cleveland_row(row: dict, state: str, city: str) -> dict | None:
    address = _clean(row.get("PRIMARY_ADDRESS", ""))
    if not address:
        return None
    return {
        "state":          state,
        "city":           city,
        "address":        address,
        "parcel_id":      _clean(row.get("PARCEL_NUMBER", "")),
        "violation_type": _clean(row.get("SOURCE", "")),
        "violation_sub":  _clean(row.get("DW_Neighborhood", "")),
        "case_status":    _clean(row.get("VIOLATION_APP_STATUS", "")),
        "filed_date":     _clean(row.get("FILE_DATE", ""))[:10],
        "last_insp_date": "",
        "source_url":     _clean(row.get("VIOLATION_ACCELA_CITIZEN_ACCESS_URL", "")),
        "scraped_at":     _now_iso(),
        "expires_at":     _expires_iso(),
    }


_INDY_CLOSED = {
    'Closed', 'Case Closed', 'Case Closed - Refer to DPW', 'Complied',
    'Abated by Violator', 'Abated by Vendor', 'Abated by Vendor-Aff Recvd',
    'Self Abated', 'VIO-Closed', 'VIO-Closed, Fees Pending',
    'Violation(s) Abated', 'Violation(s) Corrected', 'Violations Corrected',
    'Void', 'Paid', 'Denied', 'Destroyed', 'No Damage',
    'Cleaned by Violator', 'Cleaned by Vendor', 'Cleaned by Vendor-Aff Recvd',
    'Closure', 'Closed - Fees Due', 'Closed, DEM', 'Closed, Fees Due',
    'Closed, Fees Pending', 'Closed, No Violation', 'Closed, Notice',
    'Closed, Proactive HWG', 'Closed, RDA', 'Closed, Reactive HWG',
    'Closed, Refer to DPW', 'Closed, RNH', 'Closed, RWH', 'Closed, VBO',
    'Closed, VIO', 'Closed-Fees Due', 'Closed-Fines Due',
}

def _indy_filter(row: dict) -> bool:
    status = _clean(row.get("CASE_STATUS", ""))
    return bool(status) and status not in _INDY_CLOSED

def _indy_row(row: dict, state: str, city: str) -> dict | None:
    address = _clean(row.get("STREET_ADDRESS", ""))
    if not address:
        return None
    city_f = _clean(row.get("CITY", "")) or "Indianapolis"
    zip_f  = _clean(row.get("ZIP", ""))
    if zip_f:
        address = f"{address}, {city_f}, IN {zip_f}"
    open_date = row.get("OPEN_DATE")
    if isinstance(open_date, (int, float)) and open_date:
        try:
            filed = datetime.fromtimestamp(open_date / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
        except Exception:
            filed = ""
    else:
        filed = _clean(open_date)[:10] if open_date else ""
    return {
        "state":          state,
        "city":           city,
        "address":        address,
        "parcel_id":      _clean(row.get("CASE_NUMBER", "")),
        "violation_type": _clean(row.get("CASE_TYPE", "")),
        "violation_sub":  _clean(row.get("TOWNSHIP", "")),
        "case_status":    _clean(row.get("CASE_STATUS", "")),
        "filed_date":     filed,
        "last_insp_date": "",
        "source_url":     _clean(row.get("LINK", "")),
        "scraped_at":     _now_iso(),
        "expires_at":     _expires_iso(),
    }


def _kcmo_row(row: dict, state: str, city: str) -> dict | None:
    address = _clean(row.get("street_address", ""))
    if not address:
        return None
    zip_code = _clean(row.get("postalcode", ""))
    if zip_code:
        address = f"{address}, Kansas City, MO {zip_code}"
    chapter = _clean(row.get("chapter", ""))
    desc    = _clean(row.get("description", ""))
    vtype   = f"Ch.{chapter} — {desc}" if chapter and desc else (desc or chapter)
    filed   = _clean(row.get("date_found", ""))
    return {
        "state":          state,
        "city":           city,
        "address":        address,
        "parcel_id":      _clean(row.get("pin", "")),
        "violation_type": vtype[:200],
        "violation_sub":  _clean(row.get("ordinance", "")),
        "case_status":    _clean(row.get("case_status", "")),
        "filed_date":     filed[:10] if filed else "",
        "last_insp_date": "",
        "source_url":     "",
        "scraped_at":     _now_iso(),
        "expires_at":     _expires_iso(),
    }


def _austin_row(row: dict, state: str, city: str) -> dict | None:
    address = _clean(row.get("address", ""))
    if not address:
        return None
    return {
        "state":          state,
        "city":           city,
        "address":        address,
        "parcel_id":      _clean(row.get("parcelid", "")),
        "violation_type": _clean(row.get("case_type", "")),
        "violation_sub":  _clean(row.get("description", "")),
        "case_status":    _clean(row.get("status", "")),
        "filed_date":     _clean(row.get("opened_date", ""))[:10],
        "last_insp_date": _clean(row.get("date_updated", ""))[:10],
        "source_url":     _clean(row.get("violationcaselink", "")),
        "scraped_at":     _now_iso(),
        "expires_at":     _expires_iso(),
    }


def _lacity_row(row: dict, state: str, city: str) -> dict | None:
    # Build address from LADBS parts: stno + predir + stname + suffix
    parts = [
        _clean(row.get("stno", "")),
        _clean(row.get("predir", "")),
        _clean(row.get("stname", "")),
        _clean(row.get("suffix", "")),
    ]
    address = " ".join(p for p in parts if p)
    if not address.strip():
        return None
    zip_code = _clean(row.get("zip", ""))
    if zip_code:
        address = f"{address}, Los Angeles, CA {zip_code}"
    filed = _clean(row.get("adddttm", ""))
    return {
        "state":          state,
        "city":           city,
        "address":        address.strip(),
        "parcel_id":      _clean(row.get("prclid", "")),
        "violation_type": _clean(row.get("aptype", "")),
        "violation_sub":  _clean(row.get("apc", "")),
        "case_status":    _clean(row.get("stat", "")),
        "filed_date":     filed[:10] if filed else "",
        "last_insp_date": "",
        "source_url":     "",
        "scraped_at":     _now_iso(),
        "expires_at":     _expires_iso(),
    }


def _sf_row(row: dict, state: str, city: str) -> dict | None:
    parts = [
        _clean(row.get("street_number", "")),
        _clean(row.get("street_name", "")),
        _clean(row.get("street_suffix", "")),
    ]
    address = " ".join(p for p in parts if p)
    if not address.strip():
        return None
    zip_code = _clean(row.get("zipcode", ""))
    if zip_code:
        address = f"{address}, San Francisco, CA {zip_code}"
    block = _clean(row.get("block", ""))
    lot   = _clean(row.get("lot", ""))
    parcel = f"{block}-{lot}" if block and lot else ""
    filed = _clean(row.get("date_filed", ""))
    return {
        "state":          state,
        "city":           city,
        "address":        address.strip(),
        "parcel_id":      parcel,
        "violation_type": _clean(row.get("nov_category_description", "")),
        "violation_sub":  "",
        "case_status":    _clean(row.get("status", "")),
        "filed_date":     filed[:10] if filed else "",
        "last_insp_date": "",
        "source_url":     "",
        "scraped_at":     _now_iso(),
        "expires_at":     _expires_iso(),
    }


CITY_CONFIGS = [
    # ── OHIO ─────────────────────────────────────────────────────────────────
    # Cleveland — Building Complaint Violation Notices (ArcGIS FeatureServer)
    {
        "state": "Ohio",
        "city":  "Cleveland",
        "source_type": "arcgis_fs",
        "url":   "https://services3.arcgis.com/dty2kHktVXHrqO8i/arcgis/rest/services/Complaint_Violation_Notices/FeatureServer/0",
        "arcgis_where": "VIOLATION_APP_STATUS='Open'",
        "verify_ssl": True,
        "row_fn": _cleveland_row,
        "filter_fn": None,
    },
    {
        "state": "Ohio",
        "city":  "Columbus",
        "url":   "https://opendata.columbus.gov/datasets/columbus::code-enforcement-cases.csv?where=1%3D1&outFields=*&outSR=4326&f=csv",
        "verify_ssl": True,
        "row_fn": _columbus_row,
        "filter_fn": _columbus_filter,
    },

    # ── FLORIDA ───────────────────────────────────────────────────────────────
    # Hillsborough County (Tampa area) — ArcGIS FeatureServer, 2,793 active cases
    {
        "state": "Florida",
        "city":  "Tampa",
        "source_type": "arcgis_fs",
        "url":   "https://services.arcgis.com/apTfC6SUmnNfnxuF/arcgis/rest/services/CodeEnforcementCasesMapService/FeatureServer/0",
        "arcgis_where": "1=1",
        "verify_ssl": True,
        "row_fn": _hillsborough_row,
        "filter_fn": _hillsborough_filter,
    },

    # ── TENNESSEE ─────────────────────────────────────────────────────────────
    # Nashville Metro Codes — Property Standards Violations (ArcGIS FeatureServer)
    {
        "state": "Tennessee",
        "city":  "Nashville",
        "source_type": "arcgis_fs",
        "url":   "https://services2.arcgis.com/HdTo6HJqh92wn4D8/arcgis/rest/services/Property_Standards_Violations_2/FeatureServer/0",
        "arcgis_where": "Status='OPEN'",
        "verify_ssl": True,
        "row_fn": _nashville_row,
        "filter_fn": None,
    },

    # ── TEXAS ─────────────────────────────────────────────────────────────────
    {
        "state": "Texas",
        "city":  "Austin",
        "url":   "https://data.austintexas.gov/resource/6wtj-zbtb.csv?$limit=500000&$where=status=%27Active%27",
        "verify_ssl": True,
        "row_fn": _austin_row,
        "filter_fn": None,
    },
    # Houston — ArcGIS FeatureServer (active = New, Recurring, Verified - Referred to Service Center)
    {
        "state": "Texas",
        "city":  "Houston",
        "source_type": "arcgis_fs",
        "url":   "https://services.arcgis.com/NummVBqZSIJKUeVR/arcgis/rest/services/CodeEnforcementFieldWork_view_layer/FeatureServer/0",
        "arcgis_where": "ViolationStatus IN ('New','Recurring','Verified - Referred to Service Center')",
        "verify_ssl": True,
        "row_fn": _houston_row,
        "filter_fn": None,
    },

    # ── NEVADA ────────────────────────────────────────────────────────────────
    # Las Vegas City — Code Enforcement Open Data (ArcGIS FeatureServer)
    # 9,357 open violations | Fields: ADDRESS, STAT, DESCRIPT, Parcel_Number, Event_Date
    {
        "state": "Nevada",
        "city":  "Las Vegas",
        "source_type": "arcgis_fs",
        "url":   "https://services1.arcgis.com/F1v0ufATbBQScMtY/arcgis/rest/services/Code_Enforcement_Open_Data/FeatureServer/0",
        "arcgis_where": "STAT='Open'",
        "verify_ssl": True,
        "row_fn": _lasvegas_row,
        "filter_fn": None,
    },

    # ── INDIANA ───────────────────────────────────────────────────────────────
    # Indianapolis — Code Enforcement Violations and Investigations (ArcGIS MapServer)
    # 910k total records; server-side filter excludes closed/resolved statuses.
    # Fields: STREET_ADDRESS, CASE_NUMBER, CASE_TYPE, CASE_STATUS, OPEN_DATE, OWNER, TOWNSHIP, LINK
    {
        "state": "Indiana",
        "city":  "Indianapolis",
        "source_type": "arcgis_fs",
        "url":   "https://gis.indy.gov/server/rest/services/OpenData/OpenData_NonSpatial/MapServer/1",
        "arcgis_where": (
            "CASE_STATUS NOT IN ('Closed','Case Closed','Case Closed - Refer to DPW',"
            "'Complied','Abated by Violator','Abated by Vendor','Abated by Vendor-Aff Recvd',"
            "'Self Abated','VIO-Closed','VIO-Closed, Fees Pending',"
            "'Violation(s) Abated','Violation(s) Corrected','Violations Corrected',"
            "'Void','Paid','Denied','Destroyed','No Damage',"
            "'Cleaned by Violator','Cleaned by Vendor','Cleaned by Vendor-Aff Recvd',"
            "'Closure','Closed - Fees Due','Closed, DEM','Closed, Fees Due',"
            "'Closed, Fees Pending','Closed, No Violation','Closed, Notice',"
            "'Closed, Proactive HWG','Closed, RDA','Closed, Reactive HWG',"
            "'Closed, Refer to DPW','Closed, RNH','Closed, RWH','Closed, VBO',"
            "'Closed, VIO','Closed-Fees Due','Closed-Fines Due') AND CASE_STATUS IS NOT NULL"
        ),
        "verify_ssl": True,
        "row_fn": _indy_row,
        "filter_fn": _indy_filter,
    },

    # ── MISSOURI ──────────────────────────────────────────────────────────────
    # Kansas City — Property Violations from EnerGov (Socrata vq3e-m9ge)
    # Confirmed active status values: In-Violation, Pending Investigation,
    # Court Case Pending, Case Pending (NOT 'Open' — that value doesn't exist)
    {
        "state": "Missouri",
        "city":  "Kansas City",
        "url":   (
            "https://data.kcmo.org/resource/vq3e-m9ge.csv?$limit=500000"
            "&$where=case_status+IN+(%27In-Violation%27%2C%27Pending+Investigation%27"
            "%2C%27Court+Case+Pending%27%2C%27Case+Pending%27)"
        ),
        "verify_ssl": True,
        "row_fn": _kcmo_row,
        "filter_fn": None,
    },

    # ── CALIFORNIA ────────────────────────────────────────────────────────────
    # LA City (LADBS) — Socrata u82d-eh7z, ~29k open cases
    # Fields: apno, stno, predir, stname, suffix, zip, adddttm, prclid, aptype, apc, stat
    {
        "state": "California",
        "city":  "Los Angeles",
        "url":   "https://data.lacity.org/resource/u82d-eh7z.csv?$limit=500000&$where=stat=%27O%27",
        "verify_ssl": True,
        "row_fn": _lacity_row,
        "filter_fn": None,
    },
    # San Francisco (DBI Notices of Violation) — Socrata nbtm-fbw5, ~29k active
    # Fields: complaint_number, street_number, street_name, street_suffix, zipcode,
    #         status, nov_category_description, block, lot, date_filed
    {
        "state": "California",
        "city":  "San Francisco",
        "url":   "https://data.sfgov.org/resource/nbtm-fbw5.csv?$limit=500000&$where=status=%27active%27",
        "verify_ssl": True,
        "row_fn": _sf_row,
        "filter_fn": None,
    },

    # ── INDIANA ───────────────────────────────────────────────────────────────
    # Indianapolis - code violations (verify dataset ID)
    # {
    #     "state": "Indiana",
    #     "city":  "Indianapolis",
    #     "url":   "https://data.indy.gov/resource/VERIFY_ID.csv?$limit=500000",
    #     "verify_ssl": True,
    #     "row_fn": _generic_row,
    #     "filter_fn": None,
    # },
]


def _generic_row(row: dict, state: str, city: str) -> dict | None:
    """Generic mapper — tries common column name patterns."""
    keys = {k.upper().strip(): v for k, v in row.items()}

    def get(*candidates):
        for c in candidates:
            v = keys.get(c.upper())
            if v is not None and str(v).strip():
                return str(v).strip()
        return ""

    address = get("SITE_ADDRESS", "ADDRESS", "LOCATION", "PROPERTY_ADDRESS",
                  "SITUS_ADDRESS", "VIOLATION_ADDRESS", "STREET_ADDRESS")
    if not address:
        return None

    return {
        "state":          state,
        "city":           city,
        "address":        address,
        "parcel_id":      get("PARCEL_ID", "PARCEL", "PARCEL_NBR", "PARCEL_NUMBER", "APN"),
        "violation_type": get("VIOLATION_TYPE", "CASE_TYPE", "TYPE", "B1_PER_TYPE",
                              "VIOLATION_DESC", "DESCRIPTION", "CODE_SECTION"),
        "violation_sub":  get("VIOLATION_SUB", "SUB_TYPE", "B1_PER_SUB_TYPE", "CATEGORY"),
        "case_status":    get("STATUS", "CASE_STATUS", "B1_APPL_STATUS", "APPL_STATUS"),
        "filed_date":     get("FILED_DATE", "FILE_DATE", "B1_FILE_DD", "OPEN_DATE",
                              "CASE_DATE", "COMPLAINT_DATE"),
        "last_insp_date": get("LAST_INSP_DATE", "INSP_LAST_DATE", "INSPECTION_DATE",
                              "LAST_ACTIVITY_DATE"),
        "source_url":     get("ACA_URL", "URL", "LINK", "CASE_URL"),
        "scraped_at":     _now_iso(),
        "expires_at":     _expires_iso(),
    }


# ─── ST. LOUIS ROLLING-WINDOW SCRAPER ────────────────────────────────────────
# Uses stlcitypermits.com JSON API (90-day max window; queries in 7-day chunks).
# Returns residential inspections that have actual violations or cited results.

_STL_VIOLATION_KEYWORDS = {
    "VIOLATION", "CITATION", "CONDEMN", "WARNING",
    "NOTICE", "FAILED", "DEFICIENT", "COMPLAINT",
}

def _stl_query_window(start_str: str, end_str: str) -> list[dict]:
    url = "https://www.stlcitypermits.com/API/HCES/GetAllInspectionsReporting"
    try:
        r = requests.get(
            url,
            params={"startDate": start_str, "endDate": end_str},
            headers=HEADERS,
            timeout=30,
        )
        if not r.ok or not r.content:
            return []
        return r.json()
    except Exception:
        return []


def scrape_stlouis_mo() -> list[dict]:
    """
    St. Louis City MO — stlcitypermits.com/API/HCES/GetAllInspectionsReporting
    ~85 records/7-day window; query last 90 days in weekly chunks.
    Keeps residential inspections with violations or violation-related results.
    Fields with owner: OwnerName, OwnerAddress/City/State/ZipCode,
                       ProjectAddress, ProjectZipCode, ProjectASRParcelID,
                       CurrentResult, Violations (array), ProjectNeighborhood
    """
    from datetime import timedelta
    print("  [St. Louis, MO] Querying last 90 days of inspections...", end="", flush=True)

    end_dt   = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=90)

    raw = []
    cur = start_dt
    while cur < end_dt:
        win_end = min(cur + timedelta(days=7), end_dt)
        raw.extend(_stl_query_window(
            cur.strftime("%Y-%m-%d"),
            win_end.strftime("%Y-%m-%d"),
        ))
        cur = win_end

    leads  = []
    skipped = 0
    seen   = set()

    for rec in raw:
        use_code = _clean(rec.get("UseCode", "")).upper()
        if use_code and "RESIDENTIAL" not in use_code:
            skipped += 1
            continue

        violations = rec.get("Violations") or []
        result_upper = _clean(rec.get("CurrentResult", "")).upper()
        has_violation = bool(violations) or any(
            kw in result_upper for kw in _STL_VIOLATION_KEYWORDS
        )
        if not has_violation:
            skipped += 1
            continue

        insp_id = str(rec.get("InspectionID", ""))
        if insp_id in seen:
            continue
        seen.add(insp_id)

        address = _clean(rec.get("ProjectAddress", ""))
        if not address:
            continue
        zip_code = _clean(rec.get("ProjectZipCode", ""))
        if zip_code:
            address = f"{address}, St. Louis, MO {zip_code}"

        vio_types = ", ".join(
            _clean(v.get("ViolationItemDescription", v.get("ViolationItem", "")))
            for v in violations if isinstance(v, dict)
        )[:200]

        leads.append({
            "state":          "Missouri",
            "city":           "St. Louis",
            "address":        address,
            "parcel_id":      _clean(rec.get("ProjectASRParcelID", "")),
            "violation_type": vio_types or _clean(rec.get("CurrentResult", "")),
            "violation_sub":  _clean(rec.get("ProjectNeighborhood", "")),
            "case_status":    _clean(rec.get("CurrentResult", "")),
            "filed_date":     _clean(rec.get("CreatedDate", ""))[:10],
            "last_insp_date": _clean(rec.get("ModifiedDate", ""))[:10],
            "source_url":     "",
            "scraped_at":     _now_iso(),
            "expires_at":     _expires_iso(),
        })

    print(f" {len(leads):,} residential violations (skipped {skipped:,})")
    return leads


# ─── SCRAPER ──────────────────────────────────────────────────────────────────

def scrape_city(config: dict) -> list[dict]:
    state       = config["state"]
    city        = config["city"]
    url         = config["url"]
    source_type = config.get("source_type", "csv")
    row_fn      = config.get("row_fn", _generic_row)
    filter_fn   = config.get("filter_fn")
    verify      = config.get("verify_ssl", True)

    print(f"  [{city}, {state}] Downloading...", end="", flush=True)

    if source_type == "arcgis_fs":
        rows = _get_arcgis_fs(url, where=config.get("arcgis_where", "1=1"))
    else:
        rows = _get_csv(url, verify_ssl=verify)

    if rows is None:
        print(" failed")
        return []

    leads = []
    for row in rows:
        if filter_fn and not filter_fn(row):
            continue
        lead = row_fn(row, state, city)
        if lead:
            leads.append(lead)

    print(f" {len(leads):,} active violations")
    return leads


# ─── DB SAVE ──────────────────────────────────────────────────────────────────

def _save_leads(db, leads: list[dict], city: str) -> int:
    if not leads:
        print(f"  → [{city}] No leads to save")
        return 0

    # Deduplicate by (state, city, address, violation_type)
    seen = {}
    for lead in leads:
        key = (lead["state"], lead["city"], lead["address"], lead["violation_type"])
        seen[key] = lead
    unique = list(seen.values())

    CHUNK = 500
    total = 0
    for i in range(0, len(unique), CHUNK):
        chunk = unique[i:i + CHUNK]
        try:
            resp = db.table("code_violation_leads").upsert(
                chunk,
                on_conflict="state,city,address,violation_type"
            ).execute()
            total += len(resp.data) if resp.data else len(chunk)
        except Exception as e:
            print(f"  → [{city}] DB error chunk {i//CHUNK+1}: {e}")

    print(f"  → [{city}] {total:,} violations saved")
    return total


# ─── MAIN RUNNER ──────────────────────────────────────────────────────────────

def run(state: str | None = None, city: str | None = None):
    from supabase import create_client

    url_env = os.environ.get("SUPABASE_URL", "")
    key_env = os.environ.get("SUPABASE_SERVICE_KEY", "")
    if not url_env or not key_env:
        print("ERROR: Set SUPABASE_URL and SUPABASE_SERVICE_KEY.")
        return

    db = create_client(url_env, key_env)

    configs = [
        c for c in CITY_CONFIGS
        if (not state or c["state"].lower() == state.lower()) and
           (not city  or c["city"].lower()  == city.lower())
    ]

    # St. Louis uses a custom rolling-window API outside CITY_CONFIGS
    run_stl = (not city or city.lower() == "st. louis") and \
              (not state or state.lower() == "missouri")

    if not configs and not run_stl:
        print(f"No confirmed sources for state='{state}' city='{city}'")
        return

    city_count = len(configs) + (1 if run_stl else 0)
    print(f"\n{'='*55}")
    print(f"  Code Violations Scraper — {city_count} city/cities")
    print(f"{'='*55}\n")

    total = 0
    for config in configs:
        leads = scrape_city(config)
        total += _save_leads(db, leads, config["city"])
        time.sleep(random.uniform(2, 4))

    if run_stl:
        leads = scrape_stlouis_mo()
        total += _save_leads(db, leads, "St. Louis")
        time.sleep(random.uniform(2, 4))

    print(f"\n{'='*55}")
    print(f"  Done. Total saved: {total:,}")
    print(f"{'='*55}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", type=str, default=None)
    parser.add_argument("--city",  type=str, default=None)
    args = parser.parse_args()
    run(state=args.state, city=args.city)
