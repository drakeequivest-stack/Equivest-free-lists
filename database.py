"""
FSBO Scraper — Supabase operations
"""
import streamlit as st
import requests as _http
from supabase import create_client, Client
from datetime import datetime, timezone

def _admin() -> Client:
    return create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_SERVICE_KEY"])

def _anon() -> Client:
    return create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_ANON_KEY"])

CSV_BUCKET = "csv-exports"

def upload_csv(data: bytes, filename: str) -> str:
    """Upload CSV to Supabase Storage and return public URL. Overwrites if exists."""
    try:
        sb = _admin()
        path = f"exports/{filename}"
        sb.storage.from_(CSV_BUCKET).upload(
            path, data,
            file_options={"content-type": "text/csv", "upsert": "true"},
        )
        return sb.storage.from_(CSV_BUCKET).get_public_url(path)
    except Exception as e:
        print(f"[DB] upload_csv error: {e}")
        return ""


def _fetch_csv_export(table: str, select: str,
                      filters: list[tuple[str, str]],
                      order: str,
                      limit: int = 500_000,
                      offset: int = 0) -> bytes:
    """
    Direct PostgREST CSV export — single HTTP request, bypasses SDK's 1k-row cap.
    Returns raw CSV bytes ready for st.download_button.
    `filters` is a list of (column, postgrest_value) e.g. ("state","eq.Arizona").
    Use ("col","neq.") to exclude empty strings.
    """
    base_url = st.secrets.get("SUPABASE_URL", "")
    svc_key  = st.secrets.get("SUPABASE_SERVICE_KEY", "")
    if not base_url or not svc_key:
        return b""
    url = f"{base_url}/rest/v1/{table}"
    headers = {
        "apikey":        svc_key,
        "Authorization": f"Bearer {svc_key}",
        "Accept":        "text/csv",
        "Prefer":        "count=none",
    }
    # list-of-tuples preserves duplicate keys (e.g. two filters on same column)
    params: list[tuple[str, str]] = [
        ("select", select),
        ("order",  order),
        ("limit",  str(limit)),
        ("offset", str(offset)),
    ]
    params.extend(filters)
    try:
        r = _http.get(url, headers=headers, params=params, timeout=120)
        return r.content if r.ok else b""
    except Exception as e:
        print(f"[DB] _fetch_csv_export error: {e}")
        return b""


def _count_by(table: str, **eq_filters) -> int:
    """Fast exact-count query filtered by arbitrary equality conditions."""
    try:
        q = _admin().table(table).select("id", count="exact")
        for col, val in eq_filters.items():
            q = q.eq(col, val)
        return q.execute().count or 0
    except Exception:
        return 0


# ── Auth (reuses existing Equivest Supabase project) ──────────────────────────

def sign_in(email: str, password: str) -> tuple[dict | None, str]:
    try:
        resp = _anon().auth.sign_in_with_password({"email": email, "password": password})
        if resp.user:
            return {"id": str(resp.user.id), "email": resp.user.email}, ""
        return None, "Invalid email or password."
    except Exception as e:
        msg = str(e)
        if "Invalid login credentials" in msg:
            return None, "Invalid email or password."
        return None, "Login error. Please try again."

def sign_up(email: str, password: str) -> tuple[dict | None, str]:
    try:
        resp = _anon().auth.sign_up({"email": email, "password": password})
        if resp.user:
            return {"id": str(resp.user.id), "email": resp.user.email}, ""
        return None, "Sign-up failed."
    except Exception as e:
        msg = str(e)
        if "already registered" in msg or "already exists" in msg:
            return None, "Account already exists. Please log in."
        return None, f"Error: {msg}"


# ── Leads ─────────────────────────────────────────────────────────────────────

def get_fsbo_count(state: str) -> int:
    try:
        now = datetime.now(timezone.utc).isoformat()
        resp = (
            _admin()
            .table("fsbo_leads")
            .select("id", count="exact")
            .eq("state", state)
            .gt("expires_at", now)
            .execute()
        )
        return resp.count or 0
    except Exception:
        return 0

def get_fsbo_leads_for_download(state: str, limit: int = 500_000, offset: int = 0) -> bytes:
    """CSV bytes — active FSBO leads for state, with optional chunking."""
    now = datetime.now(timezone.utc).isoformat()
    return _fetch_csv_export(
        "fsbo_leads",
        "title,price,city,state,address,phone,url,source,posted_at",
        [
            ("state",      f"eq.{state}"),
            ("expires_at", f"gt.{now}"),
        ],
        "posted_at.desc",
        limit=limit,
        offset=offset,
    )

def upsert_leads(leads: list[dict]) -> int:
    """Insert new leads, skip duplicates by URL. Returns count inserted."""
    if not leads:
        return 0
    try:
        resp = _admin().table("fsbo_leads").upsert(leads, on_conflict="url").execute()
        return len(resp.data) if resp.data else 0
    except Exception as e:
        print(f"[DB] upsert_leads error: {e}")
        return 0

def get_leads(state: str, user_id: str) -> list[dict]:
    """
    Get active leads for a state.
    Excludes leads claimed by OTHER users with active (non-expired) claims.
    Includes leads claimed by THIS user.
    """
    try:
        now = datetime.now(timezone.utc).isoformat()
        # Get all active leads for the state
        resp = (
            _admin()
            .table("fsbo_leads")
            .select("*, fsbo_claims(user_id, claim_expires_at)")
            .eq("state", state)
            .gt("expires_at", now)
            .order("posted_at", desc=True)
            .execute()
        )
        leads = resp.data or []

        result = []
        for lead in leads:
            claims = lead.pop("fsbo_claims", []) or []
            # Check for active claims by OTHER users
            active_claim = next(
                (c for c in claims
                 if c["user_id"] != user_id and c["claim_expires_at"] > now),
                None
            )
            my_claim = next(
                (c for c in claims
                 if c["user_id"] == user_id and c["claim_expires_at"] > now),
                None
            )
            lead["claimed_by_other"] = active_claim is not None
            lead["my_claim_expires"] = my_claim["claim_expires_at"] if my_claim else None
            result.append(lead)

        return result
    except Exception as e:
        print(f"[DB] get_leads error: {e}")
        return []

def get_my_claims(user_id: str) -> list[dict]:
    try:
        now = datetime.now(timezone.utc).isoformat()
        resp = (
            _admin()
            .table("fsbo_claims")
            .select("*, fsbo_leads(*)")
            .eq("user_id", user_id)
            .gt("claim_expires_at", now)
            .order("claimed_at", desc=True)
            .execute()
        )
        return resp.data or []
    except Exception as e:
        print(f"[DB] get_my_claims error: {e}")
        return []

def claim_lead(lead_id: str, user_id: str, user_email: str, hours: int = 48) -> tuple[bool, str]:
    try:
        now = datetime.now(timezone.utc).isoformat()
        # Check if already claimed by someone else
        resp = (
            _admin()
            .table("fsbo_claims")
            .select("user_id")
            .eq("lead_id", lead_id)
            .gt("claim_expires_at", now)
            .neq("user_id", user_id)
            .execute()
        )
        if resp.data:
            return False, "Someone just claimed this lead. Try another."

        from datetime import timedelta
        expires = (datetime.now(timezone.utc) + timedelta(hours=hours)).isoformat()
        _admin().table("fsbo_claims").upsert({
            "lead_id":          lead_id,
            "user_id":          user_id,
            "user_email":       user_email,
            "claim_expires_at": expires,
        }, on_conflict="lead_id,user_id").execute()
        return True, expires
    except Exception as e:
        print(f"[DB] claim_lead error: {e}")
        return False, "Error claiming lead."

# ── Tax Delinquent Leads ───────────────────────────────────────────────────────

def get_td_lead_count(state: str) -> int:
    """Count active tax delinquent leads for a state."""
    try:
        now = datetime.now(timezone.utc).isoformat()
        resp = (
            _admin()
            .table("tax_delinquent_leads")
            .select("id", count="exact")
            .eq("state", state)
            .or_(f"expires_at.gt.{now},expires_at.is.null")
            .execute()
        )
        return resp.count or 0
    except Exception:
        return 0

def get_td_leads(state: str, user_id: str) -> list[dict]:
    """Get active tax delinquent leads for a state with claim status."""
    try:
        now = datetime.now(timezone.utc).isoformat()
        resp = (
            _admin()
            .table("tax_delinquent_leads")
            .select("*, tax_delinquent_claims(user_id, claim_expires_at)")
            .eq("state", state)
            .or_(f"expires_at.gt.{now},expires_at.is.null")
            .order("scraped_at", desc=True)
            .execute()
        )
        leads = resp.data or []

        result = []
        for lead in leads:
            claims = lead.pop("tax_delinquent_claims", []) or []
            active_claim = next(
                (c for c in claims
                 if c["user_id"] != user_id and c["claim_expires_at"] > now),
                None
            )
            my_claim = next(
                (c for c in claims
                 if c["user_id"] == user_id and c["claim_expires_at"] > now),
                None
            )
            lead["claimed_by_other"] = active_claim is not None
            lead["my_claim_expires"] = my_claim["claim_expires_at"] if my_claim else None
            result.append(lead)

        return result
    except Exception as e:
        print(f"[DB] get_td_leads error: {e}")
        return []

def get_td_counties(state: str) -> list[str]:
    """Get list of counties with TD data for a state."""
    try:
        resp = (
            _admin()
            .table("tax_delinquent_leads")
            .select("county")
            .eq("state", state)
            .execute()
        )
        seen = set()
        counties = []
        for row in (resp.data or []):
            c = row["county"]
            if c not in seen:
                seen.add(c)
                counties.append(c)
        return sorted(counties)
    except Exception:
        return []

def get_td_county_count(state: str, county: str) -> int:
    return _count_by("tax_delinquent_leads", state=state, county=county)

def get_td_leads_for_download(state: str, county: str, limit: int = 500_000, offset: int = 0) -> bytes:
    """CSV bytes — TD records for state/county with optional chunking."""
    return _fetch_csv_export(
        "tax_delinquent_leads",
        "owner_name,property_address,county,state,parcel_id,amount_owed,assessed_value,tax_year",
        [
            ("state",            f"eq.{state}"),
            ("county",           f"eq.{county}"),
            ("owner_name",       "neq."),
            ("property_address", "neq."),
        ],
        "amount_owed.desc",
        limit=limit,
        offset=offset,
    )

def claim_td_lead(lead_id: str, user_id: str, user_email: str, hours: int = 48) -> tuple[bool, str]:
    try:
        now = datetime.now(timezone.utc).isoformat()
        resp = (
            _admin()
            .table("tax_delinquent_claims")
            .select("user_id")
            .eq("lead_id", lead_id)
            .gt("claim_expires_at", now)
            .neq("user_id", user_id)
            .execute()
        )
        if resp.data:
            return False, "Someone just claimed this lead. Try another."

        from datetime import timedelta
        expires = (datetime.now(timezone.utc) + timedelta(hours=hours)).isoformat()
        _admin().table("tax_delinquent_claims").upsert({
            "lead_id":          lead_id,
            "user_id":          user_id,
            "user_email":       user_email,
            "claim_expires_at": expires,
        }, on_conflict="lead_id,user_id").execute()
        return True, expires
    except Exception as e:
        print(f"[DB] claim_td_lead error: {e}")
        return False, "Error claiming lead."

def get_my_td_claims(user_id: str) -> list[dict]:
    try:
        now = datetime.now(timezone.utc).isoformat()
        resp = (
            _admin()
            .table("tax_delinquent_claims")
            .select("*, tax_delinquent_leads(*)")
            .eq("user_id", user_id)
            .gt("claim_expires_at", now)
            .order("claimed_at", desc=True)
            .execute()
        )
        return resp.data or []
    except Exception as e:
        print(f"[DB] get_my_td_claims error: {e}")
        return []


# ── Absentee Owner Leads ───────────────────────────────────────────────────────

def get_ao_lead_count(state: str) -> int:
    try:
        resp = (
            _admin()
            .table("absentee_owner_leads")
            .select("id", count="exact")
            .eq("state", state)
            .execute()
        )
        return resp.count or 0
    except Exception:
        return 0

def get_ao_counties(state: str) -> list[str]:
    try:
        resp = (
            _admin()
            .table("absentee_owner_leads")
            .select("county")
            .eq("state", state)
            .execute()
        )
        seen = set()
        counties = []
        for row in (resp.data or []):
            c = row["county"]
            if c not in seen:
                seen.add(c)
                counties.append(c)
        return sorted(counties)
    except Exception:
        return []

def get_ao_county_count(state: str, county: str) -> int:
    return _count_by("absentee_owner_leads", state=state, county=county)

def get_ao_leads_for_download(state: str, county: str, limit: int = 500_000, offset: int = 0) -> bytes:
    """CSV bytes — AO records for state/county with optional chunking."""
    return _fetch_csv_export(
        "absentee_owner_leads",
        "owner_name,owner_address,property_address,county,state,parcel_id,amount_owed,source_url",
        [
            ("state",            f"eq.{state}"),
            ("county",           f"eq.{county}"),
            ("owner_name",       "neq."),
            ("property_address", "neq."),
        ],
        "scraped_at.desc",
        limit=limit,
        offset=offset,
    )


# ── Code Violation Leads ───────────────────────────────────────────────────────

def get_cv_lead_count(state: str) -> int:
    try:
        resp = (
            _admin()
            .table("code_violation_leads")
            .select("id", count="exact")
            .eq("state", state)
            .execute()
        )
        return resp.count or 0
    except Exception:
        return 0

def get_cv_cities(state: str) -> list[str]:
    try:
        resp = (
            _admin()
            .table("code_violation_leads")
            .select("city")
            .eq("state", state)
            .execute()
        )
        seen = set()
        cities = []
        for row in (resp.data or []):
            c = row["city"]
            if c not in seen:
                seen.add(c)
                cities.append(c)
        return sorted(cities)
    except Exception:
        return []

def get_cv_city_count(state: str, city: str) -> int:
    return _count_by("code_violation_leads", state=state, city=city)

def get_cv_leads_for_download(state: str, city: str, limit: int = 500_000, offset: int = 0) -> bytes:
    """CSV bytes — CV records for state/city with optional chunking."""
    return _fetch_csv_export(
        "code_violation_leads",
        "address,city,state,parcel_id,violation_type,violation_sub,case_status,filed_date,last_insp_date,source_url",
        [
            ("state",   f"eq.{state}"),
            ("city",    f"eq.{city}"),
            ("address", "neq."),
        ],
        "filed_date.desc",
        limit=limit,
        offset=offset,
    )


def get_last_scraped(state: str) -> str | None:
    try:
        resp = (
            _admin()
            .table("fsbo_leads")
            .select("scraped_at")
            .eq("state", state)
            .order("scraped_at", desc=True)
            .limit(1)
            .execute()
        )
        return resp.data[0]["scraped_at"] if resp.data else None
    except Exception:
        return None
