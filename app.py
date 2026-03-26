"""
Equivest Academy — Lead Finder
"""
import streamlit as st
import database
from config import MARKETS, CLAIM_HOURS
from datetime import datetime, timezone
import base64, json

def _csv_link(data: bytes, filename: str, label: str) -> str:
    """Return an HTML anchor tag that downloads data as a CSV file."""
    b64 = base64.b64encode(data).decode()
    return (
        f'<a href="data:text/csv;base64,{b64}" download="{filename}" '
        f'style="display:block;width:100%;text-align:center;padding:0.6rem 1.5rem;'
        f'background:linear-gradient(135deg,#C9A84C,#E8D070);color:#080a14;'
        f'font-family:Outfit,sans-serif;font-weight:800;font-size:0.95rem;'
        f'border-radius:10px;text-decoration:none;box-sizing:border-box">'
        f'{label}</a>'
    )

def _save_session(user: dict):
    token = base64.urlsafe_b64encode(
        json.dumps({"id": user["id"], "e": user["email"]}).encode()
    ).decode()
    st.query_params["s"] = token

def _load_session() -> dict | None:
    token = st.query_params.get("s")
    if not token:
        return None
    try:
        d = json.loads(base64.urlsafe_b64decode(token + "==").decode())
        return {"id": d["id"], "email": d["e"]}
    except Exception:
        return None

def _clear_session():
    st.query_params.clear()

st.set_page_config(page_title="Lead Finder — Equivest", page_icon="🏠",
                   layout="centered", initial_sidebar_state="collapsed")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700;800;900&display=swap');
*, *::before, *::after { box-sizing: border-box; }

/* Star background */
html, body, .stApp {
  font-family: 'Outfit', sans-serif !important;
  background-color: #080a14 !important;
  color: #F2EFE6 !important;
}
.stApp {
  background-image:
    radial-gradient(circle, rgba(255,255,255,0.12) 1px, transparent 1px),
    radial-gradient(circle, rgba(201,168,76,0.06) 1px, transparent 1px) !important;
  background-size: 60px 60px, 120px 120px !important;
  background-position: 0 0, 30px 30px !important;
}

.block-container { padding: 0 1.5rem 4rem !important; max-width: 960px; position:relative; z-index:1; }
#MainMenu, footer, header { visibility: hidden; }
[data-testid="stSidebar"], [data-testid="collapsedControl"] { display: none; }

/* Header */
.eq-logo { font-size:0.7rem; font-weight:700; letter-spacing:0.3em; text-transform:uppercase;
  color:rgba(201,168,76,0.7); margin-bottom:0.3rem; margin-top:2rem; }
.eq-title { font-size:2.2rem; font-weight:900;
  background:linear-gradient(135deg,#C9A84C 0%,#F0D878 50%,#C9A84C 100%);
  -webkit-background-clip:text; -webkit-text-fill-color:transparent; background-clip:text; }

/* List type cards - fixed size grid */
.list-grid { display:grid; grid-template-columns:repeat(7,1fr); gap:0.7rem; margin:1.5rem 0 2rem; }
.list-card {
  height:130px; border-radius:14px; padding:1rem 0.6rem; text-align:center;
  display:flex; flex-direction:column; justify-content:center; align-items:center; gap:4px;
  border:3px solid; position:relative; overflow:hidden;
}
.list-card-icon { font-size:1.5rem; line-height:1; }
.list-card-name { font-size:0.72rem; font-weight:800; color:#F2EFE6; letter-spacing:0.02em; line-height:1.2; }
.list-card-count { font-size:1.8rem; font-weight:900; line-height:1; }
.list-card-sub { font-size:0.65rem; color:rgba(242,239,230,0.45); }

/* Card colors */
.card-gold   { background:rgba(201,168,76,0.1);  border-color:#C9A84C; box-shadow:0 0 22px rgba(201,168,76,0.35); }
.card-gold .list-card-count { color:#C9A84C; }
.card-red    { background:rgba(220,60,60,0.1);   border-color:#e05555; box-shadow:0 0 22px rgba(220,60,60,0.3); }
.card-red .list-card-count { color:#e05555; }
.card-orange { background:rgba(230,130,50,0.1);  border-color:#E07D35; box-shadow:0 0 22px rgba(230,130,50,0.3); }
.card-orange .list-card-count { color:#E07D35; }
.card-purple { background:rgba(139,92,246,0.1);  border-color:#8B5CF6; box-shadow:0 0 22px rgba(139,92,246,0.3); }
.card-purple .list-card-count { color:#8B5CF6; }
.card-blue   { background:rgba(59,130,246,0.1);  border-color:#60A5FA; box-shadow:0 0 22px rgba(59,130,246,0.3); }
.card-blue .list-card-count { color:#60A5FA; }
.card-green  { background:rgba(16,185,129,0.1);  border-color:#10B981; box-shadow:0 0 22px rgba(16,185,129,0.3); }
.card-green .list-card-count { color:#10B981; }
.card-teal   { background:rgba(20,184,166,0.1);  border-color:#14B8A6; box-shadow:0 0 22px rgba(20,184,166,0.3); }
.card-teal .list-card-count { color:#14B8A6; }
.card-soon { opacity:0.62; }

/* Coming soon chips */
.soon-chip {
  font-size:0.72rem; font-weight:700; padding:4px 12px; border-radius:20px;
  border:1px solid; opacity:0.45;
}
.soon-chip.card-orange { border-color:#E07D35; color:#E07D35; background:rgba(230,130,50,0.07); }
.soon-chip.card-purple { border-color:#8B5CF6; color:#8B5CF6; background:rgba(139,92,246,0.07); }
.soon-chip.card-green  { border-color:#10B981; color:#10B981; background:rgba(16,185,129,0.07); }
.soon-chip.card-gold   { border-color:#C9A84C; color:#C9A84C; background:rgba(201,168,76,0.07); }

/* Lead cards */
.lead-card { background:#0f1120; border:2px solid rgba(201,168,76,0.2);
  border-radius:14px; padding:1.4rem 1.6rem; margin-bottom:1rem;
  box-shadow:0 2px 20px rgba(0,0,0,0.4); }
.lead-card.mine { border-color:rgba(201,168,76,0.55); background:rgba(201,168,76,0.05);
  box-shadow:0 0 24px rgba(201,168,76,0.15); }
.lead-card.claimed { opacity:0.3; }
.lead-title { font-size:1.05rem; font-weight:700; color:#F2EFE6; margin-bottom:0.4rem; line-height:1.4; }
.lead-price { font-size:1.5rem; font-weight:900; color:#C9A84C; margin-bottom:0.3rem; }
.lead-meta { font-size:0.87rem; color:rgba(242,239,230,0.5); margin-top:0.2rem; }
.lead-phone { font-size:1.1rem; font-weight:800; color:#F2EFE6; margin-top:0.6rem; }
.badge-claimed { background:rgba(220,60,60,0.15); color:#e05555;
  border:1px solid rgba(220,60,60,0.3); border-radius:20px; padding:3px 12px; font-size:0.78rem; font-weight:700; }
.badge-mine { background:rgba(201,168,76,0.15); color:#C9A84C;
  border:1px solid rgba(201,168,76,0.4); border-radius:20px; padding:3px 12px; font-size:0.78rem; font-weight:700; }
.section-label { font-size:0.8rem; font-weight:700; letter-spacing:0.18em;
  text-transform:uppercase; color:rgba(201,168,76,0.65); margin:1.8rem 0 0.9rem; }
.no-results { text-align:center; padding:3rem 1rem; color:rgba(242,239,230,0.3); font-size:1rem; }

/* Rules */
.rules-box { background:rgba(201,168,76,0.04); border:1px solid rgba(201,168,76,0.18);
  border-left:3px solid #C9A84C; border-radius:10px; padding:1rem 1.4rem; margin:1rem 0 1.5rem; }
.rules-box p { font-size:0.88rem; color:rgba(242,239,230,0.6); margin:0.28rem 0; }

/* Toggle */
.toggle-wrap { display:flex; background:#0f1120; border:2px solid rgba(201,168,76,0.2);
  border-radius:12px; padding:4px; gap:4px; margin-bottom:2rem; }
.toggle-btn { flex:1; padding:0.65rem 1rem; border-radius:9px; text-align:center;
  font-size:0.95rem; font-weight:700; cursor:pointer; transition:all 0.2s;
  color:rgba(242,239,230,0.4); }
.toggle-btn.active { background:linear-gradient(135deg,#C9A84C,#E8D070); color:#080a14; }

/* Inputs */
.stSelectbox > div > div { background:#0f1120 !important; border:2px solid rgba(201,168,76,0.25) !important;
  border-radius:10px !important; color:#F2EFE6 !important; font-size:1rem !important; }
.stTextInput input { background:#0f1120 !important; border:2px solid rgba(201,168,76,0.25) !important;
  border-radius:10px !important; color:#F2EFE6 !important; font-size:1rem !important; padding:0.6rem 0.9rem !important; }
.stButton > button { background:linear-gradient(135deg,#C9A84C,#E8D070) !important;
  color:#080a14 !important; font-family:'Outfit',sans-serif !important; font-weight:800 !important;
  font-size:0.95rem !important; border:none !important; border-radius:10px !important;
  padding:0.6rem 1.5rem !important; }
.stButton > button:hover { opacity:0.87 !important; }
label { color:rgba(242,239,230,0.7) !important; font-size:0.9rem !important;
  font-weight:600 !important; letter-spacing:0.03em !important; }
div[data-testid="stMetricValue"] { font-size:2rem !important; font-weight:900 !important; color:#F2EFE6 !important; }
div[data-testid="stMetricLabel"] { font-size:0.85rem !important; color:rgba(242,239,230,0.5) !important; }

/* ── Kill ALL Streamlit rerun fades / transitions ── */
[data-testid="stStatusWidget"] { display:none !important; }
*, *::before, *::after {
  animation-duration: 0s !important;
  animation-delay: 0s !important;
  transition-duration: 0s !important;
  transition-delay: 0s !important;
}
/* Re-allow our intentional UI transitions */
.toggle-btn { transition-duration: 0.2s !important; }
.stButton > button { transition-duration: 0.15s !important; }
</style>
""", unsafe_allow_html=True)


# ── Auth wall ──────────────────────────────────────────────────────────────────
def show_auth():
    if st.session_state.get("user"):
        return True

    # Try to restore from query param session token
    if not st.session_state.get("logged_out"):
        user = _load_session()
        if user:
            st.session_state["user"] = user
            return True

    st.markdown("""
    <div style="text-align:center;padding:3.5rem 1rem 2.5rem">
      <div style="font-size:0.72rem;font-weight:700;letter-spacing:0.3em;text-transform:uppercase;
        color:rgba(201,168,76,0.7);margin-bottom:0.5rem">Equivest Academy</div>
      <div style="font-size:3rem;font-weight:900;
        background:linear-gradient(135deg,#C9A84C,#F0D878,#C9A84C);
        -webkit-background-clip:text;-webkit-text-fill-color:transparent;
        background-clip:text;margin-bottom:0.7rem">Lead Finder</div>
      <div style="font-size:1rem;color:rgba(242,239,230,0.45)">
        Exclusive leads for Equivest Academy members</div>
    </div>""", unsafe_allow_html=True)

    _, col, _ = st.columns([1, 2, 1])
    with col:
        mode = st.radio("Mode", ["Log In", "Create Account"], horizontal=True, label_visibility="collapsed")
        st.divider()
        email    = st.text_input("Email", placeholder="you@example.com")
        password = st.text_input("Password", type="password", placeholder="••••••••")
        err_box  = st.empty()

        if mode == "Log In":
            if st.button("Log In →", use_container_width=True):
                if email and password:
                    user, err = database.sign_in(email.strip().lower(), password)
                    if user:
                        st.session_state["user"] = user
                        st.session_state.pop("logged_out", None)
                        _save_session(user)
                        st.rerun()
                    else:
                        err_box.error(err)
        else:
            if st.button("Create Account →", use_container_width=True):
                if email and password:
                    user, err = database.sign_up(email.strip().lower(), password)
                    if user:
                        st.session_state["user"] = user
                        st.session_state.pop("logged_out", None)
                        _save_session(user)
                        st.rerun()
                    else:
                        err_box.error(err)
    return False

if not show_auth():
    st.stop()

user       = st.session_state["user"]
user_email = user["email"]

CHUNK = 20_000

def _download_buttons(count, filename_base, fetch_fn, label_color="#C9A84C"):
    """Render one download link per 20k-row chunk."""
    chunks = max(1, -(-count // CHUNK))  # ceiling division
    with st.spinner("Preparing download..."):
        if chunks == 1:
            data = fetch_fn(0, count)
            st.markdown(
                _csv_link(data, f"{filename_base}.csv", f"⬇️  Download {count:,} Records as CSV"),
                unsafe_allow_html=True,
            )
        else:
            cols = st.columns(min(chunks, 3))
            for i in range(chunks):
                offset  = i * CHUNK
                end     = min(offset + CHUNK, count)
                data    = fetch_fn(offset, CHUNK)
                with cols[i % 3]:
                    st.markdown(
                        _csv_link(data, f"{filename_base}_part{i+1}.csv", f"⬇️  Rows {offset+1:,}–{end:,}"),
                        unsafe_allow_html=True,
                    )


# ── Header ─────────────────────────────────────────────────────────────────────
col_h, col_out = st.columns([5, 1])
with col_h:
    st.markdown('<div class="eq-logo">Equivest Academy</div><div class="eq-title">Lead Finder</div>',
                unsafe_allow_html=True)
with col_out:
    st.markdown("<div style='height:2.5rem'></div>", unsafe_allow_html=True)
    if st.button("Sign Out"):
        st.session_state.pop("user", None)
        st.session_state["logged_out"] = True
        _clear_session()
        st.rerun()

st.markdown(f"<p style='font-size:0.8rem;color:rgba(242,239,230,0.22);margin-top:-0.3rem'>{user_email}</p>",
            unsafe_allow_html=True)


# ── State selector ─────────────────────────────────────────────────────────────
state = st.selectbox("Select Your State", list(MARKETS.keys()), index=0)

# ── List type selector ─────────────────────────────────────────────────────────
if "list_type" not in st.session_state:
    st.session_state["list_type"] = "fsbo"

# ── Counts for cards ───────────────────────────────────────────────────────────
fsbo_count = database.get_fsbo_count(state)
td_count   = database.get_td_lead_count(state)
ao_count   = database.get_ao_lead_count(state)
cv_count   = database.get_cv_lead_count(state)

# ── List type cards ────────────────────────────────────────────────────────────
ACTIVE_TYPES = [
    ("fsbo", "🏠", "FSBO",            fsbo_count, f"{fsbo_count:,} leads", "card-gold"),
    ("td",   "📋", "Tax Delinquent",  td_count,   f"{td_count:,} total",   "card-red"),
    ("ao",   "🏢", "Absentee Owner",  ao_count,   f"{ao_count:,} total",   "card-blue"),
    ("cv",   "🚨", "Code Violations", cv_count,   f"{cv_count:,} total",   "card-teal"),
]

COMING_SOON = [
    ("🏚️", "Pre-Foreclosure", "card-orange"),
    ("⚖️",  "Divorce",         "card-purple"),
    ("💰", "Free & Clear",    "card-green"),
    ("📬", "Probate",         "card-gold"),
]

# Active cards
card_cols = st.columns(len(ACTIVE_TYPES))
for i, (lt_key, icon, name, count, sub, color) in enumerate(ACTIVE_TYPES):
    active = count > 0
    with card_cols[i]:
        soon_class = "" if active else " card-soon"
        count_html = f'<div class="list-card-count">{count:,}</div>' if active else ""
        st.markdown(f"""
        <div class="list-card {color}{soon_class}">
          <div class="list-card-icon">{icon}</div>
          <div class="list-card-name">{name}</div>
          {count_html}
          <div class="list-card-sub">{sub}</div>
        </div>""", unsafe_allow_html=True)
        if active:
            if st.button("View", key=f"lt_{lt_key}", use_container_width=True):
                st.session_state["list_type"] = lt_key
                st.rerun()

# Coming soon row
st.markdown("<div style='margin-top:1rem'></div>", unsafe_allow_html=True)
soon_html = "".join(
    f'<div class="soon-chip {color}">{icon} {name}</div>'
    for icon, name, color in COMING_SOON
)
st.markdown(f"""
<div style="display:flex;align-items:center;gap:0.5rem;flex-wrap:wrap">
  <span style="font-size:0.7rem;font-weight:700;letter-spacing:0.15em;text-transform:uppercase;
    color:rgba(242,239,230,0.25)">Coming Soon</span>
  {soon_html}
</div>""", unsafe_allow_html=True)

st.markdown("""
<div class="rules-box" style="margin-top:1.5rem">
  <p>⬇️ Select a list type above, then download the full CSV — no limits</p>
  <p>🔄 Lists are refreshed regularly from public government &amp; listing data</p>
  <p>🔍 Skip trace owners to get phone/email contact info</p>
</div>""", unsafe_allow_html=True)

st.markdown("<div style='height:0.5rem'></div>", unsafe_allow_html=True)


# ── FSBO ───────────────────────────────────────────────────────────────────────
if st.session_state["list_type"] == "fsbo":
    last = database.get_last_scraped(state)
    if last:
        try:
            last_dt   = datetime.fromisoformat(last.replace("Z", "+00:00"))
            hours_ago = (datetime.now(timezone.utc) - last_dt).total_seconds() / 3600
            freshness = "Updated less than 1 hour ago" if hours_ago < 1 else \
                        f"Updated {int(hours_ago)} hours ago" if hours_ago < 24 else \
                        f"Updated {int(hours_ago/24)} days ago"
        except Exception:
            freshness = "Recently updated"
        st.markdown(f"<p style='font-size:0.82rem;color:rgba(242,239,230,0.3);margin-bottom:0.5rem'>🔄 {freshness}</p>",
                    unsafe_allow_html=True)

    st.markdown("""
    <div style="background:rgba(201,168,76,0.06);border:1px solid rgba(201,168,76,0.2);
      border-left:4px solid #C9A84C;border-radius:12px;padding:1.4rem 1.6rem;margin-bottom:1.5rem">
      <div style="font-size:1rem;font-weight:800;color:#C9A84C;margin-bottom:0.8rem">🏠 For Sale By Owner Leads</div>
      <p style="font-size:0.9rem;color:rgba(242,239,230,0.7);margin:0.3rem 0">
        Active FSBO listings scraped from Craigslist and fsbo.com — owners selling
        without an agent, often open to creative offers and below-market deals.
      </p>
      <div style="margin-top:1rem;display:grid;grid-template-columns:1fr 1fr;gap:0.6rem">
        <div style="background:rgba(201,168,76,0.08);border-radius:8px;padding:0.7rem 1rem">
          <div style="font-size:0.78rem;font-weight:700;color:#C9A84C;margin-bottom:0.2rem">HOW TO USE</div>
          <div style="font-size:0.82rem;color:rgba(242,239,230,0.6)">
            1. Download the CSV<br>
            2. Call the phone number directly<br>
            3. Ask if they'd consider a cash offer<br>
            4. Move fast — FSBOs list with agents quickly
          </div>
        </div>
        <div style="background:rgba(201,168,76,0.08);border-radius:8px;padding:0.7rem 1rem">
          <div style="font-size:0.78rem;font-weight:700;color:#C9A84C;margin-bottom:0.2rem">WHAT TO LOOK FOR</div>
          <div style="font-size:0.82rem;color:rgba(242,239,230,0.6)">
            • Listings with a phone number included<br>
            • Price reductions or "motivated" language<br>
            • Days on market — older = more motivated<br>
            • fsbo.com leads have owner name included
          </div>
        </div>
      </div>
    </div>""", unsafe_allow_html=True)

    if not fsbo_count:
        st.markdown('<div class="no-results">No active FSBO listings for this state.<br>'
                    '<span style="font-size:0.85rem">Check back soon — we scrape daily.</span></div>',
                    unsafe_allow_html=True)
    else:
        st.markdown(
            f"<p style='color:rgba(242,239,230,0.5);font-size:0.95rem;margin:0.5rem 0 0.3rem'>"
            f"<strong style='color:#C9A84C;font-size:1.4rem'>{fsbo_count:,}</strong>"
            f"  active listings — {state}</p>"
            f"<p style='font-size:0.78rem;color:rgba(242,239,230,0.25);margin:0 0 1rem'>"
            f"On Mac: right-click → Open With → Excel or Google Sheets to avoid Numbers.</p>",
            unsafe_allow_html=True
        )
        _download_buttons(
            fsbo_count,
            f"fsbo_leads_{state}_{datetime.now().strftime('%Y%m%d')}",
            lambda off, lim: database.get_fsbo_leads_for_download(state, limit=lim, offset=off),
        )

    st.markdown("<p style='text-align:center;font-size:0.82rem;color:rgba(242,239,230,0.18);margin-top:2rem'>"
                "Don't see your state? More markets coming soon.</p>", unsafe_allow_html=True)


# ── Tax Delinquent ─────────────────────────────────────────────────────────────
elif st.session_state["list_type"] == "td":
    st.markdown("""
    <div style="background:rgba(220,60,60,0.06);border:1px solid rgba(220,60,60,0.2);
      border-left:4px solid #e05555;border-radius:12px;padding:1.4rem 1.6rem;margin-bottom:1.5rem">
      <div style="font-size:1rem;font-weight:800;color:#e05555;margin-bottom:0.8rem">
        📋 What is a Tax Delinquent List?
      </div>
      <p style="font-size:0.9rem;color:rgba(242,239,230,0.7);margin:0.3rem 0">
        These are property owners who owe <strong style="color:#F2EFE6">unpaid property taxes</strong> —
        but have <strong style="color:#F2EFE6">NOT yet been foreclosed on or gone to tax sale</strong>.
        They still own the property and can sell it.
      </p>
      <p style="font-size:0.9rem;color:rgba(242,239,230,0.7);margin:0.3rem 0">
        Owners behind on taxes are often cash-strapped, dealing with life changes, or simply overwhelmed.
        A fast cash offer that clears their debt can be extremely attractive — making these
        some of the most motivated sellers you'll ever call.
      </p>
      <div style="margin-top:1rem;display:grid;grid-template-columns:1fr 1fr;gap:0.6rem">
        <div style="background:rgba(220,60,60,0.08);border-radius:8px;padding:0.7rem 1rem">
          <div style="font-size:0.78rem;font-weight:700;color:#e05555;margin-bottom:0.2rem">HOW TO USE THIS LIST</div>
          <div style="font-size:0.82rem;color:rgba(242,239,230,0.6)">
            1. Download the CSV<br>
            2. Skip trace the owner name + address<br>
            3. Call and lead with paying off their debt<br>
            4. Make an offer on the equity
          </div>
        </div>
        <div style="background:rgba(220,60,60,0.08);border-radius:8px;padding:0.7rem 1rem">
          <div style="font-size:0.78rem;font-weight:700;color:#e05555;margin-bottom:0.2rem">WHAT TO LOOK FOR</div>
          <div style="font-size:0.82rem;color:rgba(242,239,230,0.6)">
            • Higher amount owed = more distress<br>
            • Multiple years delinquent = very motivated<br>
            • LLCs/corps already filtered out<br>
            • Individual owners only
          </div>
        </div>
      </div>
    </div>""", unsafe_allow_html=True)

    td_counties = database.get_td_counties(state)
    if not td_counties:
        st.markdown('<div class="no-results">No tax delinquent records yet for this state.<br>'
                    '<span style="font-size:0.85rem">Currently available: Florida, Ohio, Tennessee, Nevada, Texas.</span></div>',
                    unsafe_allow_html=True)
    else:
        td_county  = st.selectbox("Select County", td_counties, key="td_county_select")
        td_count_n = database.get_td_county_count(state, td_county)
        st.markdown(
            f"<p style='color:rgba(242,239,230,0.5);font-size:0.95rem;margin:0.5rem 0 0.3rem'>"
            f"<strong style='color:#e05555;font-size:1.4rem'>{td_count_n:,}</strong>"
            f"  individual owner records — {td_county} County, {state}</p>"
            f"<p style='font-size:0.78rem;color:rgba(242,239,230,0.25);margin:0 0 1rem'>"
            f"On Mac: right-click → Open With → Excel or Google Sheets to avoid Numbers.</p>",
            unsafe_allow_html=True
        )
        if td_count_n:
            _download_buttons(
                td_count_n,
                f"tax_delinquent_{state}_{td_county}_{datetime.now().strftime('%Y%m%d')}",
                lambda off, lim: database.get_td_leads_for_download(state, td_county, limit=lim, offset=off),
            )


# ── Absentee Owner ─────────────────────────────────────────────────────────────
elif st.session_state["list_type"] == "ao":
    st.markdown("""
    <div style="background:rgba(96,165,250,0.06);border:1px solid rgba(96,165,250,0.2);
      border-left:4px solid #60A5FA;border-radius:12px;padding:1.4rem 1.6rem;margin-bottom:1.5rem">
      <div style="font-size:1rem;font-weight:800;color:#60A5FA;margin-bottom:0.8rem">
        🏢 What is an Absentee Owner List?
      </div>
      <p style="font-size:0.9rem;color:rgba(242,239,230,0.7);margin:0.3rem 0">
        These are property owners whose <strong style="color:#F2EFE6">mailing address is different from the property address</strong> —
        meaning they don't live there. They're landlords or out-of-state investors managing a property remotely.
      </p>
      <p style="font-size:0.9rem;color:rgba(242,239,230,0.7);margin:0.3rem 0">
        Absentee owners are often tired of dealing with tenants, maintenance, and management from afar.
        <strong style="color:#F2EFE6">Out-of-state absentees are especially motivated</strong> — a fast,
        no-hassle cash offer lets them be done with a problem they can't even drive to fix.
      </p>
      <div style="margin-top:1rem;display:grid;grid-template-columns:1fr 1fr;gap:0.6rem">
        <div style="background:rgba(96,165,250,0.08);border-radius:8px;padding:0.7rem 1rem">
          <div style="font-size:0.78rem;font-weight:700;color:#60A5FA;margin-bottom:0.2rem">HOW TO USE THIS LIST</div>
          <div style="font-size:0.82rem;color:rgba(242,239,230,0.6)">
            1. Download the CSV<br>
            2. Skip trace the owner using their mailing address<br>
            3. Call and lead with convenience &amp; speed<br>
            4. Make a cash offer
          </div>
        </div>
        <div style="background:rgba(96,165,250,0.08);border-radius:8px;padding:0.7rem 1rem">
          <div style="font-size:0.78rem;font-weight:700;color:#60A5FA;margin-bottom:0.2rem">WHAT TO LOOK FOR</div>
          <div style="font-size:0.82rem;color:rgba(242,239,230,0.6)">
            • Out-of-state owners = highest motivation<br>
            • Cross-ref with tax delinquent list<br>
            • LLCs/corps already filtered out<br>
            • Individual owners only
          </div>
        </div>
      </div>
    </div>""", unsafe_allow_html=True)

    ao_counties = database.get_ao_counties(state)
    if not ao_counties:
        st.markdown('<div class="no-results">No absentee owner records yet for this state.<br>'
                    '<span style="font-size:0.85rem">Currently available: Arizona, Texas, Florida, Ohio, Tennessee, Georgia, Indiana, Alabama.</span></div>',
                    unsafe_allow_html=True)
    else:
        ao_county  = st.selectbox("Select County", ao_counties, key="ao_county_select")
        ao_count_n = database.get_ao_county_count(state, ao_county)
        st.markdown(
            f"<p style='color:rgba(242,239,230,0.5);font-size:0.95rem;margin:0.5rem 0 0.3rem'>"
            f"<strong style='color:#60A5FA;font-size:1.4rem'>{ao_count_n:,}</strong>"
            f"  absentee owner records — {ao_county} County, {state}</p>"
            f"<p style='font-size:0.78rem;color:rgba(242,239,230,0.25);margin:0 0 1rem'>"
            f"On Mac: right-click → Open With → Excel or Google Sheets to avoid Numbers.</p>",
            unsafe_allow_html=True
        )
        if ao_count_n:
            _download_buttons(
                ao_count_n,
                f"absentee_owners_{state}_{ao_county}_{datetime.now().strftime('%Y%m%d')}",
                lambda off, lim: database.get_ao_leads_for_download(state, ao_county, limit=lim, offset=off),
            )


# ── Code Violations ────────────────────────────────────────────────────────────
elif st.session_state["list_type"] == "cv":
    st.markdown("""
    <div style="background:rgba(20,184,166,0.06);border:1px solid rgba(20,184,166,0.2);
      border-left:4px solid #14B8A6;border-radius:12px;padding:1.4rem 1.6rem;margin-bottom:1.5rem">
      <div style="font-size:1rem;font-weight:800;color:#14B8A6;margin-bottom:0.8rem">
        🚨 What is a Code Violations List?
      </div>
      <p style="font-size:0.9rem;color:rgba(242,239,230,0.7);margin:0.3rem 0">
        These are properties flagged by the city for open code enforcement cases — broken windows,
        overgrown lots, structural damage, unpermitted work, or habitability issues.
      </p>
      <p style="font-size:0.9rem;color:rgba(242,239,230,0.7);margin:0.3rem 0">
        Owners sitting on unresolved violation notices are often <strong style="color:#F2EFE6">overwhelmed,
        over-leveraged, or simply done</strong> with the property — a perfect target for a cash offer.
      </p>
      <div style="margin-top:1rem;display:grid;grid-template-columns:1fr 1fr;gap:0.6rem">
        <div style="background:rgba(20,184,166,0.08);border-radius:8px;padding:0.7rem 1rem">
          <div style="font-size:0.78rem;font-weight:700;color:#14B8A6;margin-bottom:0.2rem">HOW TO USE THIS LIST</div>
          <div style="font-size:0.82rem;color:rgba(242,239,230,0.6)">
            1. Download the CSV<br>
            2. Look up the owner at the county assessor<br>
            3. Skip trace to get phone/email<br>
            4. Call and make an offer
          </div>
        </div>
        <div style="background:rgba(20,184,166,0.08);border-radius:8px;padding:0.7rem 1rem">
          <div style="font-size:0.78rem;font-weight:700;color:#14B8A6;margin-bottom:0.2rem">WHAT TO LOOK FOR</div>
          <div style="font-size:0.82rem;color:rgba(242,239,230,0.6)">
            • Multiple violations = more motivated<br>
            • Old filed dates = ignored for years<br>
            • Residential properties only<br>
            • Cross-reference with tax delinquent list
          </div>
        </div>
      </div>
    </div>""", unsafe_allow_html=True)

    cv_cities = database.get_cv_cities(state)
    if not cv_cities:
        st.markdown('<div class="no-results">No code violation records yet for this state.<br>'
                    '<span style="font-size:0.85rem">Currently available: Ohio, Tennessee, Florida, Arizona, Texas, Georgia, Nevada, California, Indiana, Missouri.</span></div>',
                    unsafe_allow_html=True)
    else:
        cv_city    = st.selectbox("Select City", cv_cities, key="cv_city_select")
        cv_count_n = database.get_cv_city_count(state, cv_city)
        st.markdown(
            f"<p style='color:rgba(242,239,230,0.5);font-size:0.95rem;margin:0.5rem 0 0.3rem'>"
            f"<strong style='color:#14B8A6;font-size:1.4rem'>{cv_count_n:,}</strong>"
            f"  active violation records — {cv_city}, {state}</p>"
            f"<p style='font-size:0.78rem;color:rgba(242,239,230,0.25);margin:0 0 1rem'>"
            f"On Mac: right-click → Open With → Excel or Google Sheets to avoid Numbers.</p>",
            unsafe_allow_html=True
        )
        if cv_count_n:
            _download_buttons(
                cv_count_n,
                f"code_violations_{state}_{cv_city.replace(' ','_')}_{datetime.now().strftime('%Y%m%d')}",
                lambda off, lim: database.get_cv_leads_for_download(state, cv_city, limit=lim, offset=off),
            )


# ── Footer ──────────────────────────────────────────────────────────────────────
st.markdown("""
<div style="text-align:center;padding:3rem 0 1rem;border-top:1px solid rgba(201,168,76,0.08);margin-top:3rem;">
  <div style="font-size:0.75rem;color:rgba(242,239,230,0.13);letter-spacing:0.1em;text-transform:uppercase;">
    © 2026 Equivest Academy LLC — Data sourced from public listings
  </div>
</div>
""", unsafe_allow_html=True)
