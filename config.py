# FSBO Scraper — Config
import streamlit as st

def _s(key):
    return st.secrets[key]

SUPABASE_URL         = _s("SUPABASE_URL")
SUPABASE_ANON_KEY    = _s("SUPABASE_ANON_KEY")
SUPABASE_SERVICE_KEY = _s("SUPABASE_SERVICE_KEY")

# Markets: state → list of (display_city, craigslist_subdomain)
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

CLAIM_HOURS   = 48     # how long a claim locks a lead
LISTING_DAYS  = 30     # how long before a listing expires
MAX_PER_CITY  = 40     # max listings to scrape per city
