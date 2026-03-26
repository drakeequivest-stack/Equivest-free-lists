#!/bin/bash
# ─── Equivest Academy — Daily Lead Scraper ────────────────────────────────────
# Runs all scrapers once per day. Add to cron:
#   crontab -e
#   0 3 * * * /bin/bash /Users/drakegibson/Desktop/FSBO\ Scraper/run_daily.sh >> /Users/drakegibson/Desktop/FSBO\ Scraper/logs/daily.log 2>&1

# ── Config ────────────────────────────────────────────────────────────────────
DIR="/Users/drakegibson/Desktop/FSBO Scraper"
LOG_DIR="$DIR/logs"
export SUPABASE_URL="https://ynqrcefaokyysrzqkqdm.supabase.co"
export SUPABASE_SERVICE_KEY="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InlucXJjZWZhb2t5eXNyenFrcWRtIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3Mzg5OTgyOSwiZXhwIjoyMDg5NDc1ODI5fQ.PpGpykzNLNfcWAIJokN5Wa2BMF_8X0j7GaoePGKGmDw"

mkdir -p "$LOG_DIR"
echo ""
echo "============================================="
echo "  Equivest Daily Scraper — $(date '+%Y-%m-%d %H:%M')"
echo "============================================="

cd "$DIR"

# ── 1. FSBO (Craigslist + fsbo.com) ──────────────────────────────────────────
echo ""
echo "[1/6] FSBO Scraper..."
python3 scraper.py
echo "FSBO done."

# ── 2. Tax Delinquent ─────────────────────────────────────────────────────────
echo ""
echo "[2/6] Tax Delinquent Scraper..."
python3 tax_scraper.py
echo "Tax Delinquent done."

# ── 3. Absentee Owner ─────────────────────────────────────────────────────────
echo ""
echo "[3/6] Absentee Owner Scraper..."
python3 absentee_scraper.py
echo "Absentee Owner done."

# ── 4. Code Violations / Tired Landlord ──────────────────────────────────────
echo ""
echo "[4/6] Code Violations Scraper..."
python3 codevio_scraper.py
echo "Code Violations done."

# ── 5. Pre-Foreclosure (requires paid data — skipped) ────────────────────────
# echo "[5/6] Pre-Foreclosure Scraper..."
# python3 preforeclosure_scraper.py

# ── 6. Divorce (requires paid data — skipped) ────────────────────────────────
# echo "[6/6] Divorce Scraper..."
# python3 divorce_scraper.py

echo ""
echo "============================================="
echo "  All scrapers complete — $(date '+%Y-%m-%d %H:%M')"
echo "============================================="
