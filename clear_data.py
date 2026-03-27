"""
clear_data.py — wipe AO, TD, and CV tables before monthly re-scrape.
FSBO leads are left alone (they expire naturally via expires_at).

Run manually:  python3 clear_data.py
"""
import os
import sys
from supabase import create_client

TABLES = [
    "absentee_owner_leads",
    "tax_delinquent_leads",
    "code_violation_leads",
]

def main():
    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_SERVICE_KEY", "")
    if not url or not key:
        print("ERROR: Set SUPABASE_URL and SUPABASE_SERVICE_KEY.")
        sys.exit(1)

    db = create_client(url, key)
    for table in TABLES:
        try:
            # Delete all rows by filtering id > 0 (works for both int and uuid)
            db.table(table).delete().neq("id", "00000000-0000-0000-0000-000000000000").execute()
            print(f"[clear] {table} — cleared")
        except Exception:
            # Fallback: filter on a column that every row has
            try:
                db.table(table).delete().gte("id", "0").execute()
                print(f"[clear] {table} — cleared (fallback)")
            except Exception as e:
                print(f"[clear] {table} — ERROR: {e}")

if __name__ == "__main__":
    main()
