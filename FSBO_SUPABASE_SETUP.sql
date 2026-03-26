-- FSBO Lead Finder — Supabase Setup
-- Run this in your Supabase SQL editor

-- ── FSBO Leads table ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fsbo_leads (
  id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  state        text NOT NULL,
  city         text NOT NULL,
  title        text NOT NULL,
  price        text,
  address      text,
  phone        text,
  description  text,
  url          text UNIQUE NOT NULL,
  posted_at    timestamptz,
  scraped_at   timestamptz DEFAULT now(),
  expires_at   timestamptz
);

CREATE INDEX IF NOT EXISTS idx_fsbo_leads_state      ON fsbo_leads(state);
CREATE INDEX IF NOT EXISTS idx_fsbo_leads_expires_at ON fsbo_leads(expires_at);
CREATE INDEX IF NOT EXISTS idx_fsbo_leads_scraped_at ON fsbo_leads(scraped_at);

-- ── FSBO Claims table ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fsbo_claims (
  id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  lead_id          uuid NOT NULL REFERENCES fsbo_leads(id) ON DELETE CASCADE,
  user_id          uuid NOT NULL,
  user_email       text NOT NULL,
  claimed_at       timestamptz DEFAULT now(),
  claim_expires_at timestamptz NOT NULL,
  UNIQUE(lead_id, user_id)
);

CREATE INDEX IF NOT EXISTS idx_fsbo_claims_user_id          ON fsbo_claims(user_id);
CREATE INDEX IF NOT EXISTS idx_fsbo_claims_lead_id          ON fsbo_claims(lead_id);
CREATE INDEX IF NOT EXISTS idx_fsbo_claims_claim_expires_at ON fsbo_claims(claim_expires_at);

-- ── Row Level Security ────────────────────────────────────────────────────────
ALTER TABLE fsbo_leads  ENABLE ROW LEVEL SECURITY;
ALTER TABLE fsbo_claims ENABLE ROW LEVEL SECURITY;

-- Service role bypasses RLS (used by scraper + admin queries)
-- Anon/authenticated users: read leads, manage own claims

CREATE POLICY "Anyone can read active leads"
  ON fsbo_leads FOR SELECT
  USING (expires_at > now());

CREATE POLICY "Service role can upsert leads"
  ON fsbo_leads FOR ALL
  USING (true);

CREATE POLICY "Users can read all claims"
  ON fsbo_claims FOR SELECT
  USING (true);

CREATE POLICY "Users can insert own claims"
  ON fsbo_claims FOR INSERT
  WITH CHECK (auth.uid() = user_id);

CREATE POLICY "Users can update own claims"
  ON fsbo_claims FOR UPDATE
  USING (auth.uid() = user_id);

CREATE POLICY "Service role can manage all claims"
  ON fsbo_claims FOR ALL
  USING (true);
