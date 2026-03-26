-- Tax Delinquent Lead Finder — Supabase Setup
-- Run this in your Supabase SQL editor

-- ── Tax Delinquent Leads table ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS tax_delinquent_leads (
  id             uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  state          text NOT NULL,
  county         text NOT NULL,
  owner_name     text,
  property_address text,
  parcel_id      text,
  assessed_value text,
  amount_owed    text,
  tax_year       text,
  url            text,
  source_url     text,
  scraped_at     timestamptz DEFAULT now(),
  expires_at     timestamptz,
  UNIQUE(state, county, parcel_id)
);

CREATE INDEX IF NOT EXISTS idx_td_leads_state     ON tax_delinquent_leads(state);
CREATE INDEX IF NOT EXISTS idx_td_leads_county    ON tax_delinquent_leads(county);
CREATE INDEX IF NOT EXISTS idx_td_leads_scraped   ON tax_delinquent_leads(scraped_at);
CREATE INDEX IF NOT EXISTS idx_td_leads_expires   ON tax_delinquent_leads(expires_at);

-- ── Tax Delinquent Claims table ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS tax_delinquent_claims (
  id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  lead_id          uuid NOT NULL REFERENCES tax_delinquent_leads(id) ON DELETE CASCADE,
  user_id          uuid NOT NULL,
  user_email       text NOT NULL,
  claimed_at       timestamptz DEFAULT now(),
  claim_expires_at timestamptz NOT NULL,
  UNIQUE(lead_id, user_id)
);

CREATE INDEX IF NOT EXISTS idx_td_claims_user_id ON tax_delinquent_claims(user_id);
CREATE INDEX IF NOT EXISTS idx_td_claims_lead_id ON tax_delinquent_claims(lead_id);

-- ── Row Level Security ────────────────────────────────────────────────────────
ALTER TABLE tax_delinquent_leads   ENABLE ROW LEVEL SECURITY;
ALTER TABLE tax_delinquent_claims  ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Anyone can read active td leads"
  ON tax_delinquent_leads FOR SELECT
  USING (expires_at > now() OR expires_at IS NULL);

CREATE POLICY "Service role can upsert td leads"
  ON tax_delinquent_leads FOR ALL
  USING (true);

CREATE POLICY "Users can read all td claims"
  ON tax_delinquent_claims FOR SELECT
  USING (true);

CREATE POLICY "Users can insert own td claims"
  ON tax_delinquent_claims FOR INSERT
  WITH CHECK (auth.uid() = user_id);

CREATE POLICY "Users can update own td claims"
  ON tax_delinquent_claims FOR UPDATE
  USING (auth.uid() = user_id);

CREATE POLICY "Service role can manage all td claims"
  ON tax_delinquent_claims FOR ALL
  USING (true);
