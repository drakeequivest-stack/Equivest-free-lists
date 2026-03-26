-- Equivest Academy — Additional List Types Setup
-- Run in Supabase SQL Editor

-- ── Code Violations / Tired Landlord ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS code_violation_leads (
  id             uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  state          text NOT NULL,
  city           text NOT NULL,
  address        text NOT NULL,
  parcel_id      text,
  violation_type text,
  violation_sub  text,
  case_status    text,
  filed_date     text,
  last_insp_date text,
  source_url     text,
  scraped_at     timestamptz DEFAULT now(),
  expires_at     timestamptz,
  UNIQUE(state, city, address, violation_type)
);

CREATE INDEX IF NOT EXISTS idx_cv_state   ON code_violation_leads(state);
CREATE INDEX IF NOT EXISTS idx_cv_city    ON code_violation_leads(city);
CREATE INDEX IF NOT EXISTS idx_cv_scraped ON code_violation_leads(scraped_at);

ALTER TABLE code_violation_leads ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Anyone can read cv leads"
  ON code_violation_leads FOR SELECT USING (true);

CREATE POLICY "Service role can upsert cv leads"
  ON code_violation_leads FOR ALL USING (true);


-- ── Absentee Owner ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS absentee_owner_leads (
  id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  state            text NOT NULL,
  county           text NOT NULL,
  owner_name       text,
  owner_address    text,
  property_address text,
  parcel_id        text NOT NULL,
  amount_owed      text,
  source_url       text,
  scraped_at       timestamptz DEFAULT now(),
  expires_at       timestamptz,
  UNIQUE(state, county, parcel_id)
);

CREATE INDEX IF NOT EXISTS idx_ao_state   ON absentee_owner_leads(state);
CREATE INDEX IF NOT EXISTS idx_ao_county  ON absentee_owner_leads(county);
CREATE INDEX IF NOT EXISTS idx_ao_scraped ON absentee_owner_leads(scraped_at);

ALTER TABLE absentee_owner_leads ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Anyone can read ao leads"
  ON absentee_owner_leads FOR SELECT USING (true);

CREATE POLICY "Service role can upsert ao leads"
  ON absentee_owner_leads FOR ALL USING (true);


-- ── Pre-Foreclosure (placeholder — paid data required) ────────────────────────
CREATE TABLE IF NOT EXISTS preforeclosure_leads (
  id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  state            text NOT NULL,
  county           text NOT NULL,
  owner_name       text,
  property_address text,
  parcel_id        text,
  filing_date      text,
  lender           text,
  loan_amount      text,
  case_number      text,
  source_url       text,
  scraped_at       timestamptz DEFAULT now(),
  expires_at       timestamptz,
  UNIQUE(state, county, case_number)
);

ALTER TABLE preforeclosure_leads ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Anyone can read pf leads"  ON preforeclosure_leads FOR SELECT USING (true);
CREATE POLICY "Service role can upsert pf" ON preforeclosure_leads FOR ALL USING (true);


-- ── Divorce (placeholder — paid data required) ────────────────────────────────
CREATE TABLE IF NOT EXISTS divorce_leads (
  id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  state            text NOT NULL,
  county           text NOT NULL,
  petitioner_name  text,
  respondent_name  text,
  property_address text,
  parcel_id        text,
  filing_date      text,
  case_number      text,
  source_url       text,
  scraped_at       timestamptz DEFAULT now(),
  expires_at       timestamptz,
  UNIQUE(state, county, case_number)
);

ALTER TABLE divorce_leads ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Anyone can read dv leads"  ON divorce_leads FOR SELECT USING (true);
CREATE POLICY "Service role can upsert dv" ON divorce_leads FOR ALL USING (true);


-- ── Free & Clear (placeholder — paid data required) ──────────────────────────
CREATE TABLE IF NOT EXISTS free_clear_leads (
  id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  state            text NOT NULL,
  county           text NOT NULL,
  owner_name       text,
  property_address text,
  parcel_id        text NOT NULL,
  assessed_value   text,
  year_acquired    text,
  source_url       text,
  scraped_at       timestamptz DEFAULT now(),
  expires_at       timestamptz,
  UNIQUE(state, county, parcel_id)
);

ALTER TABLE free_clear_leads ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Anyone can read fc leads"  ON free_clear_leads FOR SELECT USING (true);
CREATE POLICY "Service role can upsert fc" ON free_clear_leads FOR ALL USING (true);
