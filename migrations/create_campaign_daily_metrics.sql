-- Run this in Supabase SQL Editor

CREATE TABLE IF NOT EXISTS campaign_daily_metrics (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  date          DATE NOT NULL,
  campaign_id   TEXT NOT NULL,
  campaign_name TEXT,
  account_id    TEXT NOT NULL,
  spend         NUMERIC DEFAULT 0,
  revenue       NUMERIC DEFAULT 0,
  roas          NUMERIC DEFAULT 0,
  conversions   NUMERIC DEFAULT 0,
  impressions   BIGINT  DEFAULT 0,
  clicks        BIGINT  DEFAULT 0,
  ctr           NUMERIC DEFAULT 0,
  atc           NUMERIC DEFAULT 0,
  atc_value     NUMERIC DEFAULT 0,
  checkout      NUMERIC DEFAULT 0,
  synced_at     TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(date, campaign_id)
);

CREATE INDEX IF NOT EXISTS idx_cdm_date       ON campaign_daily_metrics(date);
CREATE INDEX IF NOT EXISTS idx_cdm_account    ON campaign_daily_metrics(account_id);
CREATE INDEX IF NOT EXISTS idx_cdm_campaign   ON campaign_daily_metrics(campaign_id);
