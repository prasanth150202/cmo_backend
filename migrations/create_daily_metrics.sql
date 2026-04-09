-- Run this in Supabase SQL Editor

-- Daily metrics cache (one row per date per account)
CREATE TABLE IF NOT EXISTS daily_metrics (
  id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  date         DATE NOT NULL,
  account_id   TEXT NOT NULL,
  spend        NUMERIC DEFAULT 0,
  revenue      NUMERIC DEFAULT 0,
  roas         NUMERIC DEFAULT 0,
  conversions  NUMERIC DEFAULT 0,
  impressions  BIGINT DEFAULT 0,
  clicks       BIGINT DEFAULT 0,
  ctr          NUMERIC DEFAULT 0,
  atc          NUMERIC DEFAULT 0,
  atc_value    NUMERIC DEFAULT 0,
  checkout     NUMERIC DEFAULT 0,
  synced_at    TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(date, account_id)
);

-- If table already exists, add the new columns:
ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS atc       NUMERIC DEFAULT 0;
ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS atc_value NUMERIC DEFAULT 0;
ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS checkout  NUMERIC DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_daily_metrics_date    ON daily_metrics(date);
CREATE INDEX IF NOT EXISTS idx_daily_metrics_account ON daily_metrics(account_id);

-- Sync job tracker (one row per account per sync request)
CREATE TABLE IF NOT EXISTS sync_jobs (
  id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  account_id   TEXT NOT NULL,
  status       TEXT DEFAULT 'pending',   -- pending | running | completed | failed
  date_from    DATE NOT NULL,
  date_to      DATE NOT NULL,
  rows_synced  INT DEFAULT 0,
  total_chunks INT DEFAULT 0,
  done_chunks  INT DEFAULT 0,
  error        TEXT,
  created_at   TIMESTAMPTZ DEFAULT NOW(),
  updated_at   TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sync_jobs_account ON sync_jobs(account_id);
CREATE INDEX IF NOT EXISTS idx_sync_jobs_status  ON sync_jobs(status);

-- Note: Run this SQL in Supabase SQL Editor
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

CREATE INDEX IF NOT EXISTS idx_campaign_daily_metrics_date ON campaign_daily_metrics(date);
CREATE INDEX IF NOT EXISTS idx_campaign_daily_metrics_account ON campaign_daily_metrics(account_id);

CREATE TABLE IF NOT EXISTS campaigns (
  id            TEXT PRIMARY KEY,
  account_id    TEXT NOT NULL,
  name          TEXT NOT NULL,
  status        TEXT DEFAULT 'ACTIVE',
  updated_at    TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_campaigns_account ON campaigns(account_id);
