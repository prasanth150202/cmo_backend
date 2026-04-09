-- Run this in your Supabase SQL Editor:
-- https://supabase.com/dashboard/project/jpwptucnudeltkhyvxwr/sql/new

ALTER TABLE performance_metrics
  ADD COLUMN IF NOT EXISTS account_id TEXT;

-- Index for fast brand-level queries
CREATE INDEX IF NOT EXISTS idx_pm_account_id ON performance_metrics(account_id);
