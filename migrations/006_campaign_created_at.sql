-- Add created_at to campaigns so we can sort by when Meta created the campaign.
-- Uses Meta's created_time value (ISO string). ON CONFLICT DO NOTHING so
-- existing rows keep their value once set.
ALTER TABLE campaigns
  ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ;
