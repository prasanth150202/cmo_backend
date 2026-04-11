-- Add ad effective_status column to ad_daily_metrics.
-- Populated from Meta effective_status at sync time; reflects current delivery state.

alter table ad_daily_metrics
  add column if not exists ad_status text not null default 'UNKNOWN';

create index if not exists idx_ad_daily_status on ad_daily_metrics (ad_status);
