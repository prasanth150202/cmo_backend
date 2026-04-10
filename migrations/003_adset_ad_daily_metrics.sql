-- Ad set daily metrics
-- Stores per-adset, per-day performance data pulled from Meta Insights API.
-- Unique constraint: one row per (date, adset_id).

create table if not exists adset_daily_metrics (
  id           uuid primary key default gen_random_uuid(),
  date         date        not null,
  adset_id     text        not null,
  adset_name   text        not null default '',
  campaign_id  text        not null,
  account_id   text        not null,
  spend        numeric(12,2) not null default 0,
  revenue      numeric(12,2) not null default 0,
  roas         numeric(8,4)  not null default 0,
  conversions  numeric(10,1) not null default 0,
  impressions  bigint        not null default 0,
  clicks       bigint        not null default 0,
  ctr          numeric(6,2)  not null default 0,
  atc          numeric(10,1) not null default 0,
  atc_value    numeric(12,2) not null default 0,
  checkout     numeric(10,1) not null default 0,
  synced_at    timestamptz not null default now(),
  constraint adset_daily_metrics_date_adset_id_key unique (date, adset_id)
);

create index if not exists idx_adset_daily_campaign on adset_daily_metrics (campaign_id, date);
create index if not exists idx_adset_daily_account  on adset_daily_metrics (account_id, date);


-- Ad daily metrics
-- Stores per-ad, per-day performance data pulled from Meta Insights API.
-- Unique constraint: one row per (date, ad_id).

create table if not exists ad_daily_metrics (
  id           uuid primary key default gen_random_uuid(),
  date         date        not null,
  ad_id        text        not null,
  ad_name      text        not null default '',
  adset_id     text        not null,
  campaign_id  text        not null,
  account_id   text        not null,
  spend        numeric(12,2) not null default 0,
  revenue      numeric(12,2) not null default 0,
  roas         numeric(8,4)  not null default 0,
  conversions  numeric(10,1) not null default 0,
  impressions  bigint        not null default 0,
  clicks       bigint        not null default 0,
  ctr          numeric(6,2)  not null default 0,
  atc          numeric(10,1) not null default 0,
  atc_value    numeric(12,2) not null default 0,
  checkout     numeric(10,1) not null default 0,
  synced_at    timestamptz not null default now(),
  constraint ad_daily_metrics_date_ad_id_key unique (date, ad_id)
);

create index if not exists idx_ad_daily_adset    on ad_daily_metrics (adset_id, date);
create index if not exists idx_ad_daily_campaign on ad_daily_metrics (campaign_id, date);
create index if not exists idx_ad_daily_account  on ad_daily_metrics (account_id, date);
