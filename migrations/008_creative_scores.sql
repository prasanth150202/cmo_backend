-- Creative performance + AI scores
-- Stores analyzed results per ad per brand per date range.
-- Valid for 6 hours; stale entries are recomputed on next load.

create table if not exists creative_scores (
  id                uuid          primary key default gen_random_uuid(),
  ad_id             text          not null,
  brand_id          text          not null,
  date_from         date          not null,
  date_to           date          not null,
  performance_score numeric(5,2)  not null default 0,
  ai_score          numeric(5,2)  not null default 0,
  score_gap         numeric(5,2)  not null default 0,
  category          text          not null default 'AVERAGE', -- GOOD | AVERAGE | BAD
  spend             numeric(12,2) not null default 0,
  roas              numeric(8,4)  not null default 0,
  ctr               numeric(6,2)  not null default 0,
  cpm               numeric(8,2)  not null default 0,
  hook_rate         numeric(8,4)  not null default 0,
  conversions       numeric(10,1) not null default 0,
  metric_scores     jsonb         not null default '{}',
  ai_reasoning      text          not null default '',
  analyzed_at       timestamptz   not null default now(),
  constraint creative_scores_unique unique (ad_id, brand_id, date_from, date_to)
);

create index if not exists idx_creative_scores_brand    on creative_scores (brand_id, analyzed_at);
create index if not exists idx_creative_scores_ad       on creative_scores (ad_id);
create index if not exists idx_creative_scores_category on creative_scores (brand_id, category);
