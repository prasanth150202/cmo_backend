-- Add creative detail columns to ad_daily_metrics
-- Creative info (title, body, type, thumbnail) is static per ad but stored
-- alongside each daily row for simplicity — no separate join needed.

alter table ad_daily_metrics
  add column if not exists ad_title       text    not null default '',
  add column if not exists ad_body        text    not null default '',
  add column if not exists creative_type  text    not null default '',
  add column if not exists thumbnail_url  text    not null default '',
  add column if not exists image_url      text    not null default '',
  add column if not exists call_to_action text    not null default '',
  add column if not exists destination_url text   not null default '';
