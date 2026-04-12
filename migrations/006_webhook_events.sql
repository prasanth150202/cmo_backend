-- Audit log for all incoming webhook events.
-- Every webhook payload is written here before processing so you can
-- replay events, debug integrations, or inspect raw data.

create table if not exists webhook_events (
  id          bigint generated always as identity primary key,
  source      text not null,                -- 'meta' | 'shopify' | 'woocommerce'
  topic       text not null,                -- e.g. 'adaccount', 'orders/paid', 'order.created'
  account_id  text,                         -- ad account id when applicable (Meta)
  payload     jsonb,                        -- raw webhook body
  received_at timestamptz not null default now()
);

create index if not exists idx_webhook_events_source     on webhook_events (source);
create index if not exists idx_webhook_events_account_id on webhook_events (account_id);
create index if not exists idx_webhook_events_received   on webhook_events (received_at desc);
