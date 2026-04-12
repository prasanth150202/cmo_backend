-- Ecommerce orders pushed via Shopify / WooCommerce webhooks.
-- Revenue flows in here in real time without any polling.
-- UTM params are extracted from order metadata so you can join
-- orders back to Meta/Google campaigns.

create table if not exists ecommerce_orders (
  order_id     text primary key,            -- 'shopify_<id>' | 'wc_<id>'
  source       text not null,              -- 'shopify' | 'woocommerce'
  date         date not null,              -- order date (created_at truncated to day)
  brand_id     uuid references brands(id) on delete set null,
  status       text not null default '',   -- paid | pending | refunded | completed | cancelled
  total        numeric(12,2) not null default 0,
  currency     text not null default 'USD',
  utm_source   text not null default '',   -- e.g. 'facebook', 'google'
  utm_campaign text not null default '',   -- campaign name or id
  utm_medium   text not null default '',   -- e.g. 'cpc', 'email'
  customer_id  text not null default '',
  raw_payload  jsonb,
  created_at   timestamptz not null default now()
);

create index if not exists idx_ecommerce_orders_date         on ecommerce_orders (date desc);
create index if not exists idx_ecommerce_orders_source       on ecommerce_orders (source);
create index if not exists idx_ecommerce_orders_utm_campaign on ecommerce_orders (utm_campaign);
create index if not exists idx_ecommerce_orders_brand        on ecommerce_orders (brand_id);
create index if not exists idx_ecommerce_orders_status       on ecommerce_orders (status);
