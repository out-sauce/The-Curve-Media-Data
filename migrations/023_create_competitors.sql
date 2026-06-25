-- Competitor registry. The Curve App writes this list; the competitor run
-- reads enabled rows at runtime to scrape follower counts + recent posts.
-- Mirrors the `sources` shape so the app's source-management UI patterns transfer.
-- Idempotent — safe to re-run.

create table if not exists competitors (
  id          bigserial   primary key,
  name        text        not null unique,
  handle      text,         -- scrape target (account username without the @)
  url         text,         -- display profile URL
  source_type text        not null check (source_type in ('instagram', 'tiktok')),
  enabled     boolean     not null default true,
  created_at  timestamptz not null default now()
);

create index if not exists idx_competitors_type    on competitors (source_type);
create index if not exists idx_competitors_enabled on competitors (enabled);
