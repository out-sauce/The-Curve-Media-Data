-- Competitor metrics captured by the competitor run.
--   * competitor_snapshots — append-only follower time series (one row per run),
--     so the Curve App can chart follower growth over time.
--   * competitor_posts      — the ≤10 most recent posts within the lookback window,
--     keyed by guid '{platform}:{post_id}' and re-upserted each run so engagement
--     counts (likes/comments/views) refresh as posts mature.
-- Idempotent — safe to re-run.

create table if not exists competitor_snapshots (
  id             bigserial   primary key,
  competitor_id  bigint      references competitors(id),
  handle         text,
  platform       text,
  follower_count bigint,
  following_count bigint,
  post_count     bigint,
  captured_at    timestamptz not null default now()
);

create index if not exists idx_competitor_snapshots_competitor
  on competitor_snapshots (competitor_id, captured_at);

create table if not exists competitor_posts (
  id            bigserial   primary key,
  competitor_id bigint      references competitors(id),
  guid          text        unique,   -- '{platform}:{post_id}'
  platform      text,
  post_url      text,
  caption       text,
  like_count    bigint,
  comment_count bigint,
  view_count    bigint,
  published_at  timestamptz,
  fetched_at    timestamptz
);

create index if not exists idx_competitor_posts_competitor on competitor_posts (competitor_id);
create index if not exists idx_competitor_posts_published  on competitor_posts (published_at);
