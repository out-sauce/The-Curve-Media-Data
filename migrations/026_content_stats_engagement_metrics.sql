-- content_stats: post publish time + explicit engagement ratios, populated by the
-- is_self competitor scrape (ingestion/competitors.py -> _run_channel ->
-- storage.upsert_self_content_stats).
--
--   posted_at           — the platform publish time (the date the admin dashboard
--                         and Socials table order/plot by).
--   engagement_reach    — interactions / views, as a fraction (supersedes the
--                         earlier engagement_rate proxy from migration 025).
--   engagement_audience — interactions / followers-at-time, as a fraction.
--
-- "interactions" = likes + comments + shares + saves (whatever the platform
-- exposes). The writer drops unknown keys, so applying this is what "turns the
-- columns on". Idempotent — safe to re-run. Mirrors the admin app's
-- supabase/add_content_stats_posted_at_engagement.sql.

alter table content_stats add column if not exists posted_at           timestamptz;
alter table content_stats add column if not exists engagement_reach    numeric;
alter table content_stats add column if not exists engagement_audience numeric;

create index if not exists idx_content_stats_posted_at on content_stats (posted_at desc);
