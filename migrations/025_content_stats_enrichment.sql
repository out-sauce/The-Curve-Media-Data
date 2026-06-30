-- Enrichment columns for content_stats, populated by the is_self competitor scrape
-- (ingestion/competitors.py -> _run_self -> storage.upsert_content_stats).
-- The scrape fills these only after this migration runs; until then the writer
-- drops the unknown keys, so applying this is what "turns the columns on".
--
-- Availability note: views/likes/comments/shares/saves + caption/hashtags/
-- duration_sec come from the public Apify scrape. engagement_rate here is
-- interactions/views (a proxy) — TRUE reach / unique-views and average watch time
-- are owner-only analytics (Instagram Graph API / TikTok Business API) and are NOT
-- available from the public scrape, so the reach/opens/clicks/downloads columns
-- stay null until such an integration is added.
-- Idempotent — safe to re-run.

alter table content_stats add column if not exists caption         text;
alter table content_stats add column if not exists hashtags        text[];
alter table content_stats add column if not exists duration_sec    integer;
alter table content_stats add column if not exists engagement_rate numeric;
