-- Social sources (Instagram / TikTok) for the scan stage.
-- Mirrors supabase/add_social_sources.sql already applied via the admin repo;
-- kept here so this repo's migration history reflects the live schema.
-- Idempotent — safe to re-run.

-- handle: the scrape target (account username without the @, e.g. 'curve.media').
-- Null for feed sources. For social sources, `url` holds the display profile URL.
ALTER TABLE sources
  ADD COLUMN IF NOT EXISTS handle text;

-- source_type is free text (no enum) and now also takes 'instagram' / 'tiktok'.
-- No DDL needed for that; documented here for completeness.
