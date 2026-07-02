-- 027: allow linkedin / youtube / youtube_shorts as platform values on
-- competitor_posts and content_stats.
--
-- The Python competitor run (ingestion/competitors.py) writes these platform values
-- for The Curve's LinkedIn + YouTube channels, but the original CHECK constraints
-- (shipped by the Admin app) only permitted 'instagram'/'tiktok', so the inserts failed
-- with `competitor_posts_platform_check`. Existing rows are only instagram/tiktok, so
-- widening the allowed set is safe. Idempotent — drops the constraint if present, then
-- re-adds it. NULL is permitted (platform is a nullable text column). ⚠️ The Admin app
-- owns this schema — mirror this change there so the two definitions do not drift.

alter table competitor_posts drop constraint if exists competitor_posts_platform_check;
alter table competitor_posts
  add constraint competitor_posts_platform_check
  check (platform is null or platform in
    ('instagram', 'tiktok', 'linkedin', 'youtube', 'youtube_shorts'));

alter table content_stats drop constraint if exists content_stats_platform_check;
alter table content_stats
  add constraint content_stats_platform_check
  check (platform is null or platform in
    ('instagram', 'tiktok', 'linkedin', 'youtube', 'youtube_shorts'));
