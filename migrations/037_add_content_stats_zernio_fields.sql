-- 037_add_content_stats_zernio_fields.sql
--
-- Per-post fields Zernio returns that Outstand never did. All nullable — every other
-- writer (the Apify competitor path, the admin's bulk imports) simply omits them, and
-- upsert_self_content_stats skips None on update, so nothing existing is disturbed.
--
--   follows              PostAnalytics.follows — organic accounts that started following
--                        FROM this post. Instagram FEED posts and STORIES only; Meta
--                        returns 0 for reels and for every other platform, so a 0 here
--                        is not the same as "nobody followed" on a reel.
--   avg_watch_time_ms    PostAnalytics.igReelsAvgWatchTime — average watch time per PLAY,
--                        in milliseconds. Instagram Reels only.
--   total_watch_time_ms  PostAnalytics.igReelsVideoViewTotalTime — total watch time
--                        INCLUDING replays, in milliseconds. Instagram Reels only.
--
-- Pair avg_watch_time_ms with the existing duration_sec (fed from Zernio's
-- videoDurationSeconds) to estimate retention. duration_sec is null when Instagram
-- withholds the media URL — reels with copyrighted audio, most commonly — so a null
-- there is expected, not a fetch failure.
--
-- Milliseconds, not seconds, deliberately: that is the unit Meta reports and rounding
-- to seconds at write time would throw away precision we cannot recover.
--
-- GOTCHA: ingestion/storage.py's _content_stats_column_set() discovers the live column
-- list ONCE per process and caches it. Apply this migration BEFORE (or with) the deploy
-- that restarts the service, or these three keys are silently filtered out of every
-- write until the next restart.
--
-- Idempotent; applied manually like every other migration here.

alter table content_stats
  add column if not exists follows              bigint,
  add column if not exists avg_watch_time_ms    bigint,
  add column if not exists total_watch_time_ms  bigint;

comment on column content_stats.follows is
  'Accounts that followed from this post. Zernio/Meta: Instagram feed posts and stories only — always 0 for reels and non-Instagram platforms.';
comment on column content_stats.avg_watch_time_ms is
  'Average watch time per play, milliseconds. Instagram Reels only (igReelsAvgWatchTime).';
comment on column content_stats.total_watch_time_ms is
  'Total watch time including replays, milliseconds. Instagram Reels only (igReelsVideoViewTotalTime).';
