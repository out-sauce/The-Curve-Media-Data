-- 043_widen_content_stats_platform_spotify.sql
--
-- Allow platform = 'spotify' on content_stats.
--
-- Podcast episodes are landing in content_stats for the first time: the Curve Auth
-- Chrome extension pulls per-episode analytics from the operator's logged-in Spotify
-- for Creators session and POSTs them to /podcast/import (ingestion/podcast.py).
-- Episodes carry a different metric vocabulary from social posts (plays/streams/
-- listeners/retention — no likes, comments or shares), so they get their own platform
-- value rather than being folded into an existing one, exactly as instagram_story
-- (migration 038) and youtube_shorts already do. Folding them in would inject podcast
-- rows into every query that filters on a social platform value and silently change
-- what those aggregates mean.
--
-- WITHOUT THIS MIGRATION every episode insert is rejected outright with a 23514 check
-- violation. That is a safe failure (nothing lands in the wrong bucket) but a total one
-- — the podcast import simply does not work until this is applied.
--
-- No new columns: content_stats already has podcast_episode_id (uuid, Admin-created)
-- and the episode metrics map onto existing columns (views=plays, reach=listeners,
-- platform_specific for the Spotify-only vocabulary). So the per-process
-- _content_stats_column_set() cache imposes no deploy-ordering constraint here — only
-- the CHECK needs to land before the first button press.
--
-- ⚠️ content_stats is shared with the Admin app (Curve_Admin_NextJS) — mirror this
-- change there if that repo ever re-creates the constraint.
--
-- Idempotent; applied manually like every other migration here.

alter table content_stats drop constraint if exists content_stats_platform_check;

alter table content_stats add constraint content_stats_platform_check
  check (
    platform is null
    or platform in (
      'instagram', 'tiktok', 'linkedin', 'youtube', 'youtube_shorts',
      'instagram_story', 'spotify'
    )
  );
