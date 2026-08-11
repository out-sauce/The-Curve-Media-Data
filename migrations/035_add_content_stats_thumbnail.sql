-- Post thumbnail for The Curve's own content_stats rows — a stable public URL
-- in the competitor-thumbnails Storage bucket (posts/{platform}_{post_id}.jpg),
-- the same persisted object competitor_posts.thumbnail_url points at, so Admin
-- has an image to show for every post. For video posts this may be a video
-- frame / first media item rather than a curated cover — "something to show"
-- is the bar. Written by ingestion/outstand.py (self Instagram) and
-- ingestion/competitors.py (self TikTok/LinkedIn/YouTube; card-window posts
-- only). Nullable; the skip-None update in upsert_self_content_stats never
-- blanks a previously stored value.
-- Idempotent — safe to re-run.

alter table content_stats add column if not exists thumbnail_url text;

comment on column content_stats.thumbnail_url is 'Public URL in the competitor-thumbnails bucket (posts/{platform}_{post_id}.jpg) — persisted copy of the post''s media image; same object competitor_posts.thumbnail_url references. May be a video frame for video posts.';
