-- LinkedIn/YouTube competitor stat columns; transcript on competitor_posts and
-- content_stats. All idempotent — safe to re-run.

-- LinkedIn stat columns on competitors
alter table competitors
  add column if not exists linkedin_handle          text,
  add column if not exists linkedin_url             text,
  add column if not exists linkedin_avatar_url      text,
  add column if not exists linkedin_follower_count  bigint,
  add column if not exists linkedin_engagement_rate numeric,
  add column if not exists linkedin_post_count      bigint;

-- YouTube stat columns on competitors (shared by youtube + youtube_shorts channels)
alter table competitors
  add column if not exists youtube_handle           text,
  add column if not exists youtube_url              text,
  add column if not exists youtube_avatar_url       text,
  add column if not exists youtube_follower_count   bigint,
  add column if not exists youtube_engagement_rate  numeric,
  add column if not exists youtube_post_count       bigint;

-- Transcript on competitor_posts (null for non-video posts / failed fetches)
alter table competitor_posts
  add column if not exists transcript text;

-- Transcript on content_stats (The Curve's own is_self posts)
alter table content_stats
  add column if not exists transcript text;
