-- 045_add_rss_download_stats.sql
--
-- Per-episode RSS download stats from OP3 (op3.dev), the open-source prefix analytics
-- service already wrapped around every enclosure in the Flightcast feed.
--
-- WHAT THIS IS AND IS NOT. OP3 is a download prefix: it sees a request for the audio
-- file and nothing after it. So there is no completion, no retention, no listen time and
-- — by design, OP3 is privacy-preserving — no age or gender. What it does give, which
-- nothing else does per episode, is country, app and device.
--
-- ⚠️ THESE ARE NOT LIFETIME FIGURES FOR OLDER EPISODES. OP3 only counts downloads made
-- after the prefix was added to the feed. An episode published before that shows only
-- its back-catalogue trickle since, which understates its real total badly and
-- plausibly. `rss_partial` marks exactly those rows. NEVER sum rss_downloads_all into a
-- lifetime total — Flightcast is the source of truth for plays (it reports every
-- platform on one basis for the show's whole history); OP3 is for the dimensions.
--
-- ⚠️ SPOTIFY AND YOUTUBE PLAYS DO NOT APPEAR HERE. Both ingest the feed once to their
-- own CDN and serve listeners from that copy, so they never touch the prefix — verified
-- live 2026-09-07: zero Spotify entries across 32 apps and 77,911 downloads, of which
-- Apple Podcasts is 87.5%. That is a feature: the three sources measure disjoint
-- audiences and add rather than overlap.
--
-- JOIN KEY. OP3's aggregate endpoint returns `itemGuid` matching podcast_episodes.guid
-- ("flightcast:01KY…") exactly. Its RAW download rows identify the episode by audio URL
-- instead, and the file's ULID is NOT the guid's ULID — they share a timestamp prefix
-- and differ in the random suffix on 422 of 424 episodes. The RSS feed is the only
-- mapping between the two, so the geo/app breakdown requires parsing it.
--
-- Idempotent; applied manually like every other migration here.

alter table podcast_episodes
  add column if not exists rss_downloads_all bigint,
  add column if not exists rss_downloads_7d bigint,
  add column if not exists rss_geo_downloads_nz bigint,
  add column if not exists rss_geo_downloads_au bigint,
  add column if not exists rss_geo_downloads_gb bigint,
  add column if not exists rss_geo_downloads_us bigint,
  add column if not exists rss_geo_downloads_row bigint,
  add column if not exists rss_apple_pct numeric,
  add column if not exists rss_partial boolean,
  add column if not exists rss_updated_at timestamptz;

comment on column podcast_episodes.rss_downloads_all is
  'OP3 downloads since the prefix was added — NOT lifetime when rss_partial is true.';
comment on column podcast_episodes.rss_partial is
  'True when the episode predates OP3 tracking, so its RSS figures cover only the '
  'back-catalogue trickle since the prefix went on, not its launch.';
comment on column podcast_episodes.rss_apple_pct is
  'Share of this episode''s OP3 downloads from Apple Podcasts (0-100).';
