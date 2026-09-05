-- 044_namespace_podcast_episode_demographics.sql
--
-- Per-episode demographics get a `spotify_` prefix, because they are about to stop being
-- the only source. Spotify, YouTube and the RSS host each report their own audience, and
-- `age_28_34_pct` with no source in the name can only ever hold one of them — the second
-- source to arrive would either overwrite the first or need a differently-shaped column.
--
-- This matches what podcast_episodes ALREADY does everywhere else: plays_spotify,
-- plays_youtube, plays_apple, spotify_completion_pct, youtube_completion_pct. The
-- demographic columns are the exception, not the rule.
--
-- ⚠️ ADD AND COPY, NEVER RENAME. podcast_episodes belongs to the Admin app
-- (Curve_Admin_NextJS) and its UI reads the unprefixed columns. Renaming them here would
-- break that app on deploy, from a repo that cannot see it. So:
--   1. (this migration) add spotify_* columns, copy existing values across
--   2. the pipeline writes BOTH during the transition
--   3. Admin switches its reads to spotify_*
--   4. a LATER migration drops the unprefixed columns — only after step 3 has shipped
-- Do not skip to step 4.
--
-- ASSUMPTION, worth confirming: the 378 rows of existing age/gender and 415 of geo were
-- hand-entered from Spotify's own dashboard, which is why they are copied into the
-- spotify_* columns. If any of them came from Flightcast or another host, they are RSS
-- figures wearing a Spotify label. Nothing here overwrites them, so a correction is a
-- plain UPDATE if that turns out to be wrong.
--
-- Idempotent; applied manually like every other migration here.

alter table podcast_episodes
  add column if not exists spotify_age_18_22_pct numeric,
  add column if not exists spotify_age_23_27_pct numeric,
  add column if not exists spotify_age_28_34_pct numeric,
  add column if not exists spotify_age_35_44_pct numeric,
  add column if not exists spotify_age_45_59_pct numeric,
  add column if not exists spotify_age_60plus_pct numeric,
  add column if not exists spotify_gender_female_pct numeric,
  add column if not exists spotify_gender_male_pct numeric,
  add column if not exists spotify_gender_non_binary_pct numeric,
  add column if not exists spotify_gender_not_specified_pct numeric,
  add column if not exists spotify_geo_plays_nz bigint,
  add column if not exists spotify_geo_plays_au bigint,
  add column if not exists spotify_geo_plays_gb bigint,
  add column if not exists spotify_geo_plays_us bigint,
  add column if not exists spotify_geo_plays_row bigint;

-- Copy the hand-entered history across. Fill-only-if-null so re-running cannot clobber
-- anything the pipeline has since written.
update podcast_episodes set
  spotify_age_18_22_pct            = coalesce(spotify_age_18_22_pct, age_18_22_pct),
  spotify_age_23_27_pct            = coalesce(spotify_age_23_27_pct, age_23_27_pct),
  spotify_age_28_34_pct            = coalesce(spotify_age_28_34_pct, age_28_34_pct),
  spotify_age_35_44_pct            = coalesce(spotify_age_35_44_pct, age_35_44_pct),
  spotify_age_45_59_pct            = coalesce(spotify_age_45_59_pct, age_45_59_pct),
  spotify_age_60plus_pct           = coalesce(spotify_age_60plus_pct, age_60plus_pct),
  spotify_gender_female_pct        = coalesce(spotify_gender_female_pct, gender_female_pct),
  spotify_gender_male_pct          = coalesce(spotify_gender_male_pct, gender_male_pct),
  spotify_gender_non_binary_pct    = coalesce(spotify_gender_non_binary_pct, gender_non_binary_pct),
  spotify_gender_not_specified_pct = coalesce(spotify_gender_not_specified_pct, gender_not_specified_pct),
  spotify_geo_plays_nz             = coalesce(spotify_geo_plays_nz, geo_plays_nz),
  spotify_geo_plays_au             = coalesce(spotify_geo_plays_au, geo_plays_au),
  spotify_geo_plays_gb             = coalesce(spotify_geo_plays_gb, geo_plays_gb),
  spotify_geo_plays_us             = coalesce(spotify_geo_plays_us, geo_plays_us),
  spotify_geo_plays_row            = coalesce(spotify_geo_plays_row, geo_plays_row);
