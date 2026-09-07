-- 046_widen_demographic_dimensions.sql
--
-- Allow dimension = 'app' and 'device' on audience_demographics.
--
-- OP3 (ingestion/podcast_rss.py) reports which podcast app and device type each RSS
-- download came from — Apple Podcasts 87.5%, then CastBox, Pocket Casts, Overcast, and
-- mobile/computer/smart_speaker. Neither Spotify nor Flightcast exposes that per show,
-- and it is a real audience dimension: it says how listeners reach the show, which
-- drives where a change to the feed or a new format actually lands.
--
-- WITHOUT THIS every app/device row is rejected with a 23514 and the country rows still
-- write, so the failure is partial and quiet — exactly what it did on the first live run.
--
-- The existing dimensions are kept verbatim. 'age_gender' is unused by this pipeline but
-- belongs to the Admin app; do not tidy it away from here.
--
-- ⚠️ audience_demographics is shared with the Admin app (Curve_Admin_NextJS) — mirror
-- this change there if that repo ever re-creates the constraint.
--
-- Idempotent; applied manually like every other migration here.

alter table audience_demographics drop constraint if exists audience_demographics_dimension_check;

alter table audience_demographics add constraint audience_demographics_dimension_check
  check (dimension in ('country', 'city', 'age', 'gender', 'age_gender', 'app', 'device'));
