-- 028: allow linkedin / youtube (+ youtube_shorts) as platform values on
-- social_accounts.
--
-- Follow-up to 027, which widened competitor_posts + content_stats but MISSED
-- social_accounts. The Curve's own LinkedIn + YouTube channels scrape fine and land
-- in competitor_posts, but the is_self daily follower-snapshot path
-- (ingestion/competitors.py `_snapshot_self_follower` → storage.get_self_social_accounts)
-- can only write a snapshot when a matching social_accounts row exists. Inserting a
-- linkedin/youtube social_accounts row was rejected by `social_accounts_platform_check`
-- (allowed instagram/tiktok/... but not linkedin/youtube), so those two channels never
-- got daily follower_snapshots — only a manual monthly backfill.
--
-- Widen the allowed set (superset of every value currently present, so the re-add is
-- safe against existing rows). Idempotent — drops the constraint if present, then
-- re-adds it. NULL is permitted. ⚠️ The Admin app owns this schema — mirror this change
-- there so the two definitions do not drift.

alter table social_accounts drop constraint if exists social_accounts_platform_check;
alter table social_accounts
  add constraint social_accounts_platform_check
  check (platform is null or platform in
    ('instagram', 'tiktok', 'linkedin', 'youtube', 'youtube_shorts',
     'substack', 'spotify', 'apple_music', 'xero', 'email'));

-- Seed The Curve's own LinkedIn + YouTube channel rows so the daily competitor run
-- has a social_accounts target to attach follower_snapshots to. follower_count is
-- seeded from the latest known values; the pipeline refreshes it each run. Skip if a
-- row for the platform already exists.
insert into social_accounts (platform, account_name, category, connected, follower_count)
select 'linkedin', '', 'social', false, 1810
where not exists (select 1 from social_accounts where platform = 'linkedin');

insert into social_accounts (platform, account_name, category, connected, follower_count)
select 'youtube', '', 'social', false, 1780
where not exists (select 1 from social_accounts where platform = 'youtube');
