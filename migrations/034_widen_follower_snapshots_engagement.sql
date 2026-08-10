-- Account-level engagement aggregate from Outstand's Insights-backed metrics
-- (GET /v1/social-accounts/{id}/metrics `engagement` block), previously fetched
-- hourly and thrown away. Rides along on the existing one-row-per-UTC-day
-- follower_snapshots upsert, so the table becomes the daily account-metrics
-- series, not just followers.
--
-- IMPORTANT SEMANTICS: these are TRAILING ~30-DAY ROLLING WINDOW totals (the
-- payload's period.since/until spans ~30 days), NOT that day's activity.
-- Adjacent daily rows overlap by ~29 days — charting the trend is fine, but
-- never SUM rows or diff two days to derive a daily figure. The _30d suffix
-- exists to keep that from being forgotten.
--
-- Nullable: pre-existing rows and non-Outstand channels (Apify-scraped TikTok/
-- LinkedIn/YouTube snapshots) simply leave them NULL.
-- Idempotent — safe to re-run.

alter table follower_snapshots add column if not exists views_30d              bigint;
alter table follower_snapshots add column if not exists likes_30d              bigint;
alter table follower_snapshots add column if not exists comments_30d           bigint;
alter table follower_snapshots add column if not exists shares_30d             bigint;
alter table follower_snapshots add column if not exists saves_30d              bigint;
alter table follower_snapshots add column if not exists reach_30d              bigint;
alter table follower_snapshots add column if not exists accounts_engaged_30d   bigint;
alter table follower_snapshots add column if not exists total_interactions_30d bigint;

comment on column follower_snapshots.views_30d              is 'Trailing ~30-day rolling total (Outstand account metrics). Adjacent daily rows overlap — never SUM or diff.';
comment on column follower_snapshots.likes_30d              is 'Trailing ~30-day rolling total (Outstand account metrics). Adjacent daily rows overlap — never SUM or diff.';
comment on column follower_snapshots.comments_30d           is 'Trailing ~30-day rolling total (Outstand account metrics). Adjacent daily rows overlap — never SUM or diff.';
comment on column follower_snapshots.shares_30d             is 'Trailing ~30-day rolling total (Outstand account metrics). Adjacent daily rows overlap — never SUM or diff.';
comment on column follower_snapshots.saves_30d              is 'Trailing ~30-day rolling total (Outstand account metrics). Adjacent daily rows overlap — never SUM or diff.';
comment on column follower_snapshots.reach_30d              is 'Trailing ~30-day rolling total (Outstand account metrics). Adjacent daily rows overlap — never SUM or diff.';
comment on column follower_snapshots.accounts_engaged_30d   is 'Trailing ~30-day rolling total (Outstand account metrics). Adjacent daily rows overlap — never SUM or diff.';
comment on column follower_snapshots.total_interactions_30d is 'Trailing ~30-day rolling total (Outstand account metrics). Adjacent daily rows overlap — never SUM or diff.';
