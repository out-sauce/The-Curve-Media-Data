-- Rich per-post fields returned by Outstand's Insights-backed analytics
-- (GET /v1/posts/{id}/analytics, GET /v1/social-accounts/{id}/metrics) that content_stats
-- has no column for yet. impressions/accounts_engaged/total_interactions are common across
-- the platforms Outstand covers; platform_specific is a catch-all jsonb for whatever nested,
-- network-specific data it returns (varies by platform and changes as Outstand's API
-- evolves) so the writer doesn't silently drop it the way unknown keys were dropped before
-- 025/026 turned on their respective columns.
-- Idempotent — safe to re-run. Mirrors the Admin app's equivalent content_stats migration.

alter table content_stats add column if not exists impressions       bigint;
alter table content_stats add column if not exists accounts_engaged  bigint;
alter table content_stats add column if not exists total_interactions bigint;
alter table content_stats add column if not exists platform_specific jsonb;
