-- Outstand (outstand.so) is a unified social API used to pull real Instagram Insights
-- (shares/saves/reach/accounts_engaged/total_interactions) for The Curve's own account,
-- which Apify's public scrape structurally cannot provide (see
-- 025_content_stats_enrichment.sql). social_accounts already has generic OAuth-connection
-- columns (account_id, client_id, access_token, refresh_token, token_expires_at, connected)
-- from the existing Xero integration — the Admin app's Outstand connect flow reuses
-- account_id/connected the same way, so no new identity column is needed here.
--
-- outstand_last_imported_at is the one genuinely new concept: the incremental-import
-- watermark. Imports are billed per post and NOT deduped by Outstand on overlapping
-- ranges, so this must always advance forward, never be reset, to avoid re-billing or
-- duplicating posts on every hourly run.
-- Idempotent — safe to re-run. ⚠️ The Admin app owns social_accounts — mirror this change
-- there so the two definitions do not drift.

alter table social_accounts add column if not exists outstand_last_imported_at timestamptz;
