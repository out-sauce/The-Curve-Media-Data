-- Per-publisher-domain login session for the research-stage browser scraper.
-- Mirrors the live schema; written by the portal/admin app, read by this data app.
-- Idempotent — safe to re-run.
--
-- Auth is keyed by registrable domain (e.g. 'ft.com'), not by feed source: a single
-- subscriber session unlocks every article on that publisher. The portal captures a
-- logged-in browser session and exports a full Playwright storageState() JSON
-- (context.storage_state()) — cookies WITH domain/path/expires/httpOnly/secure/
-- sameSite plus per-origin localStorage — which carries the login, CSRF/consent state
-- and metering tokens paywalls actually check, giving the best paywall survival.
--
-- Legacy sources.cookies (raw Cookie header, migration 019) is NOT consulted by the
-- browser path — auth starts fresh from this table.

CREATE TABLE IF NOT EXISTS site_auth (
  domain        text PRIMARY KEY,    -- registrable domain, e.g. 'ft.com'
  storage_state jsonb NOT NULL,      -- Playwright storage_state:
                                     --   {cookies:[{name,value,domain,path,expires,httpOnly,secure,sameSite}],
                                     --    origins:[{origin, localStorage:[{name,value}]}]}
  label         text,                -- human note, e.g. 'FT subscriber session'
  captured_at   timestamptz,         -- when the portal exported this session (for the 7/30-day staleness reminder)
  last_status   text,                -- last scrape outcome on this domain: 'scraped'|'paywalled'|'failed' — written by this app
  last_used_at  timestamptz,         -- when this app last used this session — written by this app
  updated_at    timestamptz DEFAULT now()
);

COMMENT ON TABLE site_auth IS
  'Per-publisher-domain login session for the research-stage browser scraper.
   storage_state is a full Playwright storageState() JSON, written by the portal
   (capture from a logged-in browser → context.storage_state()). Keyed by registrable
   domain. The portal writes domain/storage_state/label/captured_at; the data app
   writes last_status/last_used_at so the portal can flag stale (7/30-day) sessions.';

-- Auditability: which engine produced the row's text. Free text (no CHECK) so it can
-- carry 'browser' | 'static' without touching the scrape_status CHECK (migration 018).
ALTER TABLE news_articles
  ADD COLUMN IF NOT EXISTS scrape_method text;

COMMENT ON COLUMN news_articles.scrape_method IS
  'Which research-stage engine scraped this article: ''browser'' (Playwright/Chromium)
   or ''static'' (httpx). NULL for rows not scraped by the research stage.';
