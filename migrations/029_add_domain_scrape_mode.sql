-- Per-registrable-domain research scrape policy (auto|manual) + widen scrape_status.
-- Idempotent; applied manually (nothing auto-applies migrations at runtime).
--
-- WHY: the daily research batch renders logged-in article pages from a datacenter IP.
-- Sites behind PerimeterX/DataDome (e.g. bloomberg.com) serve an anti-bot wall, and
-- retrying them ties bot-flagged automated traffic to a paid subscriber login, risking
-- account suspension. This lets each publisher domain be 'auto' (batch may try an
-- automated logged-in scrape) or 'manual' (batch skips it; scraped only on manual
-- initiation). A domain auto-demotes to 'manual' the first time an automated scrape
-- returns 'bot_wall' or 'paywalled', so a hostile publisher is hit with the login at
-- most once.

-- 1. Allow the new 'bot_wall' scrape outcome — an anti-bot / CAPTCHA wall, distinct from
--    a subscription paywall. Without this the CHECK from migration 018 rejects the write.
ALTER TABLE news_articles DROP CONSTRAINT IF EXISTS news_articles_scrape_status_check;
ALTER TABLE news_articles ADD CONSTRAINT news_articles_scrape_status_check
  CHECK (scrape_status IN ('scraped', 'failed', 'paywalled', 'bot_wall'));

-- 2. Per-domain scrape policy. Keyed by registrable domain — the same key as
--    site_auth.domain and sources.site_auth_domain — so every feed of one publisher
--    shares a single policy row and demotes together.
CREATE TABLE IF NOT EXISTS domain_scrape_settings (
  domain      text PRIMARY KEY,
  scrape_mode text NOT NULL DEFAULT 'auto' CHECK (scrape_mode IN ('auto', 'manual')),
  last_reason text,                                   -- why it last changed, e.g. 'bot_wall'
  updated_at  timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE domain_scrape_settings IS
  'Per-registrable-domain research scrape policy (auto|manual), keyed like
   site_auth.domain / sources.site_auth_domain. The daily batch reads this; an automated
   scrape returning bot_wall/paywalled demotes the domain to manual so the subscriber
   login is not sent to a hostile publisher more than once. Toggled on the Admin Sources
   page.';

-- RLS enabled with no policies, matching sources/site_auth: the service-role pipeline and
-- Admin app bypass RLS; anon/authenticated are denied.
ALTER TABLE domain_scrape_settings ENABLE ROW LEVEL SECURITY;

-- Seed the domain already confirmed to serve a px-captcha bot wall, so the login is never
-- sent to it even once. Everything else starts 'auto' and self-demotes on first wall.
INSERT INTO domain_scrape_settings (domain, scrape_mode, last_reason)
VALUES ('bloomberg.com', 'manual', 'seed: confirmed px-captcha bot wall')
ON CONFLICT (domain) DO NOTHING;
