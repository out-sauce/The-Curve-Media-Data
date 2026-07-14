-- Research queue for the Chrome-extension content-grab lane.
-- Idempotent; applied manually (nothing auto-applies migrations at runtime).
--
-- WHY: domains that bot-wall automated reads are marked scrape_mode='manual' (migration
-- 029) and skipped by the daily batch. They are researched instead by fetching the
-- article inside a human's real, logged-in browser (the Curve Auth Chrome extension),
-- which no bot detector can flag. The Admin "Research" button enqueues an article here;
-- the extension polls, opens the page, and posts the rendered HTML back to the pipeline.

CREATE TABLE IF NOT EXISTS research_queue (
  id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  article_id   bigint NOT NULL REFERENCES news_articles(id) ON DELETE CASCADE,
  url          text NOT NULL,
  status       text NOT NULL DEFAULT 'pending'
                 CHECK (status IN ('pending', 'claimed', 'done', 'failed')),
  error        text,
  requested_at timestamptz NOT NULL DEFAULT now(),
  claimed_at   timestamptz,
  completed_at timestamptz
);

-- At most one outstanding (pending/claimed) request per article — clicking Research
-- twice, or re-enqueuing while a grab is in flight, is a no-op.
CREATE UNIQUE INDEX IF NOT EXISTS research_queue_one_outstanding
  ON research_queue (article_id) WHERE status IN ('pending', 'claimed');

-- Poll path: pending items oldest-first.
CREATE INDEX IF NOT EXISTS research_queue_status_idx
  ON research_queue (status, requested_at);

COMMENT ON TABLE research_queue IS
  'Queue for the Chrome-extension content-grab lane. Admin enqueues an article (status
   pending); the extension claims it (claimed), opens it in a logged-in browser, posts the
   HTML back, and the pipeline marks it done/failed. Used for scrape_mode=manual domains.';

-- RLS enabled with no policies, matching sources/site_auth/domain_scrape_settings: the
-- service-role pipeline and Admin app bypass RLS; anon/authenticated are denied.
ALTER TABLE research_queue ENABLE ROW LEVEL SECURITY;
