-- 031_add_story_continuation.sql
--
-- Story continuation: the daily clustering stage now APPENDS today's articles to an
-- existing story_clusters row instead of minting a new same-named row per day and tying
-- the copies together with the weekly_story slug. A running story is one row that
-- accumulates coverage across days.
--
-- Idempotent. Applied manually — nothing auto-applies migrations at runtime.

-- ---------------------------------------------------------------------------
-- last_article_at: stamped every time a cluster gains articles
-- ---------------------------------------------------------------------------

ALTER TABLE story_clusters
  ADD COLUMN IF NOT EXISTS last_article_at timestamptz;

-- Backfill: every pre-existing cluster last gained articles on the day it was created.
UPDATE story_clusters
   SET last_article_at = created_at
 WHERE last_article_at IS NULL;

-- Backstop for any writer that forgets it (e.g. the Admin app inserting a cluster).
ALTER TABLE story_clusters
  ALTER COLUMN last_article_at SET DEFAULT now();

COMMENT ON COLUMN story_clusters.last_article_at IS
  'Stamped by the clustering stage every time the cluster gains articles — both on
   creation and when an existing story is extended with a later day''s coverage. The
   briefing stage regenerates a brief when last_article_at > briefed_at, so an extended
   story is re-briefed while an unchanged story keeps run_briefing''s idempotent skip.';

COMMENT ON COLUMN story_clusters.date IS
  'The most recent pipeline run date on which this story gained coverage. NOT immutable:
   when the clustering stage appends new articles to a running story the date is bumped
   forward to that run date, so the story resurfaces in the Admin day view and in every
   downstream stage (score/tag/research/brief/daily_brief all filter date = run_date).
   created_at holds the day the story first appeared; the two differ for running stories.
   A cluster''s articles are therefore NOT all from its date — always list a story''s
   articles by news_articles.cluster_id, never by a fetched_at window.';

COMMENT ON COLUMN story_clusters.weekly_story IS
  'DEPRECATED (migration 031). Was the lowercase slug tying together the one-row-per-day
   copies of a running story before real continuation existed. Still written as a
   lowercase copy of name for back-compat; do NOT group stories by it any more — a
   running story is now a single row, so grouping degenerates to groups of one.';

-- ---------------------------------------------------------------------------
-- Candidate lookup for the continuation pass:
--   date BETWEEN <window start> AND <run date>
--   AND cluster_status NOT IN ('briefed','published','archived')
--   AND published_at IS NULL
--   AND (article_count >= 2  |  article_count = 1 over a shorter window)
-- ---------------------------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_story_clusters_open_candidates
  ON story_clusters (date DESC, cluster_status, article_count);
