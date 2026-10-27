-- 049_cluster_status_published.sql
--
-- Applied 26 Oct 2026 through the Supabase MCP (recorded as 20261026211804).
--
-- A story's last stage: 'published', set by the Admin app (lib/story-published.ts)
-- once the story has been in a podcast episode that went live — linked to the
-- episode (podcast_productions.idea_ids, ideas.production_id) or covered by it
-- (ideas.episode_id, the back catalogue), and the episode out. The app also
-- stamps published_at with the release.
--
-- The label was in 002's original enum and was lost when the type was recreated
-- by hand; this adds it back. Nothing in the pipeline sets it. Stages that must
-- leave a published story alone: clustering (CLOSED_STATUSES), re-briefing and
-- a hand-run research (see the code in the same change), and the prune (050).
--
-- Apply on its own: a new enum value cannot be used in the transaction that
-- adds it, and 050 names it. Deploy the pipeline code that filters on it only
-- after this is applied, or PostgREST rejects the whole query.

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_enum
        WHERE enumlabel = 'published'
          AND enumtypid = 'cluster_status'::regtype
    ) THEN
        ALTER TYPE cluster_status ADD VALUE 'published';
    END IF;
END$$;
