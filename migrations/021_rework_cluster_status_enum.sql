-- Rework cluster_status enum for the simplified pipeline flow:
--   pending → scored → (published, archived)
--
-- Removes: scoring, accepted, rejected, briefed
-- Adds:    scored, researched
--
-- PostgreSQL cannot drop enum values directly, so this recreates the type.
-- NOTE: if this was already run manually in Supabase, the ADD VALUE lines
-- are safe to re-run (IF NOT EXISTS guard).

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_enum
        WHERE enumlabel = 'scored'
          AND enumtypid = 'cluster_status'::regtype
    ) THEN
        ALTER TYPE cluster_status ADD VALUE 'scored';
    END IF;
END$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_enum
        WHERE enumlabel = 'researched'
          AND enumtypid = 'cluster_status'::regtype
    ) THEN
        ALTER TYPE cluster_status ADD VALUE 'researched';
    END IF;
END$$;
