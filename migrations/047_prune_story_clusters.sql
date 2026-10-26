-- 047_prune_story_clusters.sql
--
-- Applied 26 Oct 2026 through the Supabase MCP in four steps: the functions
-- (20261026084931), the anchor index, the dangling-article step, and the dry-run count
-- excluding anchors. This file is the end state; re-applying it is idempotent.
--
-- Stories are kept only while someone might still use them (decided 26 Oct 2026: the table
-- had 29,984 stories back to April, of which 828 were ever worked on or linked to).
--
--   * A story nobody worked on or linked to is deleted once it is older than p_keep_days
--     (30), and its articles with it.
--   * A story whose real score is under p_min_score (0.20) is deleted at any age. Its
--     articles STAY, marked status 'excluded' / status_reason 'low_score' with no story:
--     ingestion upserts on guid with ignore_duplicates, so a row that still exists stops the
--     next fetch re-adding the article, and clustering takes only status 'included' with no
--     cluster_id, so it cannot rebuild the story. Deleting them outright would let a feed
--     that still carries them bring the story back the next morning. They go once they are
--     older than p_keep_days.
--   * An article older than p_keep_days whose cluster_id names a story that no longer
--     exists is deleted (213 such leftovers from April 2026 were found on the first run).
--
-- A story is never pruned when it was worked on (researched / briefed / ready for content,
-- a research list or research brief, a brief, added by hand, or an article of it has a deep
-- summary) or when anything points at it: a content item's brief_ids, an idea, a newspaper
-- edition (lead or secondary), score feedback, or a later story's source_cluster_ids. None
-- of those are foreign keys, which is why the list is spelled out here. A "Scoring failed"
-- fallback 0.0 is not a low score: those stories are left for the age rule.
--
-- story_prune_candidates() is the one statement of those rules; prune_story_clusters()
-- acts on it and is what the scoring stage calls after every run (scoring/score.py).
--
--   select prune_story_clusters();                     -- dry run: counts only
--   select prune_story_clusters(p_dry_run => false);   -- delete
--
-- Both are service_role only: an RPC in the public schema is otherwise callable with the
-- anon key.

create or replace function public.story_prune_candidates(
  p_min_score numeric default 0.20,
  p_keep_days integer default 30
) returns table (cluster_id text, rule text)
language sql
stable
set search_path = public
as $$
  with used as (
    select unnest(brief_ids) as cid from content_calendar_items where brief_ids is not null
    union select i.cluster_id from ideas i where i.cluster_id is not null
    union select lead_cluster_id from newspaper_editions where lead_cluster_id is not null
    union select unnest(secondary_cluster_ids) from newspaper_editions where secondary_cluster_ids is not null
    union select f.cluster_id from story_score_feedback f where f.cluster_id is not null
    union select unnest(source_cluster_ids) from story_clusters where source_cluster_ids is not null
  ),
  deep as (
    select distinct a.cluster_id from news_articles a
    where a.cluster_id is not null and a.deep_summary is not null
  )
  select s.cluster_id,
         case when s.date < current_date - p_keep_days then 'age' else 'low_score' end
  from story_clusters s
  where s.cluster_status::text not in ('researched', 'briefed', 'ready_for_content')
    and coalesce(s.research::text, '[]') = '[]'
    and nullif(s.research_brief, '') is null
    and not coalesce(s.ready_for_content, false)
    and coalesce(s.cluster_type, 'auto') <> 'manual'
    and s.briefed_at is null
    and not exists (select 1 from used u where u.cid = s.cluster_id)
    and not exists (select 1 from deep d where d.cluster_id = s.cluster_id)
    and (
      s.date < current_date - p_keep_days
      or (s.relevance_score < p_min_score and coalesce(s.score_reason, '') not like 'Scoring failed%')
    )
$$;

create or replace function public.prune_story_clusters(
  p_min_score numeric default 0.20,
  p_keep_days integer default 30,
  p_dry_run boolean default true
) returns jsonb
language plpgsql
set search_path = public
as $$
declare
  v_age text[];
  v_low text[];
  v_cutoff timestamptz := (current_date - p_keep_days)::timestamptz;
  v_articles_deleted integer;
  v_articles_excluded integer;
  v_old_low_score_articles integer;
  v_dangling integer;
begin
  select coalesce(array_agg(c.cluster_id) filter (where c.rule = 'age'), '{}'),
         coalesce(array_agg(c.cluster_id) filter (where c.rule = 'low_score'), '{}')
    into v_age, v_low
    from story_prune_candidates(p_min_score, p_keep_days) c;

  if p_dry_run then
    select count(*) into v_articles_deleted from news_articles where cluster_id = any(v_age);
    select count(*) into v_articles_excluded from news_articles where cluster_id = any(v_low);
    select count(*) into v_old_low_score_articles from news_articles
      where status_reason = 'low_score' and cluster_id is null and fetched_at < v_cutoff;
    select count(*) into v_dangling from news_articles a
      where a.cluster_id is not null and a.fetched_at < v_cutoff
        and not exists (select 1 from story_clusters s where s.cluster_id = a.cluster_id)
        and not exists (select 1 from story_clusters s where s.anchor_article_id = a.id);
    return jsonb_build_object(
      'dry_run', true,
      'stories_deleted_age', cardinality(v_age),
      'stories_deleted_low_score', cardinality(v_low),
      'articles_deleted', v_articles_deleted + v_old_low_score_articles + v_dangling,
      'articles_excluded', v_articles_excluded
    );
  end if;

  delete from story_clusters where cluster_id = any(v_age) or cluster_id = any(v_low);

  -- An old story's articles go with it, unless one anchors a story that is staying.
  delete from news_articles a
    where a.cluster_id = any(v_age)
      and not exists (select 1 from story_clusters s where s.anchor_article_id = a.id);
  get diagnostics v_articles_deleted = row_count;
  update news_articles set cluster_id = null where cluster_id = any(v_age);

  update news_articles
    set status = 'excluded', status_reason = 'low_score', cluster_id = null
    where cluster_id = any(v_low);
  get diagnostics v_articles_excluded = row_count;

  delete from news_articles a
    where a.status_reason = 'low_score' and a.cluster_id is null and a.fetched_at < v_cutoff
      and not exists (select 1 from story_clusters s where s.anchor_article_id = a.id);
  get diagnostics v_old_low_score_articles = row_count;

  delete from news_articles a
    where a.cluster_id is not null and a.fetched_at < v_cutoff
      and not exists (select 1 from story_clusters s where s.cluster_id = a.cluster_id)
      and not exists (select 1 from story_clusters s where s.anchor_article_id = a.id);
  get diagnostics v_dangling = row_count;

  return jsonb_build_object(
    'dry_run', false,
    'stories_deleted_age', cardinality(v_age),
    'stories_deleted_low_score', cardinality(v_low),
    'articles_deleted', v_articles_deleted + v_old_low_score_articles + v_dangling,
    'articles_excluded', v_articles_excluded
  );
end;
$$;

-- Deleting an article checks story_clusters.anchor_article_id for every row; without an
-- index each check scanned the whole table and the first run timed out.
create index if not exists idx_story_clusters_anchor_article_id on public.story_clusters (anchor_article_id);

revoke all on function public.story_prune_candidates(numeric, integer) from public, anon, authenticated;
revoke all on function public.prune_story_clusters(numeric, integer, boolean) from public, anon, authenticated;
grant execute on function public.story_prune_candidates(numeric, integer) to service_role;
grant execute on function public.prune_story_clusters(numeric, integer, boolean) to service_role;

-- ── The first run (26 Oct 2026), run once by hand, not part of re-applying this file ──
-- It removed ~25,800 stories, so the rows were snapshotted first; the snapshots were
-- checked and dropped the same day. Kept here as the recipe for a future bulk run.
--
--   begin;
--   create table backfill_pruned_story_clusters_20261026 as
--     select s.*, c.rule as prune_rule from story_clusters s
--     join story_prune_candidates() c using (cluster_id);
--   create table backfill_pruned_news_articles_20261026 as
--     select a.* from news_articles a
--     where a.cluster_id in (select cluster_id from backfill_pruned_story_clusters_20261026);
--   alter table backfill_pruned_story_clusters_20261026 enable row level security;
--   alter table backfill_pruned_news_articles_20261026 enable row level security;
--   revoke all on backfill_pruned_story_clusters_20261026 from anon, authenticated;
--   revoke all on backfill_pruned_news_articles_20261026 from anon, authenticated;
--   select prune_story_clusters(p_dry_run => false);
--   commit;
