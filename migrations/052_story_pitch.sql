-- 052_story_pitch.sql
--
-- Applied 28 Oct 2026 through the Supabase MCP (recorded as 20261028095749).
--
-- A story's pitch (28 Oct 2026). After scoring and research, a researcher in the
-- Admin app writes a short pitch and sends it; an Admin approves it into
-- development ('briefed') or passes on it. Two new cluster_status labels:
--   pitched  sent, waiting for an Admin's decision
--   passed   an Admin said no; kept 30 days, then archived
--
-- The prune (story_prune_candidates) keeps a story with a pitch, as it keeps one
-- with a prep sheet, comments or votes, and never touches 'pitched' or 'passed'.
-- prune_story_clusters now also archives a passed story p_keep_days (30) after
-- the decision. Otherwise both functions are 051's / 047's, unchanged.
--
-- Apply after the Admin app's supabase/add_story_pitch.sql (the pitch columns),
-- and before deploying the clustering change that lists these labels in
-- CLOSED_STATUSES (PostgREST rejects a query naming a label the enum lacks).

alter type cluster_status add value if not exists 'pitched';
alter type cluster_status add value if not exists 'passed';

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
    union select v.story_id from story_votes v
  ),
  deep as (
    select distinct a.cluster_id from news_articles a
    where a.cluster_id is not null and a.deep_summary is not null
  )
  select s.cluster_id,
         case when s.date < current_date - p_keep_days then 'age' else 'low_score' end
  from story_clusters s
  where s.cluster_status::text not in ('researched', 'briefed', 'ready_for_content', 'published', 'pitched', 'passed')
    and s.published_at is null
    and coalesce(s.research::text, '[]') = '[]'
    and nullif(s.research_brief, '') is null
    and not coalesce(s.ready_for_content, false)
    and coalesce(s.cluster_type, 'auto') <> 'manual'
    and s.briefed_at is null
    and coalesce(s.comments, '[]'::jsonb) = '[]'::jsonb
    and nullif(trim(coalesce(s.prep_notes, '')), '') is null
    and nullif(trim(coalesce(s.pitch, '')), '') is null
    and s.owner_email is null
    and not exists (select 1 from used u where u.cid = s.cluster_id)
    and not exists (select 1 from deep d where d.cluster_id = s.cluster_id)
    and (
      s.date < current_date - p_keep_days
      or (s.relevance_score < p_min_score and coalesce(s.score_reason, '') not like 'Scoring failed%')
    )
$$;

revoke all on function public.story_prune_candidates(numeric, integer) from public, anon, authenticated;
grant execute on function public.story_prune_candidates(numeric, integer) to service_role;

create or replace function public.prune_story_clusters(
  p_min_score numeric default 0.20,
  p_keep_days integer default 30,
  p_dry_run boolean default true
) returns jsonb
language plpgsql
set search_path to 'public'
as $function$
declare
  v_age text[];
  v_low text[];
  v_cutoff timestamptz := (current_date - p_keep_days)::timestamptz;
  v_articles_deleted integer;
  v_articles_excluded integer;
  v_old_low_score_articles integer;
  v_dangling integer;
  v_passed_archived integer;
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
    select count(*) into v_passed_archived from story_clusters
      where cluster_status::text = 'passed' and pitch_decided_at < v_cutoff;
    return jsonb_build_object(
      'dry_run', true,
      'stories_deleted_age', cardinality(v_age),
      'stories_deleted_low_score', cardinality(v_low),
      'articles_deleted', v_articles_deleted + v_old_low_score_articles + v_dangling,
      'articles_excluded', v_articles_excluded,
      'passed_archived', v_passed_archived
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

  -- A pitch an Admin passed on is kept for p_keep_days after the decision, then archived.
  update story_clusters set cluster_status = 'archived'
    where cluster_status::text = 'passed' and pitch_decided_at < v_cutoff;
  get diagnostics v_passed_archived = row_count;

  return jsonb_build_object(
    'dry_run', false,
    'stories_deleted_age', cardinality(v_age),
    'stories_deleted_low_score', cardinality(v_low),
    'articles_deleted', v_articles_deleted + v_old_low_score_articles + v_dangling,
    'articles_excluded', v_articles_excluded,
    'passed_archived', v_passed_archived
  );
end;
$function$;
