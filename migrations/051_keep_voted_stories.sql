-- 051_keep_voted_stories.sql
--
-- Applied 27 Oct 2026 through the Supabase MCP (recorded as 20261027091126).
--
-- A story the team has voted for (the Admin app's story_votes, one upvote per
-- person per story) has been picked out by someone and is never pruned:
-- story_prune_candidates() now counts a vote as a use, like an idea or a
-- calendar link. Otherwise the function is 050's, unchanged.
--
-- Apply after the Admin app's supabase/add_story_votes.sql (the table must exist).

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
  where s.cluster_status::text not in ('researched', 'briefed', 'ready_for_content', 'published')
    and s.published_at is null
    and coalesce(s.research::text, '[]') = '[]'
    and nullif(s.research_brief, '') is null
    and not coalesce(s.ready_for_content, false)
    and coalesce(s.cluster_type, 'auto') <> 'manual'
    and s.briefed_at is null
    and coalesce(s.comments, '[]'::jsonb) = '[]'::jsonb
    and nullif(trim(coalesce(s.prep_notes, '')), '') is null
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
