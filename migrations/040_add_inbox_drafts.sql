-- 040 — AI-drafted DM replies.
--
-- One draft per THREAD, not per message: an operator replies to a conversation, and a
-- second draft sitting against an older message in the same thread is just noise.
--
-- The draft is a DISPOSABLE SUGGESTION, deliberately. The operator copies it into the
-- composer, edits, and sends — which creates a normal `source='admin'` row in
-- inbox_messages exactly as it does today. Nothing here is ever edited in place, so
-- there is no "operator touched this" flag to respect and the drafting stage may
-- overwrite these columns freely. That is the whole reason this is four nullable
-- columns rather than a table with its own lifecycle.
--
-- Consequence worth knowing: "was the draft used?" is not stored, because it is
-- derivable — an outgoing message in the thread with sent_at > draft_generated_at.
--
-- Applied manually, like every migration here. Nothing auto-applies at runtime.

alter table inbox_conversations
  add column if not exists draft_response     text,
  -- WHICH message the draft answers. Load-bearing, not bookkeeping:
  --   * staleness — a draft written against message #7 is still sitting there after #8
  --     arrives, now answering the wrong question, and the operator cannot tell by
  --     looking. Comparing this against the thread's newest incoming message is the
  --     only way to know.
  --   * idempotency — the stage runs on a schedule. Without this marker every run
  --     redrafts every eligible thread; with it, a thread whose draft is already
  --     current is skipped. Same shape as story_clusters.last_article_at driving
  --     _brief_is_current (migration 031).
  add column if not exists draft_for_message_id uuid
    references inbox_messages(id) on delete set null,
  add column if not exists draft_generated_at timestamptz,
  -- The gate is deliberately NOT enforced yet — the classification is written so that
  -- switching enforcement on later is a config change rather than a re-run over the
  -- whole inbox. ~24% of substantive incoming DMs touch the fund or investing, which is
  -- regulated territory; that is what this column exists to let us filter on.
  add column if not exists draft_category     text;

comment on column inbox_conversations.draft_response is
  'AI-drafted reply suggestion. Disposable — safe to overwrite; never edited in place.';
comment on column inbox_conversations.draft_for_message_id is
  'The inbox_messages row this draft answers. Drives staleness and skip-if-current.';
comment on column inbox_conversations.draft_category is
  'Intent classification. Written but not enforced — see the gate note above.';

-- The eligibility query needs the newest message per thread. PostgREST cannot express
-- "latest row per group", so it is a view; the stage filters it and joins the draft
-- columns client-side.
create or replace view inbox_thread_latest as
select distinct on (m.conversation_id)
       m.conversation_id,
       m.id                as last_message_id,
       m.direction         as last_direction,
       m.body              as last_body,
       m.sent_at           as last_sent_at
from inbox_messages m
order by m.conversation_id, m.sent_at desc, m.id;

comment on view inbox_thread_latest is
  'Newest message per conversation. Backs the drafting stage''s eligibility query.';

-- distinct on (conversation_id) ... order by conversation_id, sent_at desc
create index if not exists inbox_messages_thread_recent
  on inbox_messages (conversation_id, sent_at desc);

-- Exemplar pairs for the drafting prompt: every (their message -> our reply) adjacency,
-- which needs lag() and so cannot be expressed through PostgREST either.
--
-- The length columns are projected because PostgREST cannot filter on length(col), and
-- the filtering matters: of 473 raw pairs, 111 have replies under 25 characters ("😂",
-- "thank you!"). Those are real replies but useless as style examples — feeding them in
-- teaches the model to answer everything with an emoji.
create or replace view inbox_reply_pairs as
select conversation_id,
       incoming,
       ours,
       sent_at,
       length(incoming) as incoming_len,
       length(ours)     as ours_len
from (
  select m.conversation_id,
         m.body      as ours,
         m.direction as dir,
         m.sent_at,
         lag(m.body)      over (partition by m.conversation_id order by m.sent_at) as incoming,
         lag(m.direction) over (partition by m.conversation_id order by m.sent_at) as prev_dir
  from inbox_messages m
) t
where dir = 'outgoing' and prev_dir = 'incoming'
  and ours is not null and incoming is not null;

comment on view inbox_reply_pairs is
  'Their-message -> our-reply adjacencies. Exemplar corpus for the drafting prompt.';
