-- 041 — "To reply" becomes a judgement, not a derivation.
--
-- Until now the Admin's To-reply list was derived: the newest message in the thread came
-- from them. That is the right shape for a fresh DM and wrong for most of the backlog —
-- of 116 threads whose tail was incoming, 64 ended in an attachment with no text at all
-- (a story share, a reaction), so the list was mostly threads nobody could act on and
-- nobody could clear. The drafting stage now judges each one and records the answer here,
-- and only threads it marks are drafted.
--
-- `needs_reply` IS NULLABLE AND NULL IS LOAD-BEARING: it means "not judged yet". The
-- Admin falls back to the old derivation for those, so a DM arriving between runs still
-- shows as To reply immediately instead of waiting for the stage to catch up. Three
-- states, not two — do not add a default.
--
-- `needs_reply_for_message_id` plays the same role draft_for_message_id does (migration
-- 040): staleness and idempotency. A judgement made against message #7 tells you nothing
-- once #8 arrives, and without the marker every scheduled run would re-judge the whole
-- inbox. The judgement and the draft are written in ONE update, so the two markers move
-- together and a half-written row is not reachable.
--
-- Applied manually, like every migration here. Nothing auto-applies at runtime.

alter table inbox_conversations
  add column if not exists needs_reply boolean,
  -- One short sentence, in the model's words, shown to the operator beside the thread.
  -- A bare boolean is not reviewable: "no reply needed" on a thread that plainly needed
  -- one has to be arguable with, or nobody will trust the mark.
  add column if not exists needs_reply_reason text,
  add column if not exists needs_reply_for_message_id uuid
    references inbox_messages(id) on delete set null,
  add column if not exists needs_reply_at timestamptz;

comment on column inbox_conversations.needs_reply is
  'Judged by the drafting stage: does this thread owe a reply? NULL = not judged yet, and the Admin falls back to "newest message is theirs" for those.';
comment on column inbox_conversations.needs_reply_reason is
  'The model''s one-line justification for the mark. Operator-facing.';
comment on column inbox_conversations.needs_reply_for_message_id is
  'The inbox_messages row this judgement was made against. Drives staleness and skip-if-current.';

-- The Admin reads "every thread still owing a reply" on every page load, and that set is
-- small against a table that only grows.
create index if not exists inbox_conversations_needs_reply
  on inbox_conversations (needs_reply)
  where needs_reply;
