-- 039_add_inbox.sql
--
-- The comments + DM inbox. This is the capability the whole Outstand → Zernio move was
-- for: Outstand had no comments API and no DM API, so nothing like this existed in
-- either repo.
--
-- Ownership split, mirrored from how publishing already works:
--   Curve_Data_Py  writes every row here — the Zernio webhook receiver and the
--                  reconciliation sweep (ingestion/inbox.py).
--   Admin app      reads them, and owns exactly three things: inbox_conversations.
--                  is_read/read_at/read_by, inbox_comments.handled_at/handled_by, and
--                  the optimistic rows it inserts for its own outbound replies
--                  (source='admin'). The sweep must never overwrite those.
--
-- RLS is enabled with no policies on every table — service-role access only, matching
-- research_queue / domain_scrape_settings / site_auth.
--
-- Idempotent; applied manually like every other migration here.


-- ── the idempotency ledger ────────────────────────────────────────────────────
-- Zernio delivers at-least-once (7 retries over ~51h, then dead-letter) with a 5-second
-- ack budget, so the receiver has to be able to say "seen it" instantly and without
-- doing the work twice. The PRIMARY KEY *is* that mechanism: an INSERT ... ON CONFLICT
-- DO NOTHING that inserts nothing is a duplicate delivery, and no work is scheduled.
--
-- The ledger also makes the event durable BEFORE we ack, so a crash between the ack and
-- the write loses nothing — drain_ledger() reprocesses anything still pending.

create table if not exists inbox_webhook_events (
  event_id     text primary key,            -- envelope `id` (uuid), == X-Zernio-Event-Id
  event        text not null,
  payload      jsonb not null,
  event_time   timestamptz,                 -- envelope `timestamp`; stable across retries
  received_at  timestamptz not null default now(),
  status       text not null default 'pending'
                 check (status in ('pending','processed','ignored','deferred','failed')),
  attempts     integer not null default 0,
  error        text,
  processed_at timestamptz
);

comment on table inbox_webhook_events is
  'Zernio webhook ledger. The PK is the dedupe key for at-least-once delivery. status=deferred means the event arrived before the conversation it belongs to was mirrored — it is retried by the sweep, not dropped.';

create index if not exists inbox_webhook_events_pending_idx
  on inbox_webhook_events (status, received_at) where status <> 'processed';

alter table inbox_webhook_events enable row level security;


-- ── conversations (DMs) ───────────────────────────────────────────────────────
-- Two identity keys, deliberately. The list endpoint's `id` is documented as opaque
-- ("do not assume a fixed format") while webhooks carry both an internal conversation id
-- and a platform one; the spec only promises they are interchangeable as path params,
-- never that they are the same string. So (platform, account_id, participant_id) is the
-- durable identity and the Zernio id is a secondary lookup. Both indexes are partial, so
-- a row missing either key is still legal.

create table if not exists inbox_conversations (
  id                       uuid primary key default gen_random_uuid(),
  platform                 text not null,
  account_id               text not null,          -- Zernio social account id
  account_username         text,
  participant_id           text,
  participant_name         text,
  participant_username     text,
  participant_picture      text,
  zernio_conversation_id   text,
  platform_conversation_id text,
  last_message             text,
  last_message_at          timestamptz,
  status                   text not null default 'active',
  unread_count             integer not null default 0,     -- the PLATFORM's count
  is_read                  boolean not null default true,  -- OURS (admin-owned)
  read_at                  timestamptz,
  read_by                  text,
  ig_is_follower           boolean,
  ig_is_following          boolean,
  permalink                text,
  first_seen_at            timestamptz not null default now(),
  updated_at               timestamptz not null default now()
);

comment on column inbox_conversations.unread_count is
  'Zernio/platform unread count. NOT the admin badge: Meta pre-connect history is replayed as already-read, so this cannot represent "has an operator looked at this here". Use is_read.';
comment on column inbox_conversations.is_read is
  'Admin-owned. Set false by any incoming message, true when an operator opens the thread. The pipeline never overwrites it.';

create unique index if not exists inbox_conversations_natural_key
  on inbox_conversations (platform, account_id, participant_id)
  where participant_id is not null;
create unique index if not exists inbox_conversations_zernio_id
  on inbox_conversations (zernio_conversation_id)
  where zernio_conversation_id is not null;
create index if not exists inbox_conversations_recent_idx
  on inbox_conversations (last_message_at desc);

alter table inbox_conversations enable row level security;


-- ── messages ──────────────────────────────────────────────────────────────────

create table if not exists inbox_messages (
  id                  uuid primary key default gen_random_uuid(),
  conversation_id     uuid not null references inbox_conversations(id) on delete cascade,
  zernio_message_id   text,
  platform_message_id text,
  direction           text not null check (direction in ('incoming','outgoing')),
  body                text,
  sender_id           text,
  sender_name         text,
  attachments         jsonb not null default '[]'::jsonb,
  sent_at             timestamptz not null,
  delivery_status     text,   -- sent | delivered | read | failed | deleted
  is_deleted          boolean not null default false,
  is_edited           boolean not null default false,
  reactions           jsonb not null default '[]'::jsonb,
  story_reply         boolean,
  source              text not null default 'zernio' check (source in ('zernio','admin')),
  sent_by             text,   -- admin user email, when we sent it
  created_at          timestamptz not null default now(),
  updated_at          timestamptz not null default now()
);

comment on column inbox_messages.attachments is
  'Store each item''s refreshUrl in preference to url — Meta attachment URLs are signed CDN links that expire, refreshUrl is safe to keep. Same lesson as the post thumbnails.';
comment on column inbox_messages.source is
  'admin = written optimistically by the admin when an operator sent it (there is no comment.sent/message echo we can rely on for the immediate render). The sweep UPDATEs these rather than inserting a second copy.';

create unique index if not exists inbox_messages_zernio_id
  on inbox_messages (zernio_message_id) where zernio_message_id is not null;
create unique index if not exists inbox_messages_platform_id
  on inbox_messages (conversation_id, platform_message_id) where platform_message_id is not null;
create index if not exists inbox_messages_thread_idx
  on inbox_messages (conversation_id, sent_at);

alter table inbox_messages enable row level security;


-- ── posts that carry comments ─────────────────────────────────────────────────
-- Also the linkage anchor: platform_post_id is the platform's OWN id, which is exactly
-- what content_stats.post_id holds, so a comment thread can be traced to our own post
-- and from there (content_stats.calendar_item_id) to the calendar item. Resolved at
-- write time into content_stats_id and never re-derived at read time.

create table if not exists inbox_posts (
  id               uuid primary key default gen_random_uuid(),
  platform         text not null,
  account_id       text not null,
  account_username text,
  platform_post_id text not null,
  content          text,
  picture          text,
  permalink        text,
  posted_at        timestamptz,
  comment_count    integer not null default 0,
  like_count       integer,
  is_ad            boolean not null default false,
  ad_id            text,
  placement        text,
  content_stats_id uuid references content_stats(id) on delete set null,
  last_synced_at   timestamptz,
  last_comment_at  timestamptz,
  created_at       timestamptz not null default now(),
  updated_at       timestamptz not null default now()
);

comment on column inbox_posts.content_stats_id is
  'Filled only when NULL, and retried every sweep: a post published minutes ago has no content_stats row yet (analytics is hourly). NULL is a normal state, not an error — a competitor post or a guest-owned collab will never link.';
comment on column inbox_posts.comment_count is
  'The platform''s own count, and its meaning differs by platform — YouTube includes replies, Facebook counts top-level only. Never compare it across platforms.';

create unique index if not exists inbox_posts_key
  on inbox_posts (platform, platform_post_id);
create index if not exists inbox_posts_recent_idx on inbox_posts (posted_at desc);

alter table inbox_posts enable row level security;


-- ── comments ──────────────────────────────────────────────────────────────────

create table if not exists inbox_comments (
  id                  uuid primary key default gen_random_uuid(),
  post_id             uuid not null references inbox_posts(id) on delete cascade,
  platform            text not null,
  platform_comment_id text not null,
  parent_comment_id   text,
  body                text,
  author_id           text,
  author_username     text,
  author_name         text,
  author_picture      text,
  author_is_owner     boolean not null default false,
  like_count          integer,
  reply_count         integer,
  is_hidden           boolean not null default false,
  is_liked            boolean not null default false,
  can_reply           boolean,
  can_hide            boolean,
  can_delete          boolean,
  permalink           text,
  commented_at        timestamptz not null,
  is_deleted          boolean not null default false,
  handled_at          timestamptz,   -- OURS (admin-owned triage)
  handled_by          text,
  source              text not null default 'zernio' check (source in ('zernio','admin')),
  sent_by             text,
  created_at          timestamptz not null default now(),
  updated_at          timestamptz not null default now()
);

comment on column inbox_comments.parent_comment_id is
  'The PLATFORM id of the parent, deliberately not a uuid FK. A reply''s webhook can legitimately arrive before its parent is mirrored (the parent may predate our lookback); an FK would reject the row, a string always writes and the UI resolves nesting.';
comment on column inbox_comments.reply_count is
  'The platform''s own count — includes hidden and deleted replies, so it can exceed the replies actually fetched. Not a reliable "are we missing replies" signal on Instagram.';
comment on column inbox_comments.handled_at is
  'Admin-owned triage flag. There is no platform read-state for comments; this is ours alone. The pipeline never writes it.';

create unique index if not exists inbox_comments_key
  on inbox_comments (platform, platform_comment_id);
create index if not exists inbox_comments_post_idx on inbox_comments (post_id, commented_at);
create index if not exists inbox_comments_unhandled_idx
  on inbox_comments (commented_at desc)
  where handled_at is null and author_is_owner = false and is_deleted = false;

alter table inbox_comments enable row level security;
