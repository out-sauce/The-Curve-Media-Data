-- 042 — One message, one row: repair the inbox_messages id columns.
--
-- SYMPTOM: 494 threads rendered every message twice in the Admin inbox.
--
-- CAUSE: the two payload shapes that reach _message_row disagree about what `id`
-- MEANS, and the code trusted it blindly. In the REST listing `id` is Zernio's own
-- ObjectId and Meta's id arrives separately as `platformMessageId`; in the webhook
-- envelope `id` IS Meta's id and there is no `platformMessageId` at all. So a
-- webhook-delivered message was filed with Meta's id in the ZERNIO_MESSAGE_ID
-- column and nothing in platform_message_id. The row the sweep wrote later carried
-- the ObjectId, matched no existing row on either unique index, and inserted.
--
-- Both partial unique indexes were already correct and neither could fire: one saw
-- two different values, the other saw a NULL (and NULLs are always distinct).
--
-- THIS MIGRATION IS REQUIRED BY THE CODE FIX, not merely tidy-up. Once
-- _split_message_ids routes Meta ids into platform_message_id, a sweep row for any
-- of the 5,041 mis-keyed rows below would look itself up by (conversation_id,
-- platform_message_id), find NULL, and insert a duplicate. Shipping the code
-- without this would turn 494 doubled threads into ~5,000.
--
-- Verified before writing (live, 2026-08-25): 484 rows to delete, 5,041 to re-key,
-- zero (conversation_id, platform_message_id) collisions after re-keying, against a
-- table of 6,009. 484 + 5,041 accounts for every row holding a NULL
-- platform_message_id, so nothing is left in the broken shape.

begin;

-- Snapshot every row this deletes, permanently. The same posture as the Admin's
-- backfill_youtube_episode_dupes: a delete driven by an inferred relationship should
-- always be reversible by hand, and the pre-image is the only thing that makes the
-- "did we drop something real?" question answerable later.
create table if not exists backfill_inbox_duplicate_messages (
  like inbox_messages including defaults,
  removed_at timestamptz not null default now(),
  kept_row_id uuid
);

insert into backfill_inbox_duplicate_messages
select w.*, now(),
       (select r.id from inbox_messages r
        where r.conversation_id = w.conversation_id
          and r.platform_message_id = w.zernio_message_id
          and r.id <> w.id
        limit 1)
from inbox_messages w
where w.platform_message_id is null
  and exists (select 1 from inbox_messages r
              where r.conversation_id = w.conversation_id
                and r.platform_message_id = w.zernio_message_id
                and r.id <> w.id);

-- 1. Drop the webhook twin, keeping the REST row — it carries BOTH ids, so keeping it
--    leaves the thread fully keyed. Deleting the richer row and re-keying the poorer
--    one would reach the same shape while losing the vendor id for good.
delete from inbox_messages w
where w.platform_message_id is null
  and exists (select 1 from inbox_messages r
              where r.conversation_id = w.conversation_id
                and r.platform_message_id = w.zernio_message_id
                and r.id <> w.id);

-- 2. Re-key every surviving mis-filed row: Meta's id belongs in platform_message_id,
--    and zernio_message_id goes back to NULL because we genuinely do not know it —
--    the webhook never told us. The next sweep backfills it (see upsert_inbox_messages,
--    which now UPDATEs a row whose vendor id is missing).
--
--    The regex is the discriminator and it is exact: a Zernio vendor id is a Mongo
--    ObjectId, 24 lowercase hex characters; Meta's message ids are long base64 blobs.
--    Confirmed on live data — of the 5,525 rows holding a NULL platform_message_id,
--    5,525 are Meta-style and zero are ObjectId-style, so this cannot strand a real
--    vendor id in the wrong column.
update inbox_messages
   set platform_message_id = zernio_message_id,
       zernio_message_id   = null,
       updated_at          = now()
 where platform_message_id is null
   and zernio_message_id !~ '^[0-9a-f]{24}$';

-- 3. Prove it. Any row still holding a Meta-style id in the vendor column means the
--    discriminator missed something; any duplicate platform id in one conversation
--    means the delete missed a twin. Either way, roll the whole thing back.
do $$
declare
  stragglers int;
  dupes      int;
begin
  select count(*) into stragglers from inbox_messages
   where platform_message_id is null and zernio_message_id !~ '^[0-9a-f]{24}$';
  select count(*) into dupes from (
    select conversation_id, platform_message_id
      from inbox_messages
     where platform_message_id is not null
     group by 1, 2 having count(*) > 1) x;
  if stragglers > 0 or dupes > 0 then
    raise exception 'Aborting: % mis-keyed row(s) remain, % duplicate platform id(s)',
      stragglers, dupes;
  end if;
end $$;

commit;
