"""
Zernio inbox — comments and direct messages, mirrored into Supabase.

This is the capability the Outstand → Zernio migration was for. Outstand had no
comments API and no DM API at all, so nothing here replaces anything; it is new
ground.

WHY BOTH A WEBHOOK AND A SWEEP. The webhook is the fast path and the sweep is the
correct one, and neither is redundant:
  - Meta REPLAYS pre-connect history when an account is connected (up to 500
    conversations per account), in the background, emitting NO webhooks, and keeps
    each thread's original timestamp so replayed threads sort into date order rather
    than to the top of the list. A single pass at connect time provably cannot see
    all of it — the vendor's own docs tell integrators to re-run the sweep.
  - A webhook delivery that fails 7 times over ~51 hours is dead-lettered and gone.
  - Comment state is mutable and un-evented: a third party editing, hiding, deleting
    or liking a comment produces no webhook.
So: nothing depends on webhook completeness. The webhook exists so a new comment
shows up in seconds instead of within fifteen minutes.

WHAT THIS MODULE DOES NOT DO. Outbound actions — replying, hiding, deleting, marking
read — belong to the Admin app, which calls Zernio directly, exactly as it already
does for publishing. An operator's reply needs a synchronous success/failure to
render, and every /run/* endpoint here is fire-and-forget by convention. This module
is ingest only.

PLATFORM COVERAGE IS NOT UNIFORM, and the UI has to say so rather than showing an
empty tab: TikTok has NO comment API and NO DMs in Zernio at all — and TikTok is one
of the four brand channels. YouTube, LinkedIn and Threads have comments but no DMs.
DMs in practice means Instagram (and Facebook, if a Page is ever connected).

Never raises — failures log and skip, matching the rest of ingestion/.
"""

import hashlib
import hmac
import logging
from datetime import datetime, timedelta, timezone

from config import (
    PUBLIC_BASE_URL,
    ZERNIO_WEBHOOK_SECRET,
    INBOX_SWEEP_LOOKBACK_DAYS,
    INBOX_PAGE_LIMIT,
    INBOX_MAX_PAGES,
    INBOX_FULL_MAX_PAGES,
    INBOX_THREAD_REFRESH_DAYS,
    INBOX_EVENT_RETENTION_DAYS,
)
from ingestion.zernio import zernio_get, zernio_request
from ingestion.storage import (
    claim_webhook_events,
    find_inbox_conversation,
    get_inbox_conversation_state,
    get_inbox_post_state,
    get_webhook_event,
    log_source_run,
    mark_conversation_unread,
    mark_webhook_event,
    prune_webhook_events,
    record_webhook_event,
    resolve_inbox_post_link,
    upsert_inbox_comments,
    upsert_inbox_conversation,
    upsert_inbox_messages,
    upsert_inbox_post,
)

logger = logging.getLogger(__name__)

_RUN_CATEGORY = "zernio_inbox"

# Events we subscribe to and act on. Anything else is acknowledged and ignored — an
# unknown event must never produce a non-2xx, because 10 consecutive delivery failures
# make Zernio disable the whole subscription.
INBOX_WEBHOOK_EVENTS = (
    "comment.received",
    "message.received",
    "conversation.started",
    "message.sent",
    "message.edited",
    "message.deleted",
    "message.delivered",
    "message.read",
    "reaction.received",
)

# TikTok is deliberately absent: Zernio exposes neither comments nor DMs for it.
_COMMENT_PLATFORMS = ("instagram", "facebook", "youtube", "linkedin")
_MAX_EVENT_ATTEMPTS = 5


# ── webhook signature ─────────────────────────────────────────────────────────

def verify_signature(raw_body: bytes, header: str | None) -> bool:
    """
    Verify X-Zernio-Signature: a lowercase hex HMAC-SHA256 of the RAW request body.

    Note the difference from the Outstand verifier this replaces: there is no `sha256=`
    prefix. Stripping one that isn't there would be harmless; *requiring* one would
    reject every delivery, which is why the old verifier can't simply be copied. A
    prefix is tolerated defensively in case the vendor adds one.

    Fails closed when no secret is configured — an unverified webhook is an open write
    endpoint, and this one writes private message content.
    """
    if not ZERNIO_WEBHOOK_SECRET or not header:
        return False
    expected = hmac.new(
        ZERNIO_WEBHOOK_SECRET.encode(), raw_body, hashlib.sha256,
    ).hexdigest()
    candidate = header.strip()
    if candidate.startswith("sha256="):
        candidate = candidate[len("sha256="):]
    return hmac.compare_digest(expected, candidate.lower())


# ── helpers ───────────────────────────────────────────────────────────────────

def _parse_dt(value) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt


def _attachment_rows(items) -> list[dict]:
    """
    Normalise message attachments, preferring a durable URL over an expiring one.

    Instagram/Facebook attachment URLs are platform CDN links that expire on the
    platform's schedule, so `refreshUrl` — where the vendor offers one — is what should
    be stored. Same lesson already burned into the post-thumbnail path.
    """
    rows = []
    for item in items or []:
        if not isinstance(item, dict):
            continue
        rows.append({
            "type": item.get("type"),
            "url": item.get("refreshUrl") or item.get("url"),
            "expiring_url": item.get("url") if item.get("refreshUrl") else None,
        })
    return rows


# ── normalisers ───────────────────────────────────────────────────────────────

def _conversation_row(conversation: dict, account: dict, platform: str) -> dict:
    profile = conversation.get("instagramProfile") or {}
    return {
        "platform": platform,
        "account_id": account.get("accountId") or account.get("id"),
        "account_username": account.get("username"),
        "participant_id": conversation.get("participantId"),
        "participant_name": conversation.get("participantName"),
        "participant_username": conversation.get("participantUsername"),
        "participant_picture": conversation.get("participantPicture"),
        "zernio_conversation_id": conversation.get("id"),
        "platform_conversation_id": conversation.get("platformConversationId"),
        "status": conversation.get("status") or "active",
        # Absent means UNKNOWN, never "not a follower" — Meta gates the follow
        # relationship behind messaging consent.
        "ig_is_follower": profile.get("isFollower"),
        "ig_is_following": profile.get("isFollowing"),
    }


def _message_row(message: dict, conversation_uuid: str) -> dict:
    return {
        "conversation_id": conversation_uuid,
        "zernio_message_id": message.get("id"),
        "platform_message_id": message.get("platformMessageId"),
        "direction": message.get("direction") or "incoming",
        "body": message.get("text"),
        "sender_id": (message.get("sender") or {}).get("id"),
        "sender_name": (
            (message.get("sender") or {}).get("name")
            or (message.get("sender") or {}).get("username")
        ),
        "attachments": _attachment_rows(message.get("attachments")),
        "sent_at": message.get("sentAt") or message.get("createdAt"),
        "story_reply": bool(message.get("storyReply") or message.get("isStoryReply")),
    }


def _post_row(post: dict, account: dict, platform: str) -> dict:
    return {
        "platform": platform,
        "account_id": account.get("accountId") or account.get("id"),
        "account_username": account.get("username"),
        "platform_post_id": post.get("platformPostId") or post.get("id"),
        "content": post.get("content"),
        "picture": post.get("imageUrl") or post.get("picture"),
        "permalink": post.get("permalink"),
        "posted_at": post.get("createdTime") or post.get("publishedAt"),
        "comment_count": post.get("commentCount"),
        "like_count": post.get("likeCount"),
        "is_ad": bool(post.get("isAd")),
        "ad_id": post.get("adId"),
        "placement": post.get("placement"),
    }


def _comment_row(comment: dict, post_uuid: str, platform: str,
                 account_username: str | None,
                 parent_id: str | None = None) -> dict:
    author = comment.get("author") or {}
    author_username = author.get("username")
    return {
        "post_id": post_uuid,
        "platform": platform,
        "platform_comment_id": comment.get("id"),
        # The enclosing comment wins over the payload's own parentCommentId: that key is
        # ABSENT (not null) on top-level Facebook/Instagram entries, so .get() is the
        # only safe read, and nesting is the relationship those platforms actually
        # express.
        "parent_comment_id": parent_id or comment.get("parentCommentId"),
        "body": comment.get("text"),
        "author_id": author.get("id"),
        "author_username": author_username,
        "author_name": author.get("name"),
        "author_picture": author.get("picture"),
        "author_is_owner": bool(
            author_username and account_username
            and author_username.lstrip("@").lower() == account_username.lstrip("@").lower()
        ),
        "like_count": comment.get("likeCount"),
        "reply_count": comment.get("replyCount"),
        "is_hidden": bool(comment.get("isHidden")),
        "is_liked": bool(comment.get("isLiked")),
        "can_reply": comment.get("canReply"),
        "can_hide": comment.get("canHide"),
        "can_delete": comment.get("canDelete"),
        "permalink": comment.get("url"),
        "commented_at": comment.get("createdAt") or comment.get("createdTime"),
    }


def _flatten_comments(items, post_uuid: str, platform: str,
                      account_username: str | None,
                      parent_id: str | None = None) -> list[dict]:
    """Walk a comment thread, stamping each reply with its enclosing comment's id."""
    rows = []
    for item in items or []:
        if not isinstance(item, dict) or not item.get("id"):
            continue
        rows.append(_comment_row(item, post_uuid, platform, account_username, parent_id))
        rows.extend(
            _flatten_comments(
                item.get("replies"), post_uuid, platform, account_username, item["id"],
            )
        )
    return rows


# ── webhook processing ────────────────────────────────────────────────────────

def _resolve_conversation(payload: dict) -> tuple[str | None, dict]:
    """
    Find (or create) the conversation an inbox event belongs to.

    InboxWebhookConversation only *requires* id/platformConversationId/status —
    participantId is optional — so a payload can arrive that we cannot key. Returning
    None lets the caller mark the event `deferred` and let the sweep resolve it, which
    is strictly better than inventing a conversation from a fragment and then never
    being able to merge it with the real one.
    """
    conversation = payload.get("conversation") or {}
    account = payload.get("account") or {}
    platform = (
        conversation.get("platform")
        or account.get("platform")
        or (payload.get("message") or {}).get("platform")
    )
    account_id = account.get("accountId") or account.get("id")
    if not platform or not account_id:
        return None, {}

    row = _conversation_row(conversation, account, platform)
    # An incoming message's sender IS the participant, which recovers the natural key
    # when the conversation block omits it.
    message = payload.get("message") or {}
    if not row.get("participant_id") and message.get("direction") == "incoming":
        row["participant_id"] = (message.get("sender") or {}).get("id")

    if row.get("participant_id") or row.get("zernio_conversation_id"):
        return upsert_inbox_conversation(row), row
    return find_inbox_conversation(
        platform, account_id,
        platform_conversation_id=row.get("platform_conversation_id"),
    ), row


def _handle_comment_received(payload: dict) -> str:
    comment = payload.get("comment") or {}
    post = payload.get("post") or {}
    account = payload.get("account") or {}
    platform = comment.get("platform") or account.get("platform")
    if not platform or not comment.get("id"):
        return "ignored"

    post_payload = dict(post)
    post_payload.setdefault("platformPostId", comment.get("platformPostId"))
    post_uuid = upsert_inbox_post(_post_row(post_payload, account, platform))
    if not post_uuid:
        return "failed"
    resolve_inbox_post_link(
        post_uuid, platform, post_payload.get("platformPostId") or "",
    )

    row = _comment_row(
        comment, post_uuid, platform, account.get("username"),
        parent_id=comment.get("parentCommentId"),
    )
    upsert_inbox_comments([row])
    return "processed"


def _handle_message_event(payload: dict, event: str) -> str:
    conversation_uuid, conversation_row = _resolve_conversation(payload)
    if not conversation_uuid:
        return "deferred"

    message = payload.get("message") or {}
    if not message.get("id") and not message.get("platformMessageId"):
        return "ignored"

    row = _message_row(message, conversation_uuid)
    if event == "message.deleted":
        row["is_deleted"] = True
        row["delivery_status"] = "deleted"
    elif event == "message.edited":
        row["is_edited"] = True
    elif event in ("message.delivered", "message.read", "message.sent"):
        row["delivery_status"] = event.split(".", 1)[1]
    if not row.get("sent_at"):
        row["sent_at"] = payload.get("timestamp")

    upsert_inbox_messages([row])

    # Refresh the thread's summary line, and only ever push read-state in the unread
    # direction — is_read otherwise belongs to the admin.
    upsert_inbox_conversation({
        **conversation_row,
        "last_message": message.get("text"),
        "last_message_at": row.get("sent_at"),
    })
    if event == "message.received" and message.get("direction") == "incoming":
        mark_conversation_unread(conversation_uuid)
    return "processed"


def process_webhook_event(event_id: str) -> None:
    """
    Process one ledger row. Never raises — the receiver has already acked.

    Dispatches on the event name; anything unrecognised is marked `ignored` rather than
    failed, so it stops consuming retry budget.
    """
    try:
        row = get_webhook_event(event_id)
        if not row or row.get("status") == "processed":
            return
        attempts = (row.get("attempts") or 0) + 1
        event = row.get("event") or ""
        payload = row.get("payload") or {}

        if event == "comment.received":
            status = _handle_comment_received(payload)
        elif event in (
            "message.received", "message.sent", "message.edited",
            "message.deleted", "message.delivered", "message.read",
        ):
            status = _handle_message_event(payload, event)
        elif event in ("conversation.started", "reaction.received"):
            conversation_uuid, _ = _resolve_conversation(payload)
            status = "processed" if conversation_uuid else "deferred"
        else:
            status = "ignored"

        mark_webhook_event(event_id, status, attempts)
    except Exception as exc:
        logger.warning("Zernio webhook %s failed: %s", event_id, str(exc)[:300])
        try:
            mark_webhook_event(event_id, "failed", 99, str(exc)[:500])
        except Exception:
            pass


def accept_webhook(raw_body: bytes, payload: dict) -> tuple[bool, str | None]:
    """
    Record a verified delivery. Returns (is_new, event_id).

    Called from the request handler, so it must stay tiny — the ack budget is five
    seconds and the real work happens on a background task afterwards.
    """
    event_id = payload.get("id")
    if not event_id:
        return False, None
    is_new = record_webhook_event(
        event_id, payload.get("event") or "", payload, payload.get("timestamp"),
    )
    return is_new, event_id


def drain_inbox_ledger() -> int:
    """Re-run anything left pending/deferred/failed — the crash and out-of-order net."""
    processed = 0
    for row in claim_webhook_events(max_attempts=_MAX_EVENT_ATTEMPTS):
        process_webhook_event(row["event_id"])
        processed += 1
    return processed


# ── the sweep ─────────────────────────────────────────────────────────────────

def _sweep_conversations(full: bool) -> int:
    """
    Mirror DM threads, then their messages.

    The cursor is opaque and each page re-queries a live window, so results shift
    between requests — dedupe by id locally and treat hasMore as advisory. The
    incremental pass stops once a whole page is older than the lookback; the FULL pass
    walks to exhaustion, which is the only thing that can find a replayed pre-connect
    thread (those keep their original timestamps, so they never appear at the top).
    """
    max_pages = INBOX_FULL_MAX_PAGES if full else INBOX_MAX_PAGES
    cutoff = datetime.now(timezone.utc) - timedelta(days=INBOX_SWEEP_LOOKBACK_DAYS)
    state = get_inbox_conversation_state()
    seen: set[str] = set()
    written = 0
    cursor = None

    for _ in range(max_pages):
        params = {"limit": INBOX_PAGE_LIMIT, "sortOrder": "desc"}
        if cursor:
            params["cursor"] = cursor
        payload = zernio_get("/v1/inbox/conversations", params)
        items = payload.get("data") or []
        if not items:
            break

        page_is_old = True
        for item in items:
            conversation_id = item.get("id")
            if not conversation_id or conversation_id in seen:
                continue
            seen.add(conversation_id)
            updated = _parse_dt(item.get("updatedTime"))
            if updated and updated >= cutoff:
                page_is_old = False

            platform = item.get("platform")
            account = {
                "id": item.get("accountId"),
                "accountId": item.get("accountId"),
                "username": item.get("accountUsername"),
                "platform": platform,
            }
            row = _conversation_row(
                {
                    "id": conversation_id,
                    "participantId": item.get("participantId"),
                    "participantName": item.get("participantName"),
                    "participantPicture": item.get("participantPicture"),
                    "status": item.get("status"),
                    "instagramProfile": item.get("instagramProfile"),
                },
                account, platform,
            )
            row["last_message"] = item.get("lastMessage")
            row["last_message_at"] = item.get("updatedTime")
            row["unread_count"] = item.get("unreadCount") or 0
            row["permalink"] = item.get("url")
            conversation_uuid = upsert_inbox_conversation(row)
            if not conversation_uuid:
                continue
            written += 1

            known = state.get(conversation_id)
            moved = (
                not known
                or not known.get("last_message_at")
                or (_parse_dt(item.get("updatedTime")) or datetime.min.replace(tzinfo=timezone.utc))
                > (_parse_dt(known.get("last_message_at")) or datetime.min.replace(tzinfo=timezone.utc))
            )
            if moved or full:
                _sweep_messages(conversation_id, conversation_uuid)

        pagination = payload.get("pagination") or {}
        cursor = pagination.get("nextCursor")
        if not pagination.get("hasMore") or not cursor:
            break
        if page_is_old and not full:
            break
    return written


def _sweep_messages(zernio_conversation_id: str, conversation_uuid: str) -> None:
    """One page of the newest messages in a thread. Descending, so the newest are
    always covered; the nightly full pass is what backfills depth."""
    try:
        payload = zernio_get(
            f"/v1/inbox/conversations/{zernio_conversation_id}/messages",
            {"limit": INBOX_PAGE_LIMIT, "sortOrder": "desc"},
        )
    except Exception as exc:
        logger.warning(
            "Zernio messages fetch failed for %s: %s", zernio_conversation_id, str(exc)[:200],
        )
        return
    rows = []
    for message in payload.get("data") or payload.get("messages") or []:
        row = _message_row(message, conversation_uuid)
        if row.get("sent_at"):
            rows.append(row)
    upsert_inbox_messages(rows)


def _sweep_comments(full: bool) -> int:
    """
    Mirror posts that have comments, then the threads that changed.

    The sort is hard-coded: the vendor documents the cursor as coherent ONLY for
    sortBy=date + sortOrder=desc, and with any other sort the second page is unreliable.
    Keeping it out of the signature stops anyone parameterising it later.
    """
    max_pages = INBOX_FULL_MAX_PAGES if full else INBOX_MAX_PAGES
    since = datetime.now(timezone.utc) - timedelta(days=INBOX_SWEEP_LOOKBACK_DAYS)
    refresh_cutoff = datetime.now(timezone.utc) - timedelta(days=INBOX_THREAD_REFRESH_DAYS)
    state = get_inbox_post_state()
    seen: set[str] = set()
    written = 0
    cursor = None

    for _ in range(max_pages):
        params = {
            "limit": INBOX_PAGE_LIMIT,
            "sortBy": "date",
            "sortOrder": "desc",
            "since": since.isoformat(),
            "minComments": 1,
        }
        if cursor:
            params["cursor"] = cursor
        payload = zernio_get("/v1/inbox/comments", params)
        items = payload.get("data") or []
        if not items:
            break

        for item in items:
            key = item.get("id")
            platform = item.get("platform")
            if not key or key in seen or platform not in _COMMENT_PLATFORMS:
                continue
            seen.add(key)
            # Ad rows are a different comment thread on a different endpoint; organic
            # only for now.
            if item.get("isAd"):
                continue

            account = {
                "id": item.get("accountId"),
                "accountId": item.get("accountId"),
                "username": item.get("accountUsername"),
                "platform": platform,
            }
            post_row = _post_row(item, account, platform)
            post_row["platform_post_id"] = key
            post_uuid = upsert_inbox_post(post_row)
            if not post_uuid:
                continue
            written += 1
            resolve_inbox_post_link(post_uuid, platform, key)

            known = state.get(f"{platform}:{key}")
            posted_at = _parse_dt(item.get("createdTime"))
            count_moved = (
                not known or known.get("comment_count") != (item.get("commentCount") or 0)
            )
            # Re-open recent threads even when the count hasn't moved: an edit, a hide
            # or a deletion by a third party changes the thread without changing the
            # count, and none of them is evented.
            recent = bool(posted_at and posted_at >= refresh_cutoff)
            if count_moved or recent or full:
                _sweep_comment_thread(
                    key, item.get("accountId"), post_uuid, platform,
                    item.get("accountUsername"),
                )

        pagination = payload.get("pagination") or {}
        cursor = pagination.get("nextCursor")
        if not pagination.get("hasMore") or not cursor:
            break
    return written


def _sweep_comment_thread(platform_post_id: str, account_id: str | None,
                          post_uuid: str, platform: str,
                          account_username: str | None) -> None:
    if not account_id:
        return
    try:
        payload = zernio_get(
            f"/v1/inbox/comments/{platform_post_id}",
            {"accountId": account_id, "limit": INBOX_PAGE_LIMIT},
        )
    except Exception as exc:
        logger.warning(
            "Zernio comment thread fetch failed for %s: %s", platform_post_id, str(exc)[:200],
        )
        return
    rows = _flatten_comments(
        payload.get("data") or payload.get("comments") or [],
        post_uuid, platform, account_username,
    )
    upsert_inbox_comments([r for r in rows if r.get("platform_comment_id") and r.get("commented_at")])


def run_inbox_sweep(full: bool = False) -> None:
    """
    Reconcile the mirror against Zernio.

    Incremental (every 15 minutes) keeps up with the present; `full` walks every listing
    to exhaustion and is what finds Meta's replayed pre-connect history — run it nightly
    and immediately after connecting an account. Never raises.
    """
    name = "The Curve (inbox, zernio)"
    total = 0
    try:
        drained = drain_inbox_ledger()
        if drained:
            logger.info("Zernio inbox: re-processed %d pending webhook events", drained)
        total += _sweep_comments(full)
        total += _sweep_conversations(full)
        if full:
            pruned = prune_webhook_events(INBOX_EVENT_RETENTION_DAYS)
            if pruned:
                logger.info("Zernio inbox: pruned %d old webhook events", pruned)
        logger.info("Zernio inbox sweep (%s): %d threads touched", "full" if full else "incremental", total)
        log_source_run(name, _RUN_CATEGORY, "ok", total)
    except Exception as exc:
        logger.warning("Zernio inbox sweep failed: %s", str(exc)[:300])
        log_source_run(name, _RUN_CATEGORY, "error", 0, str(exc)[:500])


def run_inbox_sweep_full() -> None:
    run_inbox_sweep(full=True)


# ── webhook registration ──────────────────────────────────────────────────────

def register_webhook(public_url: str | None = None) -> dict:
    """
    Create or update this service's webhook subscription (idempotent).

    Zernio has no dashboard field for this — subscriptions are API objects — and the
    secret sent at registration has to be the same one verify_signature() checks with,
    which only this process knows. So registration belongs in code, next to the
    receiver, not in a runbook step performed by hand against a copy-pasted secret.

    Matches on URL: if a subscription already points at our endpoint it is updated in
    place, so re-running after a secret rotation or an event-list change is safe.
    """
    base = (public_url or PUBLIC_BASE_URL or "").rstrip("/")
    if not base:
        return {"status": "error", "detail": "PUBLIC_BASE_URL not set"}
    if not ZERNIO_WEBHOOK_SECRET:
        return {"status": "error", "detail": "ZERNIO_WEBHOOK_SECRET not set"}

    target = f"{base}/webhooks/zernio"
    body = {
        "name": "Curve pipeline inbox",
        "url": target,
        "secret": ZERNIO_WEBHOOK_SECRET,
        "events": list(INBOX_WEBHOOK_EVENTS),
        "isActive": True,
    }
    try:
        existing = zernio_request("GET", "/v1/webhooks/settings")
        match = next(
            (w for w in existing.get("webhooks") or [] if w.get("url") == target), None,
        )
        if match:
            zernio_request("PUT", f"/v1/webhooks/settings/{match['_id']}", json=body)
            return {"status": "updated", "id": match["_id"], "url": target}
        created = zernio_request("POST", "/v1/webhooks/settings", json=body)
        webhook = created.get("webhook") or created
        return {"status": "created", "id": webhook.get("_id"), "url": target}
    except Exception as exc:
        logger.warning("Zernio webhook registration failed: %s", str(exc)[:300])
        return {"status": "error", "detail": str(exc)[:300]}
