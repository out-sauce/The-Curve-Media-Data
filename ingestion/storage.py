"""
Supabase storage layer. Upserts articles using guid as the conflict key
so re-runs never create duplicates.
"""

import logging
from datetime import date, datetime, time as _time, timedelta, timezone
from typing import Any

import httpx
from supabase import create_client, Client

from config import (
    SUPABASE_URL,
    SUPABASE_SERVICE_ROLE_KEY,
    COMPETITOR_THUMBNAILS_BUCKET,
)

logger = logging.getLogger(__name__)

TABLE = "news_articles"

_client: Client | None = None


def get_client() -> Client:
    global _client
    if _client is None:
        _client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
    return _client


def upsert_articles(articles: list[dict[str, Any]]) -> int:
    """
    Insert new articles; skip any whose guid already exists.
    Returns number of rows actually inserted.
    """
    if not articles:
        return 0

    client = get_client()

    # Supabase upsert with on_conflict=guid ignores duplicates
    response = (
        client.table(TABLE)
        .upsert(articles, on_conflict="guid", ignore_duplicates=True)
        .execute()
    )

    inserted = len(response.data) if response.data else 0
    logger.info("Upserted %d new articles into %s", inserted, TABLE)
    return inserted


VALID_STATUSES = {"new", "included", "excluded", "accepted", "briefed", "published"}


def get_sources(source_type: str | None = None, enabled_only: bool = True) -> list[dict]:
    """Return configured sources from the DB. Filters by type and enabled flag."""
    client = get_client()
    query = client.table("sources").select("id, name, url, category, source_type, enabled")
    if enabled_only:
        query = query.eq("enabled", True)
    if source_type:
        query = query.eq("source_type", source_type)
    response = query.order("name").execute()
    return response.data or []


def get_social_sources() -> list[dict]:
    """
    Return enabled Instagram/TikTok sources for the scan stage.
    Includes `handle` (the scrape target — username without @); for social
    sources `url` is the display profile URL, not the scrape target.
    """
    client = get_client()
    response = (
        client.table("sources")
        .select("id, name, handle, url, category, source_type, enabled")
        .eq("enabled", True)
        .in_("source_type", ["instagram", "tiktok"])
        .order("name")
        .execute()
    )
    return response.data or []


def get_competitors(competitor_id: str | None = None) -> list[dict]:
    """
    Return tracked competitors to scrape. Each competitor is ONE brand row that may
    carry an Instagram channel, a TikTok channel, or both. The admin app seeds the
    per-channel handles/urls plus the is_self ("The Curve") flag; this run reads
    those and writes the per-platform stats + posts back in.

    Pass `competitor_id` to scrape a single row (a manual Refresh from the admin
    card); omit it for the daily job that refreshes everyone. The per-channel
    handle is the scrape target (username without @); when absent it is parsed off
    the matching *_url.
    """
    client = get_client()
    query = client.table("competitors").select(
        "id, is_self, display_name, "
        "instagram_url, tiktok_url, instagram_handle, tiktok_handle, "
        "linkedin_url, linkedin_handle, "
        "youtube_url, youtube_handle"
    )
    if competitor_id:
        query = query.eq("id", competitor_id)
    response = query.order("created_at").execute()
    return response.data or []


# Browser-like headers — IG/TikTok cover CDNs commonly 403 a bare client.
_IMAGE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.google.com/",
    "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
}


# The content-type header alone is not enough: nine pre-guard reel videos
# reached the bucket as .jpg and rendered as broken images in the Admin
# (repaired 2026-08-12 by re-encoding a frame of each in place), and a CDN can
# mislabel a payload either way — so the bytes themselves get the final say.
def _looks_like_image(data: bytes) -> bool:
    if data.startswith((b"\xff\xd8", b"\x89PNG", b"GIF8")):
        return True
    if data.startswith(b"RIFF") and data[8:12] == b"WEBP":
        return True
    # ISO-BMFF (ftyp) covers both avif images and mp4 video — admit only the
    # image brands so a reel's mp4 can't slip through.
    if data[4:8] == b"ftyp" and data[8:12] in (b"avif", b"avis", b"heic", b"heix", b"mif1"):
        return True
    return False


def store_competitor_image(url: str | None, path: str) -> str | None:
    """
    Download a competitor avatar/thumbnail from its (expiring) CDN URL and re-upload
    it to the public `competitor-thumbnails` bucket under a deterministic `path`, so
    re-runs overwrite the same object (no expiry, no dupes). Returns the stable
    public URL, or None on any failure (best-effort — never raises).
    """
    if not url:
        return None
    try:
        resp = httpx.get(url, headers=_IMAGE_HEADERS, timeout=30, follow_redirects=True)
        resp.raise_for_status()
        data = resp.content
        if not data:
            return None
        content_type = (resp.headers.get("content-type") or "image/jpeg").split(";")[0].strip()
        if not content_type.startswith("image/"):
            # e.g. a Zernio reel whose media URL is the mp4 itself — storing video
            # bytes under a .jpg path renders as a broken image, worse than nothing.
            logger.warning("Skipping non-image content (%s) for %s", content_type, path)
            return None
        if not _looks_like_image(data):
            # Header said image/* but the bytes disagree (mislabelled mp4/HTML
            # error page) — same broken-image outcome, so same skip.
            logger.warning("Skipping mislabelled non-image bytes (%s) for %s", content_type, path)
            return None
    except Exception as exc:
        logger.warning("Could not download competitor image %s: %s", url, str(exc)[:200])
        return None

    try:
        client = get_client()
        client.storage.from_(COMPETITOR_THUMBNAILS_BUCKET).upload(
            path,
            data,
            {"content-type": content_type, "upsert": "true"},
        )
        public_url = client.storage.from_(COMPETITOR_THUMBNAILS_BUCKET).get_public_url(path)
        return public_url or None
    except Exception as exc:
        logger.warning("Could not upload competitor image to %s: %s", path, str(exc)[:200])
        return None


def get_existing_post_thumbnails(
    competitor_id: str, post_ids: list[str]
) -> dict[str, str | None]:
    """
    Return {post_id: thumbnail_url} for already-stored competitor_posts, so a failed
    re-fetch of an existing post can preserve its prior (persisted) thumbnail.
    Brand-new posts are simply absent from the map.
    """
    if not post_ids:
        return {}
    client = get_client()
    try:
        response = (
            client.table("competitor_posts")
            .select("post_id, thumbnail_url")
            .eq("competitor_id", competitor_id)
            .in_("post_id", post_ids)
            .execute()
        )
    except Exception as exc:
        logger.warning("Could not read existing post thumbnails: %s", str(exc)[:200])
        return {}
    return {row["post_id"]: row.get("thumbnail_url") for row in (response.data or [])}


def get_existing_content_stats_thumbnails(
    platform: str, post_ids: list[str]
) -> dict[str, str | None]:
    """
    Return {post_id: thumbnail_url} for already-stored content_stats rows, so the
    hourly Zernio run only downloads/persists an image for posts that don't have
    one yet — the 90-day content_stats window would otherwise re-fetch every post's
    image every hour. Returns {} pre-migration-035 (selecting the missing column
    errors) — harmless, the write path filters the field out then too.
    """
    if not post_ids:
        return {}
    client = get_client()
    try:
        response = (
            client.table("content_stats")
            .select("post_id, thumbnail_url")
            .eq("platform", platform)
            .in_("post_id", post_ids)
            .execute()
        )
    except Exception as exc:
        logger.warning("Could not read existing content_stats thumbnails: %s", str(exc)[:200])
        return {}
    return {row["post_id"]: row.get("thumbnail_url") for row in (response.data or [])}


def get_existing_post_transcripts(
    competitor_id: str, post_ids: list[str]
) -> dict[str, str]:
    """
    Return {post_id: transcript} for already-stored competitor_posts that have a
    non-empty transcript, so we can skip re-fetching them and preserve the prior
    value if a re-fetch fails. Posts without a stored transcript are absent.
    """
    if not post_ids:
        return {}
    client = get_client()
    try:
        response = (
            client.table("competitor_posts")
            .select("post_id, transcript")
            .eq("competitor_id", competitor_id)
            .in_("post_id", post_ids)
            .execute()
        )
    except Exception as exc:
        logger.warning("Could not read existing post transcripts: %s", str(exc)[:200])
        return {}
    return {
        row["post_id"]: row["transcript"]
        for row in (response.data or [])
        if (row.get("transcript") or "").strip()
    }


def update_competitor_stats(competitor_id: str, fields: dict[str, Any]) -> None:
    """
    Write scraped profile stats back onto the competitors row (skips None values
    so a partial scrape never blanks existing data). Callers include
    refresh_status='idle' + last_refreshed_at on success so the admin card stops
    polling.
    """
    payload = {k: v for k, v in fields.items() if v is not None}
    if not payload:
        return
    client = get_client()
    client.table("competitors").update(payload).eq("id", competitor_id).execute()


def upsert_competitor_posts(rows: list[dict[str, Any]]) -> int:
    """
    Upsert competitor posts keyed by (competitor_id, post_id) — the admin
    table's unique key — refreshing engagement counts on each run. Returns
    number of rows written.
    """
    if not rows:
        return 0
    client = get_client()
    response = (
        client.table("competitor_posts")
        .upsert(rows, on_conflict="competitor_id,post_id", ignore_duplicates=False)
        .execute()
    )
    return len(response.data) if response.data else 0


# Fields the Apify scrape can fill on content_stats. shares/saves come from TikTok
# only (Instagram's public scrape omits them); caption/hashtags/duration_sec/
# engagement_rate need migration 025's columns. Keys absent from the live table are
# dropped, and None values are skipped on update, so a scrape that lacks a field
# never clobbers what the admin/analytics populated (reach/downloads/watch time/…).
# reach/impressions/accounts_engaged/total_interactions/platform_specific are
# owner-only Insights the Apify scrape never fills — real values only come from an
# authenticated source (Zernio), see migrations 025 and 033.
# follows/avg_watch_time_ms/total_watch_time_ms (migration 037) are Zernio-only too:
# Instagram feed-or-story follows and Reels watch time, which Outstand never returned.
_CONTENT_STATS_FIELDS = (
    "post_url", "posted_at", "views", "likes", "comments", "shares", "saves",
    "caption", "hashtags", "duration_sec",
    "engagement_rate", "engagement_reach", "engagement_audience", "engagement_on_reach",
    "reach", "impressions", "accounts_engaged", "total_interactions", "platform_specific",
    "transcript", "thumbnail_url",
    "follows", "avg_watch_time_ms", "total_watch_time_ms",
    "replies", "sticker_taps",
)

_content_stats_columns: set[str] | None = None


def _content_stats_column_set() -> set[str]:
    """Discover content_stats columns once (cached) so we only write keys that exist."""
    global _content_stats_columns
    if _content_stats_columns is None:
        client = get_client()
        sample = client.table("content_stats").select("*").limit(1).execute()
        if sample.data:
            _content_stats_columns = set(sample.data[0].keys())
        else:
            # Empty table — fall back to base columns (no enrichment until migrated).
            _content_stats_columns = {
                "platform", "post_id", "post_url", "views", "likes", "comments",
                "shares", "saves", "downloads", "reach", "opens", "clicks",
                "calendar_item_id", "stats_synced_at",
            }
    return _content_stats_columns


def upsert_self_content_stats(rows: list[dict[str, Any]]) -> int:
    """
    Upsert the is_self ("The Curve") competitor's posts into content_stats, deduped
    on (platform, post_id) with no source tag. Mirrors the admin's canonical
    lookup-then-update-else-insert: find the existing row by (platform, post_id) and
    update only the scraped fields, otherwise insert a fresh row (calendar_item_id=
    null). On update, None values are skipped so an absent field (e.g. Instagram
    shares/saves) never clobbers an existing value. Returns the number of rows
    written. Best-effort per row — one failure never aborts the rest.

    calendar_item_id is handled explicitly (never via _CONTENT_STATS_FIELDS): a row
    may carry it (the guest-post scrape, ingestion/guest_posts.py — its stats can
    never auto-link to The Curve's own scrape, so the link travels with the write).
    It lands on insert, and on update only fills a NULL — an existing admin/human
    link is never overwritten. Rows without the key behave exactly as before.
    """
    if not rows:
        return 0
    from datetime import datetime, timezone
    now_iso = datetime.now(timezone.utc).isoformat()
    client = get_client()
    columns = _content_stats_column_set()
    allowed = [f for f in _CONTENT_STATS_FIELDS if f in columns]
    written = 0
    for row in rows:
        platform = row.get("platform")
        post_id = row.get("post_id")
        if not platform or not post_id:
            continue
        scraped = {k: row.get(k) for k in allowed}
        calendar_item_id = row.get("calendar_item_id")
        try:
            existing = (
                client.table("content_stats")
                .select("id, calendar_item_id")
                .eq("platform", platform)
                .eq("post_id", post_id)
                .limit(1)
                .execute()
            )
            if existing.data:
                # Skip None so a missing field never blanks an existing value.
                changed = {k: v for k, v in scraped.items() if v is not None}
                if calendar_item_id and not existing.data[0].get("calendar_item_id"):
                    changed["calendar_item_id"] = calendar_item_id
                client.table("content_stats").update(
                    {**changed, "stats_synced_at": now_iso, "updated_at": now_iso}
                ).eq("id", existing.data[0]["id"]).execute()
            else:
                client.table("content_stats").insert({
                    "platform": platform,
                    "post_id": post_id,
                    "calendar_item_id": calendar_item_id,
                    "stats_synced_at": now_iso,
                    **scraped,
                }).execute()
            written += 1
        except Exception as exc:
            logger.warning(
                "Could not upsert content_stats row (%s/%s): %s",
                platform, post_id, str(exc)[:200],
            )
    logger.info("Upserted %d posts into content_stats", written)
    return written


# ── The Curve's own channels (is_self) → follower_snapshots time series ────────
# follower_snapshots.social_account_id is an FK to social_accounts (The Curve's own
# channels), NOT competitors. We map the scraped platform → that social_accounts row
# and append/refresh a daily snapshot so the admin app can chart follower growth.

def get_self_social_accounts() -> dict[str, str]:
    """Return {platform: social_account_id} for The Curve's own IG/TikTok/LinkedIn/YouTube rows."""
    client = get_client()
    response = (
        client.table("social_accounts")
        .select("id, platform")
        .in_("platform", ["instagram", "tiktok", "linkedin", "youtube"])
        .execute()
    )
    accounts: dict[str, str] = {}
    for row in response.data or []:
        accounts.setdefault(row["platform"], row["id"])
    return accounts


def get_self_competitor_id() -> str | None:
    """Return the id of the is_self ('The Curve') competitors row, if any — used by
    ingestion/zernio.py to write instagram_* stat columns / competitor_posts rows
    that Apify's self-Instagram scrape used to own."""
    client = get_client()
    response = client.table("competitors").select("id").eq("is_self", True).limit(1).execute()
    return response.data[0]["id"] if response.data else None


def get_zernio_connected_accounts() -> dict[str, dict[str, Any]]:
    """
    Return {platform: {social_account_id, zernio_account_id}} for self channels the
    Admin app has connected through Zernio (account_id set, connected=true — the same
    generic OAuth columns the Xero integration uses).

    account_id holds whatever the CURRENT provider's account id is: the admin's connect
    callback overwrites it, so the moment a channel is reconnected through Zernio this
    returns a Zernio 24-hex ObjectId instead of the old Outstand id. That is why the
    pipeline and the admin have to cut over in the same window — there is deliberately
    no second column, so there is never a question of which id is live.

    There is no import watermark any more: Zernio syncs each account's external posts
    on its own background cycle (~90 min, ~12 months retained) rather than billing us
    per imported post, so nothing here needs to be metered.
    """
    client = get_client()
    response = (
        client.table("social_accounts")
        .select("id, platform, account_id, connected")
        .in_("platform", ["instagram", "tiktok", "linkedin", "youtube"])
        .eq("connected", True)
        .not_.is_("account_id", "null")
        .execute()
    )
    accounts: dict[str, dict[str, Any]] = {}
    for row in response.data or []:
        accounts.setdefault(row["platform"], {
            "social_account_id": row["id"],
            "zernio_account_id": row["account_id"],
        })
    return accounts


def get_follower_snapshot_history(platform: str) -> list[tuple[str, int]]:
    """
    [(recorded_at, follower_count)] for a platform, oldest first, nulls dropped.

    Backs the followers-at-time denominator for engagement_audience — a post must be
    measured against the audience it had when it published, not today's. Paginated
    because PostgREST caps a response at 1000 rows and this table grows daily.
    """
    client = get_client()
    history: list[tuple[str, int]] = []
    page_size = 1000
    offset = 0
    while True:
        response = (
            client.table("follower_snapshots")
            .select("recorded_at, follower_count")
            .eq("platform", platform)
            .order("recorded_at")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        rows = response.data or []
        history.extend(
            (row["recorded_at"], row["follower_count"])
            for row in rows
            if row.get("follower_count") and row.get("recorded_at")
        )
        if len(rows) < page_size:
            break
        offset += page_size
    return history


# Keys of Zernio's account-insights metrics block → follower_snapshots `*_30d`
# columns (migration 034). Trailing ~30-day rolling totals, NOT daily activity —
# adjacent daily rows overlap by ~29 days, so never SUM or diff them.
_ENGAGEMENT_30D_KEYS = (
    "views", "likes", "comments", "shares", "saves",
    "reach", "accounts_engaged", "total_interactions",
)


def upsert_follower_snapshot(
    social_account_id: str, platform: str, follower_count: int,
    engagement_30d: dict | None = None,
    day: date | None = None,
    insert_only: bool = False,
) -> bool:
    """
    Record one follower snapshot for a self channel, one row per UTC day: update that
    day's row in place if present, else insert. Builds a clean daily growth series
    across re-runs (manual refreshes won't create duplicate same-day rows). Returns
    True when a row was written.

    engagement_30d, when given (Zernio-connected channels only), is the account
    insights metrics block; its values land in the *_30d columns. Omitted keys are not
    written, so an Apify-flow update never blanks a Zernio-written value.

    day + insert_only exist for the historical backfill off Zernio's follower-history
    series. `day` anchors the row to a past UTC date instead of today; `insert_only`
    makes an existing row for that day win. Both matter because this series is not all
    machine-written — much of it was typed in by hand, monthly, going back to 2021, and
    it is the denominator behind every engagement_audience value. A backfill that
    silently replaced a hand-entered count with a vendor's reconstruction would rewrite
    history no one could audit, so the backfill fills gaps only.
    """
    client = get_client()
    if day is not None:
        day_start = datetime.combine(day, _time.min, tzinfo=timezone.utc)
        recorded_at = day_start
    else:
        recorded_at = datetime.now(timezone.utc)
        day_start = recorded_at.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end = day_start + timedelta(days=1)

    values: dict = {"follower_count": follower_count, "recorded_at": recorded_at.isoformat()}
    for key in _ENGAGEMENT_30D_KEYS:
        if engagement_30d and engagement_30d.get(key) is not None:
            values[f"{key}_30d"] = engagement_30d[key]
    existing = (
        client.table("follower_snapshots")
        .select("id")
        .eq("social_account_id", social_account_id)
        .eq("platform", platform)
        .gte("recorded_at", day_start.isoformat())
        .lt("recorded_at", day_end.isoformat())
        .limit(1)
        .execute()
    )
    if existing.data:
        if insert_only:
            return False
        client.table("follower_snapshots").update(values).eq("id", existing.data[0]["id"]).execute()
    else:
        client.table("follower_snapshots").insert({
            "social_account_id": social_account_id,
            "platform": platform,
            **values,
        }).execute()
    return True


def get_recent_story_rows(platform: str, since_iso: str) -> list[dict[str, Any]]:
    """
    Stored story rows posted since `since_iso`, with their provenance blob.

    Backs the story "settle" pass. A story's metrics are only final once it expires, so
    whatever the last hourly poll captured while it was live is systematically an
    undercount. After expiry Zernio can still serve the final numbers from the
    story_insights webhook Meta pushed it (source="cached"), so we re-ask for stories
    that have dropped off the active list — and stop asking once we've got a cached
    (i.e. final) read.
    """
    client = get_client()
    response = (
        client.table("content_stats")
        .select("post_id, posted_at, platform_specific")
        .eq("platform", platform)
        .gte("posted_at", since_iso)
        .execute()
    )
    return response.data or []


def upsert_audience_demographics(rows: list[dict[str, Any]]) -> int:
    """
    Write one audience_demographics row per (dimension, bucket) for a snapshot day,
    replacing that day's set for the account+platform+dimension so a re-run inside the
    same day never accumulates duplicates.

    The table is admin-owned and predates this pipeline; its shape
    (social_account_id, platform, dimension, bucket, value, value_type, snapshot_date)
    already matches Zernio's demographics payload exactly, so nothing new is created
    here. Outstand had no demographics endpoint at all — /audience, /insights and
    /demographics were all live-probed to 404 — so this table has had no writer until
    now.

    value_type is 'count': Meta reports absolute follower counts per bucket, capped at
    the top 45 entries per dimension, so the buckets do NOT sum to the follower total
    and a percentage computed here would be wrong. Let the reader do that maths against
    the visible buckets if it wants shares.

    Written as a keyed upsert on the table's own natural key, NOT delete-then-insert:
    this table also holds hand-entered and podcast-sourced rows, and a delete scoped to
    a whole (account, platform, dimension, day) would take those with it. An upsert only
    touches the exact buckets we are reporting.

    Best-effort — a failure never aborts the caller (the table is admin-owned, so a
    missing column here must not take the whole sweep down with it).
    """
    if not rows:
        return 0
    client = get_client()
    try:
        client.table("audience_demographics").upsert(
            rows,
            on_conflict="platform,dimension,bucket,snapshot_date,social_account_id",
        ).execute()
        return len(rows)
    except Exception as exc:
        logger.warning("audience_demographics write failed: %s", str(exc)[:300])
        return 0


def update_social_account_follower_count(
    social_account_id: str, follower_count: int
) -> None:
    """Refresh the 'current' follower_count on a social_accounts row."""
    client = get_client()
    client.table("social_accounts").update(
        {"follower_count": follower_count}
    ).eq("id", social_account_id).execute()


# ── inbox (comments + DMs, migration 039) ─────────────────────────────────────
# Every row here is written by the pipeline (webhook receiver + reconciliation sweep).
# Three things belong to the Admin app and must never be overwritten from this side:
# inbox_conversations.{is_read,read_at,read_by}, inbox_comments.{handled_at,handled_by},
# and any row it inserted optimistically for its own outbound reply (source='admin').
# The upserts below achieve that simply by never naming those columns.

def record_webhook_event(event_id: str, event: str, payload: dict,
                         event_time: str | None) -> bool:
    """
    Durably record one webhook delivery. Returns True if it is NEW.

    The primary key does the deduping: Zernio delivers at-least-once, so the same
    event_id can arrive several times, and an insert that conflicts means we have
    already accepted it and must not schedule the work again. Recording BEFORE we ack
    also means a crash between the ack and the write loses nothing — the event is on
    disk and drain_inbox_ledger picks it up.
    """
    client = get_client()
    response = (
        client.table("inbox_webhook_events")
        .upsert(
            {
                "event_id": event_id,
                "event": event,
                "payload": payload,
                "event_time": event_time,
            },
            on_conflict="event_id",
            ignore_duplicates=True,
        )
        .execute()
    )
    return bool(response.data)


def get_webhook_event(event_id: str) -> dict[str, Any] | None:
    client = get_client()
    response = (
        client.table("inbox_webhook_events")
        .select("event_id, event, payload, attempts, status")
        .eq("event_id", event_id)
        .limit(1)
        .execute()
    )
    return response.data[0] if response.data else None


def claim_webhook_events(limit: int = 100, max_attempts: int = 5) -> list[dict[str, Any]]:
    """Unfinished events, oldest first — anything the receiver never got to finish."""
    client = get_client()
    response = (
        client.table("inbox_webhook_events")
        .select("event_id, event, payload, attempts, status")
        .in_("status", ["pending", "deferred", "failed"])
        .lt("attempts", max_attempts)
        .order("received_at")
        .limit(limit)
        .execute()
    )
    return response.data or []


def mark_webhook_event(event_id: str, status: str, attempts: int,
                       error: str | None = None) -> None:
    client = get_client()
    values: dict[str, Any] = {"status": status, "attempts": attempts, "error": error}
    if status in ("processed", "ignored"):
        values["processed_at"] = datetime.now(timezone.utc).isoformat()
    client.table("inbox_webhook_events").update(values).eq("event_id", event_id).execute()


def prune_webhook_events(retention_days: int = 30) -> int:
    """Drop processed/ignored events older than the retention window."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=retention_days)).isoformat()
    client = get_client()
    response = (
        client.table("inbox_webhook_events")
        .delete()
        .in_("status", ["processed", "ignored"])
        .lt("received_at", cutoff)
        .execute()
    )
    return len(response.data or [])


def find_inbox_conversation(platform: str, account_id: str,
                            participant_id: str | None = None,
                            zernio_conversation_id: str | None = None,
                            platform_conversation_id: str | None = None) -> str | None:
    """
    Resolve a conversation to our uuid, trying each identity in turn.

    Order matters: the natural key first (both the list endpoint and the webhooks carry
    all three parts in practice), then either vendor id. Returns None when nothing
    matches, which the caller treats as "defer this event" rather than "create a
    conversation from a fragment".
    """
    client = get_client()
    if participant_id:
        response = (
            client.table("inbox_conversations")
            .select("id")
            .eq("platform", platform)
            .eq("account_id", account_id)
            .eq("participant_id", participant_id)
            .limit(1)
            .execute()
        )
        if response.data:
            return response.data[0]["id"]
    for column, value in (
        ("zernio_conversation_id", zernio_conversation_id),
        ("platform_conversation_id", platform_conversation_id),
    ):
        if not value:
            continue
        response = (
            client.table("inbox_conversations").select("id").eq(column, value).limit(1).execute()
        )
        if response.data:
            return response.data[0]["id"]
    return None


def upsert_inbox_conversation(row: dict[str, Any],
                              known_id: str | None = None) -> str | None:
    """
    Insert or refresh one conversation, returning our uuid.

    Skip-None on update, so a sparse webhook payload never blanks what a full sweep
    established. is_read/read_at/read_by are never named here — they are the admin's.

    Pass `known_id` when the caller already knows our uuid. The sweep does — it loads
    every conversation up front via get_inbox_conversation_state() — so re-resolving each
    thread costs up to THREE extra SELECTs for nothing, about 1,500 wasted round trips
    across a 500-thread account.
    """
    client = get_client()
    existing = known_id or find_inbox_conversation(
        row["platform"], row["account_id"],
        row.get("participant_id"),
        row.get("zernio_conversation_id"),
        row.get("platform_conversation_id"),
    )
    values = {k: v for k, v in row.items() if v is not None}
    values["updated_at"] = datetime.now(timezone.utc).isoformat()
    try:
        if existing:
            client.table("inbox_conversations").update(values).eq("id", existing).execute()
            return existing
        response = client.table("inbox_conversations").insert(values).execute()
        return response.data[0]["id"] if response.data else None
    except Exception as exc:
        logger.warning("Could not upsert inbox conversation: %s", str(exc)[:200])
        return None


def mark_conversation_unread(conversation_id: str) -> None:
    """An incoming message makes a thread unread again — the one time the pipeline
    touches the admin's read state, and only ever in the false direction."""
    client = get_client()
    client.table("inbox_conversations").update(
        {"is_read": False, "read_at": None, "read_by": None}
    ).eq("id", conversation_id).execute()


def _key_buckets(rows: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    """
    Group rows by their exact key set.

    PostgREST rejects a batch whose objects do not all carry the same keys, and every row
    here is built with skip-None — so an ABSENT key means "leave that column alone", never
    "write NULL". Padding a batch out to a common key set would therefore blank real data.
    Bucketing preserves skip-None exactly while still collapsing hundreds of round trips
    into a handful, because rows from one sweep nearly always share a shape.
    """
    buckets: dict[frozenset, list[dict[str, Any]]] = {}
    for row in rows:
        buckets.setdefault(frozenset(row.keys()), []).append(row)
    return list(buckets.values())


def _chunks(items: list[Any], size: int):
    for i in range(0, len(items), size):
        yield items[i:i + size]


# Fields that can legitimately change after a message is first mirrored. Everything else
# about a message is immutable, so an unchanged row needs no write at all — which is what
# makes re-sweeping an already-mirrored inbox nearly free.
_MESSAGE_MUTABLE = ("body", "delivery_status", "is_edited", "is_deleted",
                    "sender_name", "attachments")


def _upsert_messages_individually(rows: list[dict[str, Any]]) -> int:
    """
    Per-row fallback, and the only path for a message with no vendor id — that one has to
    be matched on (conversation_id, platform_message_id) instead.
    """
    if not rows:
        return 0
    client = get_client()
    now = datetime.now(timezone.utc).isoformat()
    written = 0
    for values in rows:
        try:
            existing = None
            if values.get("zernio_message_id"):
                response = (
                    client.table("inbox_messages").select("id")
                    .eq("zernio_message_id", values["zernio_message_id"])
                    .limit(1).execute()
                )
                existing = response.data[0]["id"] if response.data else None
            if not existing and values.get("platform_message_id"):
                response = (
                    client.table("inbox_messages").select("id")
                    .eq("conversation_id", values["conversation_id"])
                    .eq("platform_message_id", values["platform_message_id"])
                    .limit(1).execute()
                )
                existing = response.data[0]["id"] if response.data else None
            payload = {**values, "updated_at": now}
            if existing:
                client.table("inbox_messages").update(payload).eq("id", existing).execute()
            else:
                client.table("inbox_messages").insert(payload).execute()
            written += 1
        except Exception as exc:
            logger.warning("Could not upsert inbox message: %s", str(exc)[:200])
    return written


def upsert_inbox_messages(rows: list[dict[str, Any]]) -> int:
    """
    Insert or refresh messages, deduped on the vendor id and then the platform id.

    NOT a PostgREST upsert. Both unique indexes on this table are PARTIAL
    (`WHERE ... IS NOT NULL`), and Postgres will not infer a partial index for ON CONFLICT
    unless the statement repeats its predicate — which PostgREST has no way to emit
    (verified: 42P10, "no unique or exclusion constraint matching the ON CONFLICT
    specification"). So the batching is done by hand: one grouped SELECT, one bulk INSERT
    for what is genuinely new, and an UPDATE only for a row whose mutable fields actually
    moved. Re-sweeping an already-mirrored inbox therefore costs a few SELECTs and no
    writes at all, rather than the two round trips per message this used to spend — which
    is what exhausted Supabase connections ("Server disconnected") on the 500-thread
    backfill and silently dropped rows.

    An admin-sent message is already in the table (written optimistically the moment
    Zernio accepted it), so the matching row is UPDATEd rather than duplicated — which is
    why both id columns are unique-indexed. source/sent_by are never written from here, so
    an admin row keeps its authorship.
    """
    if not rows:
        return 0
    client = get_client()
    now = datetime.now(timezone.utc).isoformat()

    prepared = []
    for row in rows:
        values = {k: v for k, v in row.items() if v is not None}
        values.pop("source", None)
        values.pop("sent_by", None)
        prepared.append(values)

    keyed = [v for v in prepared if v.get("zernio_message_id")]
    unkeyed = [v for v in prepared if not v.get("zernio_message_id")]

    existing_rows: dict[str, dict[str, Any]] = {}
    columns = "id, zernio_message_id, " + ", ".join(_MESSAGE_MUTABLE)
    for chunk in _chunks([v["zernio_message_id"] for v in keyed], 100):
        try:
            response = (
                client.table("inbox_messages").select(columns)
                .in_("zernio_message_id", chunk).execute()
            )
            for found in response.data or []:
                existing_rows[found["zernio_message_id"]] = found
        except Exception as exc:
            logger.warning("Could not read existing inbox messages: %s", str(exc)[:200])
            return _upsert_messages_individually(prepared)

    fresh: list[dict[str, Any]] = []
    changed: list[tuple[str, dict[str, Any]]] = []
    for values in keyed:
        found = existing_rows.get(values["zernio_message_id"])
        if not found:
            fresh.append({**values, "updated_at": now})
        elif any(values.get(f) != found.get(f) for f in _MESSAGE_MUTABLE if f in values):
            changed.append((found["id"], {**values, "updated_at": now}))

    written = 0
    for bucket in _key_buckets(fresh):
        try:
            client.table("inbox_messages").insert(bucket).execute()
            written += len(bucket)
        except Exception as exc:
            logger.warning(
                "Batch message insert failed for %d rows, retrying individually: %s",
                len(bucket), str(exc)[:200],
            )
            written += _upsert_messages_individually(bucket)

    for row_id, values in changed:
        try:
            client.table("inbox_messages").update(values).eq("id", row_id).execute()
            written += 1
        except Exception as exc:
            logger.warning("Could not update inbox message: %s", str(exc)[:200])

    return written + _upsert_messages_individually(unkeyed)


def upsert_inbox_post(row: dict[str, Any]) -> str | None:
    """Insert or refresh one commented-on post, returning our uuid."""
    client = get_client()
    values = {k: v for k, v in row.items() if v is not None}
    values["updated_at"] = datetime.now(timezone.utc).isoformat()
    try:
        existing = (
            client.table("inbox_posts")
            .select("id, content_stats_id")
            .eq("platform", row["platform"])
            .eq("platform_post_id", row["platform_post_id"])
            .limit(1)
            .execute()
        )
        if existing.data:
            post_uuid = existing.data[0]["id"]
            # Never overwrite a resolved link — see resolve_inbox_post_link.
            values.pop("content_stats_id", None)
            client.table("inbox_posts").update(values).eq("id", post_uuid).execute()
            return post_uuid
        response = client.table("inbox_posts").insert(values).execute()
        return response.data[0]["id"] if response.data else None
    except Exception as exc:
        logger.warning("Could not upsert inbox post: %s", str(exc)[:200])
        return None


def resolve_inbox_post_link(post_uuid: str, platform: str, platform_post_id: str) -> bool:
    """
    Point an inbox post at its content_stats row, if one exists yet.

    Fill-only-if-null and retried every sweep: a post published minutes ago has no
    content_stats row (analytics runs hourly), so a one-shot attempt would leave it
    unlinked forever. Platform folding matches the admin's own convention —
    content_stats keeps youtube_shorts as a separate value, so a YouTube comment thread
    has to look under both.
    """
    platforms = [platform]
    if platform == "youtube":
        platforms.append("youtube_shorts")
    client = get_client()
    try:
        current = (
            client.table("inbox_posts").select("content_stats_id").eq("id", post_uuid)
            .limit(1).execute()
        )
        if current.data and current.data[0].get("content_stats_id"):
            return False
        match = (
            client.table("content_stats")
            .select("id")
            .in_("platform", platforms)
            .eq("post_id", platform_post_id)
            .limit(1)
            .execute()
        )
        if not match.data:
            return False
        client.table("inbox_posts").update(
            {"content_stats_id": match.data[0]["id"]}
        ).eq("id", post_uuid).execute()
        return True
    except Exception as exc:
        logger.warning("Could not link inbox post to content_stats: %s", str(exc)[:200])
        return False


def _upsert_comments_individually(rows: list[dict[str, Any]]) -> int:
    """Per-row fallback — one bad row must not cost the whole batch."""
    client = get_client()
    written = 0
    for values in rows:
        try:
            client.table("inbox_comments").upsert(
                values, on_conflict="platform,platform_comment_id",
            ).execute()
            written += 1
        except Exception as exc:
            logger.warning("Could not upsert inbox comment: %s", str(exc)[:200])
    return written


def upsert_inbox_comments(rows: list[dict[str, Any]]) -> int:
    """
    Insert or refresh comments, deduped on (platform, platform_comment_id).

    Batched, unlike upsert_inbox_messages: `inbox_comments_key` is a FULL unique index,
    so PostgREST can infer it for ON CONFLICT. The message table's indexes are PARTIAL
    and cannot be — see that function for the workaround. Do not "tidy" the two into one
    shape; the difference is forced by the schema.

    handled_at/handled_by are never named, so an operator's triage survives every
    re-sync; source/sent_by are never written from here either, so a reply the admin
    wrote keeps its authorship when the sweep sees it come back from the platform.
    """
    if not rows:
        return 0
    client = get_client()
    now = datetime.now(timezone.utc).isoformat()
    prepared = []
    for row in rows:
        values = {k: v for k, v in row.items() if v is not None}
        values.pop("source", None)
        values.pop("sent_by", None)
        values["updated_at"] = now
        prepared.append(values)

    written = 0
    for bucket in _key_buckets(prepared):
        try:
            client.table("inbox_comments").upsert(
                bucket, on_conflict="platform,platform_comment_id",
            ).execute()
            written += len(bucket)
        except Exception as exc:
            logger.warning(
                "Batch comment upsert failed for %d rows, retrying individually: %s",
                len(bucket), str(exc)[:200],
            )
            written += _upsert_comments_individually(bucket)
    return written


def get_draft_exemplars(limit: int, min_reply_len: int,
                        min_incoming_len: int) -> list[dict[str, Any]]:
    """
    The (their message -> our reply) pairs that teach the drafter our voice.

    Reads the `inbox_reply_pairs` view (migration 040) — the adjacency needs lag(),
    which PostgREST cannot express, and the length filters need length(), which it
    cannot filter on either; both are projected by the view for that reason.

    Ordered by sent_at ASCENDING and sliced from the END, so the set is the most recent
    N in a STABLE order. Order matters beyond tidiness: this block is the cached prompt
    prefix, and prompt caching is a prefix match — reordering it on every run would
    silently cost a cache write instead of a cache read every time.
    """
    client = get_client()
    try:
        response = (
            client.table("inbox_reply_pairs")
            .select("incoming, ours, sent_at")
            .gte("ours_len", min_reply_len)
            .gte("incoming_len", min_incoming_len)
            .order("sent_at", desc=False)
            .execute()
        )
    except Exception as exc:
        logger.warning("Could not load draft exemplars: %s", str(exc)[:200])
        return []
    rows = response.data or []
    return rows[-limit:] if limit and len(rows) > limit else rows


def get_threads_needing_drafts(min_age_hours: int, limit: int) -> list[dict[str, Any]]:
    """
    Threads whose newest message is theirs, has text, and has gone unanswered.

    Two reads rather than a join: the `inbox_thread_latest` view (newest message per
    conversation) and then the conversations themselves. PostgREST will not join a view
    to a table without a declared relationship, and the set is small enough that it does
    not matter.

    A thread is SKIPPED when its draft already answers that same message — the
    skip-if-current rule that stops a scheduled run redrafting the whole backlog every
    time. Same shape as _brief_is_current (migration 031).

    Attachment-only messages are skipped too: roughly half of all mirrored messages are
    story replies and shared posts with no text at all, and there is nothing to draft a
    reply to.
    """
    client = get_client()
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=min_age_hours)).isoformat()
    try:
        latest = (
            client.table("inbox_thread_latest")
            .select("conversation_id, last_message_id, last_body, last_sent_at")
            .eq("last_direction", "incoming")
            .lt("last_sent_at", cutoff)
            .order("last_sent_at", desc=True)
            .execute()
        )
    except Exception as exc:
        logger.warning("Could not read thread tails: %s", str(exc)[:200])
        return []

    candidates = {
        row["conversation_id"]: row
        for row in latest.data or []
        if (row.get("last_body") or "").strip()
    }
    if not candidates:
        return []

    threads: list[dict[str, Any]] = []
    for chunk in _chunks(list(candidates), 100):
        try:
            response = (
                client.table("inbox_conversations")
                .select("id, platform, participant_name, participant_username, "
                        "draft_for_message_id, ig_is_follower")
                .in_("id", chunk)
                .execute()
            )
        except Exception as exc:
            logger.warning("Could not read conversations to draft: %s", str(exc)[:200])
            continue
        for row in response.data or []:
            tail = candidates[row["id"]]
            if row.get("draft_for_message_id") == tail["last_message_id"]:
                continue          # draft already answers this exact message
            threads.append({**row, **tail})

    threads.sort(key=lambda t: t["last_sent_at"])   # oldest neglect first
    return threads[:limit] if limit else threads


def get_thread_messages(conversation_id: str, limit: int) -> list[dict[str, Any]]:
    """The tail of one thread, oldest-first, for context in the drafting prompt."""
    client = get_client()
    try:
        response = (
            client.table("inbox_messages")
            .select("direction, body, sent_at")
            .eq("conversation_id", conversation_id)
            .order("sent_at", desc=True)
            .limit(limit)
            .execute()
        )
    except Exception as exc:
        logger.warning("Could not read thread %s: %s", conversation_id, str(exc)[:200])
        return []
    return list(reversed(response.data or []))


def update_conversation_draft(conversation_id: str, draft: str | None,
                              for_message_id: str | None, category: str | None) -> bool:
    """
    Stamp a draft onto a thread.

    Overwrites unconditionally, and that is deliberate: the draft is a disposable
    suggestion the operator copies out, never a document they edit in place, so there is
    no operator state here to protect. Contrast upsert_inbox_conversation, which is
    scrupulous about is_read/read_at/read_by.
    """
    client = get_client()
    try:
        client.table("inbox_conversations").update({
            "draft_response": draft,
            "draft_for_message_id": for_message_id,
            "draft_generated_at": datetime.now(timezone.utc).isoformat(),
            "draft_category": category,
        }).eq("id", conversation_id).execute()
        return True
    except Exception as exc:
        logger.warning("Could not write draft for %s: %s", conversation_id, str(exc)[:200])
        return False


def get_inbox_conversation_state() -> dict[str, dict[str, Any]]:
    """{zernio_conversation_id or uuid: {id, last_message_at, message_count}} — lets the
    sweep skip fetching messages for a thread that hasn't moved."""
    client = get_client()
    response = (
        client.table("inbox_conversations")
        .select("id, zernio_conversation_id, last_message_at")
        .execute()
    )
    state: dict[str, dict[str, Any]] = {}
    for row in response.data or []:
        key = row.get("zernio_conversation_id") or row["id"]
        state[key] = row
    return state


def get_inbox_post_state() -> dict[str, dict[str, Any]]:
    """{(platform, platform_post_id) -> row} for the comment sweep's change check."""
    client = get_client()
    response = (
        client.table("inbox_posts")
        .select("id, platform, platform_post_id, comment_count, content_stats_id, last_synced_at")
        .execute()
    )
    return {
        f"{row['platform']}:{row['platform_post_id']}": row
        for row in response.data or []
    }


def log_source_run(
    source_name: str,
    source_category: str,
    status: str,
    article_count: int = 0,
    error_message: str | None = None,
) -> None:
    """Record one source fetch attempt. status must be 'ok' or 'error'."""
    from datetime import date
    client = get_client()
    try:
        client.table("source_runs").insert({
            "run_date": date.today().isoformat(),
            "source_name": source_name,
            "source_category": source_category,
            "status": status,
            "article_count": article_count,
            "error_message": error_message,
        }).execute()
    except Exception as exc:
        logger.warning("Could not log source run (table may not exist): %s", exc)


def get_pipeline_settings() -> dict:
    """
    Fetch the single pipeline_settings row from Supabase.
    Returns a dict with tov_doc, audience_doc, similarity_threshold,
    score_threshold, and max_articles_per_source.
    Falls back to hardcoded defaults if the row is missing.
    """
    client = get_client()
    response = (
        client.table("pipeline_settings")
        .select("tov_doc, audience_doc, similarity_threshold, score_threshold, max_articles_per_source, custom_cluster_prompt, daily_brief_prompt, available_tags, available_geo_tags, research_score_threshold")
        .eq("id", 1)
        .single()
        .execute()
    )
    if response.data:
        return response.data
    # Defaults if table hasn't been migrated yet
    return {
        "tov_doc": "",
        "audience_doc": "",
        "similarity_threshold": 0.65,
        "score_threshold": 0.4,
        "max_articles_per_source": 50,
    }


def set_article_status(guid: str, status: str, reason: str | None = None) -> None:
    """Transition a single article to a new status."""
    if status not in VALID_STATUSES:
        raise ValueError(f"Unknown status '{status}'. Must be one of {VALID_STATUSES}")
    client = get_client()
    payload: dict = {"status": status}
    if reason is not None:
        payload["status_reason"] = reason
    client.table(TABLE).update(payload).eq("guid", guid).execute()


def get_existing_guids(guids: list[str]) -> set[str]:
    """Return subset of guids that already exist in the table."""
    if not guids:
        return set()
    client = get_client()
    response = (
        client.table(TABLE)
        .select("guid")
        .in_("guid", guids)
        .execute()
    )
    return {row["guid"] for row in (response.data or [])}
