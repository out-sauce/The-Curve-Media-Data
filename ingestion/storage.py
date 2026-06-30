"""
Supabase storage layer. Upserts articles using guid as the conflict key
so re-runs never create duplicates.
"""

import logging
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
        "id, is_self, instagram_url, tiktok_url, "
        "instagram_handle, tiktok_handle, display_name"
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
            content_type = "image/jpeg"
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


# Only the fields the Apify scrape actually returns — so an update never clobbers
# columns content_stats carries but Apify doesn't (shares/saves/reach/downloads/…).
_CONTENT_STATS_FIELDS = ("post_url", "views", "likes", "comments")


def upsert_self_content_stats(rows: list[dict[str, Any]]) -> int:
    """
    Upsert the is_self ("The Curve") competitor's posts into content_stats, deduped
    on (platform, post_id) with no source tag. Mirrors the admin's canonical
    lookup-then-update-else-insert: find the existing row by (platform, post_id) and
    update only the scraped fields (so shares/saves/reach/downloads etc. are
    preserved), otherwise insert a fresh row (calendar_item_id=null). Returns the
    number of rows written. Best-effort per row — one failure never aborts the rest.

    Each input row must carry: platform, post_id, post_url, views, likes, comments.
    """
    if not rows:
        return 0
    from datetime import datetime, timezone
    now_iso = datetime.now(timezone.utc).isoformat()
    client = get_client()
    written = 0
    for row in rows:
        platform = row.get("platform")
        post_id = row.get("post_id")
        if not platform or not post_id:
            continue
        scraped = {k: row.get(k) for k in _CONTENT_STATS_FIELDS}
        try:
            existing = (
                client.table("content_stats")
                .select("id")
                .eq("platform", platform)
                .eq("post_id", post_id)
                .limit(1)
                .execute()
            )
            if existing.data:
                client.table("content_stats").update(
                    {**scraped, "stats_synced_at": now_iso, "updated_at": now_iso}
                ).eq("id", existing.data[0]["id"]).execute()
            else:
                client.table("content_stats").insert({
                    "platform": platform,
                    "post_id": post_id,
                    "calendar_item_id": None,
                    "stats_synced_at": now_iso,
                    **scraped,
                }).execute()
            written += 1
        except Exception as exc:
            logger.warning(
                "Could not upsert content_stats row (%s/%s): %s",
                platform, post_id, str(exc)[:200],
            )
    logger.info("Upserted %d is_self posts into content_stats", written)
    return written


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
