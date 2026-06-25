"""
Social fetcher — pulls recent Instagram posts and TikTok videos via Apify and
normalises them into the same article dicts the rest of the pipeline consumes.

Each enabled source with source_type in ('instagram', 'tiktok') is scraped by the
relevant Apify actor using its `handle` (username without the @). The post caption
*is* the article body, so there is no separate research-stage scrape: rows are
inserted with scrape_status='scraped' and the caption stored in full_text, which
makes the research stage skip them idempotently (see research/research.py).

Resilience: an Apify failure for one account logs an 'error' source_runs row for
that account and continues — it never aborts the whole scan.

Notes on the contract vs. the original handover:
  * status is 'new' (not 'pending') — the filter stage picks up status='new'.
  * scrape_status is 'scraped' (not 'skipped'/'ok') — news_articles.scrape_status
    has a CHECK constraint allowing only 'scraped' | 'failed' | 'paywalled'.
  * guid is namespaced as '{platform}:{post_id}' so re-scans dedup on the post id
    (via upsert on_conflict='guid') without colliding across platforms.
"""

import logging
from datetime import datetime, timezone

from config import (
    APIFY_TOKEN,
    APIFY_INSTAGRAM_ACTOR,
    APIFY_TIKTOK_ACTOR,
    MAX_ARTICLES_PER_SOURCE,
)
from ingestion.apify import run_actor as _run_actor, parse_ts as _parse_ts
from ingestion.storage import get_social_sources, log_source_run

logger = logging.getLogger(__name__)

TITLE_MAX_LEN = 200


def _make_title(caption: str, handle: str, platform: str) -> str:
    """First line of the caption (truncated) as the headline; fall back to handle."""
    text = (caption or "").strip()
    if text:
        first_line = text.splitlines()[0].strip()
        title = (first_line or text)[:TITLE_MAX_LEN].strip()
        if title:
            return title
    return f"@{handle} on {platform.capitalize()}"


def _normalise_instagram(item: dict) -> tuple | None:
    """Return (post_id, caption, permalink, published_at, image_url) or None."""
    post_id = item.get("id") or item.get("shortCode") or item.get("shortcode")
    short_code = item.get("shortCode") or item.get("shortcode")
    permalink = item.get("url") or (
        f"https://www.instagram.com/p/{short_code}/" if short_code else None
    )
    caption = (item.get("caption") or "").strip()
    published_at = _parse_ts(item.get("timestamp"))
    image_url = item.get("displayUrl") or item.get("displayURL")
    if not post_id or not permalink:
        return None
    return str(post_id), caption, permalink, published_at, image_url


def _normalise_tiktok(item: dict) -> tuple | None:
    """Return (post_id, caption, permalink, published_at, image_url) or None."""
    post_id = item.get("id")
    permalink = item.get("webVideoUrl") or item.get("url")
    caption = (item.get("text") or item.get("caption") or "").strip()
    published_at = _parse_ts(item.get("createTimeISO") or item.get("createTime"))
    video_meta = item.get("videoMeta") or {}
    image_url = video_meta.get("coverUrl") or video_meta.get("originalCoverUrl")
    if not post_id or not permalink:
        return None
    return str(post_id), caption, permalink, published_at, image_url


_PLATFORMS = {
    "instagram": {
        "actor": APIFY_INSTAGRAM_ACTOR,
        "normalise": _normalise_instagram,
        "input": lambda handle, limit: {
            "directUrls": [f"https://www.instagram.com/{handle}/"],
            "resultsType": "posts",
            "resultsLimit": limit,
        },
    },
    "tiktok": {
        "actor": APIFY_TIKTOK_ACTOR,
        "normalise": _normalise_tiktok,
        "input": lambda handle, limit: {
            "profiles": [handle],
            "resultsPerPage": limit,
            "shouldDownloadVideos": False,
            "shouldDownloadCovers": False,
            "shouldDownloadSubtitles": False,
        },
    },
}


def _fetch_social_source(source: dict) -> list[dict]:
    """Scrape one Instagram/TikTok account. Never raises — logs and returns []."""
    name = source["name"]
    category = source.get("category", "")
    platform = source.get("source_type", "")
    handle = (source.get("handle") or "").lstrip("@").strip()

    spec = _PLATFORMS.get(platform)
    if spec is None:
        logger.warning("Unknown social platform '%s' for source '%s'", platform, name)
        return []

    if not handle:
        logger.warning("Social source '%s' has no handle — skipping", name)
        log_source_run(name, category, "error", 0, "No handle configured")
        return []

    limit = MAX_ARTICLES_PER_SOURCE or 50

    try:
        items = _run_actor(spec["actor"], spec["input"](handle, limit))
    except Exception as exc:
        logger.warning("Apify %s fetch for '%s' (@%s) failed: %s", platform, name, handle, exc)
        log_source_run(name, category, "error", 0, str(exc)[:500])
        return []

    now = datetime.now(timezone.utc).isoformat()
    articles = []
    for item in items[:limit]:
        try:
            normalised = spec["normalise"](item)
        except Exception as exc:
            logger.debug("Skipping malformed %s item from '%s': %s", platform, name, exc)
            continue
        if not normalised:
            continue
        post_id, caption, permalink, published_at, image_url = normalised

        articles.append({
            "guid": f"{platform}:{post_id}",
            "source_name": name,
            "source_category": category,
            "title": _make_title(caption, handle, platform),
            "url": permalink,
            "summary": caption,
            "full_text": caption,
            "author": f"@{handle}",
            "image_url": image_url,
            "published_at": published_at,
            "fetched_at": now,
            "raw_tags": [],
            "status": "new",
            # Caption is already the body — no research-stage scrape needed.
            "scrape_status": "scraped",
        })

    logger.info("Fetched %d %s posts from @%s (%s)", len(articles), platform, handle, name)
    log_source_run(name, category, "ok", len(articles))
    return articles


def fetch_social() -> list[dict]:
    """Fetch all enabled Instagram/TikTok sources via Apify."""
    if not APIFY_TOKEN:
        logger.warning("APIFY_TOKEN not set — skipping social fetch")
        return []

    sources = get_social_sources()
    if not sources:
        return []

    all_articles = []
    for source in sources:
        all_articles.extend(_fetch_social_source(source))

    logger.info("Social fetch: %d posts from %d accounts", len(all_articles), len(sources))
    return all_articles
