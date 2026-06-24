"""
RSS feed fetcher. Parses each source and returns normalised article dicts.
"""

import hashlib
import logging
from datetime import datetime, timedelta, timezone

import feedparser
import httpx

from ingestion.storage import get_sources, log_source_run
from config import MAX_ARTICLES_PER_SOURCE

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; CurveNewsPipeline/1.0; +https://curve.finance)"
    )
}


def _guid(url: str, title: str) -> str:
    """Stable content hash used for deduplication."""
    raw = f"{url}|{title}".encode()
    return hashlib.sha256(raw).hexdigest()


def _parse_date(entry) -> str:
    """Return ISO-8601 UTC string from a feedparser entry, fallback to now."""
    for attr in ("published_parsed", "updated_parsed", "created_parsed"):
        parsed = getattr(entry, attr, None)
        if parsed:
            try:
                dt = datetime(*parsed[:6], tzinfo=timezone.utc)
                return dt.isoformat()
            except Exception:
                pass
    return datetime.now(timezone.utc).isoformat()


def _extract_image(entry) -> str | None:
    """Best-effort extraction of a lead image URL from a feed entry."""
    # media:thumbnail or media:content
    media = getattr(entry, "media_thumbnail", None) or getattr(
        entry, "media_content", None
    )
    if media and isinstance(media, list) and media[0].get("url"):
        return media[0]["url"]
    # enclosures
    for enc in getattr(entry, "enclosures", []):
        if enc.get("type", "").startswith("image"):
            return enc.get("href") or enc.get("url")
    return None


def fetch_rss_source(source: dict) -> list[dict]:
    """Fetch and normalise articles from a single RSS source."""
    articles = []
    try:
        resp = httpx.get(source["url"], headers=HEADERS, timeout=15, follow_redirects=True)
        resp.raise_for_status()
        feed = feedparser.parse(resp.text)
    except Exception as exc:
        logger.warning("Failed to fetch %s: %s", source["name"], exc)
        log_source_run(source["name"], source.get("category", ""), "error", 0, str(exc))
        return articles

    entries = feed.entries
    if MAX_ARTICLES_PER_SOURCE:
        entries = entries[: MAX_ARTICLES_PER_SOURCE]

    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)

    for entry in entries:
        url = getattr(entry, "link", None)
        title = getattr(entry, "title", "").strip()
        if not url or not title:
            continue

        # Skip articles published more than 24 hours ago
        published_at = _parse_date(entry)
        pub_dt = datetime.fromisoformat(published_at)
        if pub_dt < cutoff:
            continue

        # Summary: prefer 'summary', fall back to 'description', then title
        summary = (
            getattr(entry, "summary", "")
            or getattr(entry, "description", "")
        ).strip() or title

        articles.append(
            {
                "guid": _guid(url, title),
                "source_name": source["name"],
                "source_category": source["category"],
                "title": title,
                "url": url,
                "summary": summary,
                "author": getattr(entry, "author", None),
                "image_url": _extract_image(entry),
                "published_at": published_at,
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "raw_tags": [t.get("term") for t in getattr(entry, "tags", []) if t.get("term")],
                "status": "new",
            }
        )

    logger.info("Fetched %d articles from %s", len(articles), source["name"])
    log_source_run(source["name"], source.get("category", ""), "ok", len(articles))
    return articles


def fetch_all_sources() -> list[dict]:
    """Fetch from all enabled sources — RSS, NewsAPI, Finnhub, and social (IG/TikTok)."""
    from ingestion.newsapi import fetch_newsapi
    from ingestion.finnhub import fetch_finnhub
    from ingestion.social import fetch_social

    all_articles = []

    for source in get_sources(source_type="rss"):
        all_articles.extend(fetch_rss_source(source))

    all_articles.extend(fetch_newsapi())
    all_articles.extend(fetch_finnhub())
    all_articles.extend(fetch_social())

    logger.info("Total articles fetched: %d", len(all_articles))
    return all_articles
