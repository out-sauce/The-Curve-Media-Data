"""
Competitor run — a parallel Apify flow (separate from the news social fetch) that
tracks competitors' follower counts and recent post engagement.

For each enabled competitor (Instagram or TikTok) it:
  1. captures the current follower count (append-only snapshot → growth time series),
  2. captures the <= COMPETITOR_POST_LIMIT most recent posts within the last
     COMPETITOR_LOOKBACK_DAYS, with likes / comments / views / caption,
  3. upserts both into the competitor tables (snapshots append, posts re-upsert on
     guid so engagement counts refresh as posts mature).

Reuses the existing Apify plumbing (ingestion/apify.run_actor) and APIFY_TOKEN /
actor-id config — it does NOT touch the news flow or news_articles.

Resilience: an Apify failure for one competitor logs an 'error' source_runs row
(category 'competitor') for that account and continues — it never aborts the run.
"""

import logging
from datetime import datetime, timedelta, timezone

from config import (
    APIFY_TOKEN,
    APIFY_INSTAGRAM_ACTOR,
    APIFY_TIKTOK_ACTOR,
    COMPETITOR_POST_LIMIT,
    COMPETITOR_LOOKBACK_DAYS,
)
from ingestion.apify import run_actor, parse_ts
from ingestion.storage import (
    get_competitors,
    insert_competitor_snapshot,
    upsert_competitor_posts,
    log_source_run,
)

logger = logging.getLogger(__name__)

_RUN_CATEGORY = "competitor"


def _to_int(value) -> int | None:
    """Best-effort coerce an Apify count to int, tolerating None/strings."""
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_dt(iso_value: str | None) -> datetime | None:
    """Parse an ISO-8601 string (incl. trailing 'Z') to an aware datetime."""
    if not iso_value:
        return None
    try:
        text = iso_value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(text)
    except (TypeError, ValueError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


# ── Instagram (apify~instagram-api-scraper, resultsType="details") ────────────
# The profile detail result carries followersCount/followsCount/postsCount plus a
# `latestPosts` array with per-post engagement.

def _ig_profile(items: list[dict]) -> dict:
    profile = items[0] if items else {}
    return {
        "follower_count": _to_int(profile.get("followersCount")),
        "following_count": _to_int(profile.get("followsCount")),
        "post_count": _to_int(profile.get("postsCount")),
    }


def _ig_posts(items: list[dict]) -> list[dict]:
    profile = items[0] if items else {}
    return profile.get("latestPosts") or []


def _ig_normalise_post(item: dict) -> dict | None:
    post_id = item.get("id") or item.get("shortCode") or item.get("shortcode")
    short_code = item.get("shortCode") or item.get("shortcode")
    url = item.get("url") or (
        f"https://www.instagram.com/p/{short_code}/" if short_code else None
    )
    if not post_id:
        return None
    return {
        "post_id": str(post_id),
        "caption": (item.get("caption") or "").strip(),
        "url": url,
        "published_at": parse_ts(item.get("timestamp")),
        "like_count": _to_int(item.get("likesCount")),
        "comment_count": _to_int(item.get("commentsCount")),
        "view_count": _to_int(item.get("videoViewCount") or item.get("videoPlayCount")),
    }


# ── TikTok (clockworks~tiktok-scraper) ────────────────────────────────────────
# Returns a list of videos; each carries authorMeta (fans/following/video) and
# per-video diggCount/commentCount/playCount.

def _tt_profile(items: list[dict]) -> dict:
    author = (items[0].get("authorMeta") or {}) if items else {}
    return {
        "follower_count": _to_int(author.get("fans")),
        "following_count": _to_int(author.get("following")),
        "post_count": _to_int(author.get("video")),
    }


def _tt_posts(items: list[dict]) -> list[dict]:
    return items or []


def _tt_normalise_post(item: dict) -> dict | None:
    post_id = item.get("id")
    url = item.get("webVideoUrl") or item.get("url")
    if not post_id:
        return None
    return {
        "post_id": str(post_id),
        "caption": (item.get("text") or item.get("caption") or "").strip(),
        "url": url,
        "published_at": parse_ts(item.get("createTimeISO") or item.get("createTime")),
        "like_count": _to_int(item.get("diggCount")),
        "comment_count": _to_int(item.get("commentCount")),
        "view_count": _to_int(item.get("playCount")),
    }


_PLATFORMS = {
    "instagram": {
        "actor": APIFY_INSTAGRAM_ACTOR,
        "input": lambda handle, limit: {
            "directUrls": [f"https://www.instagram.com/{handle}/"],
            "resultsType": "details",
            "resultsLimit": limit,
        },
        "profile": _ig_profile,
        "posts": _ig_posts,
        "normalise_post": _ig_normalise_post,
    },
    "tiktok": {
        "actor": APIFY_TIKTOK_ACTOR,
        "input": lambda handle, limit: {
            "profiles": [handle],
            "resultsPerPage": limit,
            "shouldDownloadVideos": False,
            "shouldDownloadCovers": False,
            "shouldDownloadSubtitles": False,
        },
        "profile": _tt_profile,
        "posts": _tt_posts,
        "normalise_post": _tt_normalise_post,
    },
}


def _run_competitor(competitor: dict) -> int:
    """Scrape one competitor. Never raises — logs and returns posts written."""
    name = competitor["name"]
    platform = competitor.get("source_type", "")
    handle = (competitor.get("handle") or "").lstrip("@").strip()

    spec = _PLATFORMS.get(platform)
    if spec is None:
        logger.warning("Unknown competitor platform '%s' for '%s'", platform, name)
        log_source_run(name, _RUN_CATEGORY, "error", 0, f"Unknown platform '{platform}'")
        return 0

    if not handle:
        logger.warning("Competitor '%s' has no handle — skipping", name)
        log_source_run(name, _RUN_CATEGORY, "error", 0, "No handle configured")
        return 0

    # Fetch a generous window of posts; the 14-day / 10-post cap is applied below.
    fetch_limit = max(COMPETITOR_POST_LIMIT * 5, 50)

    try:
        items = run_actor(spec["actor"], spec["input"](handle, fetch_limit))
    except Exception as exc:
        logger.warning("Apify %s fetch for competitor '%s' (@%s) failed: %s", platform, name, handle, exc)
        log_source_run(name, _RUN_CATEGORY, "error", 0, str(exc)[:500])
        return 0

    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()
    cutoff = now - timedelta(days=COMPETITOR_LOOKBACK_DAYS)

    # 1. Follower snapshot (append-only time series).
    profile = spec["profile"](items)
    insert_competitor_snapshot({
        "competitor_id": competitor.get("id"),
        "handle": handle,
        "platform": platform,
        "follower_count": profile.get("follower_count"),
        "following_count": profile.get("following_count"),
        "post_count": profile.get("post_count"),
        "captured_at": now_iso,
    })

    # 2. Recent posts — normalise, apply 14-day cutoff, sort desc, cap to 10.
    normalised = []
    for raw in spec["posts"](items):
        try:
            post = spec["normalise_post"](raw)
        except Exception as exc:
            logger.debug("Skipping malformed %s post from '%s': %s", platform, name, exc)
            continue
        if not post:
            continue
        published_dt = _parse_dt(post["published_at"])
        if published_dt is None or published_dt < cutoff:
            continue
        normalised.append((published_dt, post))

    normalised.sort(key=lambda pair: pair[0], reverse=True)
    selected = normalised[:COMPETITOR_POST_LIMIT]

    rows = []
    for _, post in selected:
        rows.append({
            "competitor_id": competitor.get("id"),
            "guid": f"{platform}:{post['post_id']}",
            "platform": platform,
            "post_url": post["url"],
            "caption": post["caption"],
            "like_count": post["like_count"],
            "comment_count": post["comment_count"],
            "view_count": post["view_count"],
            "published_at": post["published_at"],
            "fetched_at": now_iso,
        })

    written = upsert_competitor_posts(rows)
    logger.info(
        "Competitor '%s' (@%s, %s): follower_count=%s / %d posts",
        name, handle, platform, profile.get("follower_count"), len(rows),
    )
    log_source_run(name, _RUN_CATEGORY, "ok", len(rows))
    return written


def run_competitors() -> None:
    """Run the competitor scrape for all enabled competitors via Apify."""
    if not APIFY_TOKEN:
        logger.warning("APIFY_TOKEN not set — skipping competitor run")
        return

    competitors = get_competitors()
    if not competitors:
        logger.info("No enabled competitors configured — skipping competitor run")
        return

    total_posts = 0
    for competitor in competitors:
        total_posts += _run_competitor(competitor)

    logger.info(
        "Competitor run: %d posts across %d competitors", total_posts, len(competitors)
    )
