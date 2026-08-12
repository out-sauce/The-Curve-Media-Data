"""
Guest-post stats — one Apify run per post URL.

The admin's Content Calendar can hold "guest posts": pieces published from a
GUEST's own Instagram/TikTok/LinkedIn/YouTube account rather than The Curve's
(content_calendar_items.social_kind = 'guest'). A guest may or may not be a
tracked competitor, so neither the competitor sweep nor the is_self scrape ever
sees their posts — instead each guest item's pasted post_url is scraped
directly, one actor run per post, and upserted into content_stats WITH its
calendar_item_id (the admin's URL auto-linking only runs on item saves, so the
link travels with the write).

Runs two ways (api.py /run/guest-post-stats):
  • ?id=<calendar_item_id> — the admin drawer's "Refresh stats" (ignores the
    lookback window, works on undated items too);
  • no id — the daily sweep (scheduler.run_daily_pipeline): every guest item
    with a post_url whose publish_date is within GUEST_POST_LOOKBACK_DAYS.

Single-post actor inputs verified live (2026-08-12), one real post each:
  instagram  apify~instagram-api-scraper   {"directUrls": [url], "resultsType": "posts"}
             → post items directly (same fields as latestPosts → _ig_normalise_post)
  tiktok     clockworks~tiktok-scraper     {"postURLs": [url]}
             → video items (same fields as the profile run → _tt_normalise_post)
  youtube    streamers~youtube-scraper     {"startUrls": [{"url": url}]}
             → one type="video" item (same fields → _yt_normalise_post); a
             /shorts/ URL works the same and stays platform "youtube"
  linkedin   apimaestro~linkedin-post-detail {"post_urls": [url]}
             → {post{id,url,text,created_at{timestamp ms}}, stats{total_reactions,
             comments,shares}, media[]} — its own normaliser below (the harvestapi
             profile actor's shape doesn't apply)

Engagement: engagement_reach = interactions/views as in the is_self scrape;
engagement_audience needs the account's follower count, which a lone post
doesn't carry — left None (accepted; guest numbers never enter The Curve's own
aggregates anyway).

Resilience mirrors the competitor run: per-item failures log a warning and the
run continues; the whole run logs one source_runs row (category 'guest_post').
"""

import logging
from datetime import datetime, timedelta, timezone

from config import (
    APIFY_INSTAGRAM_ACTOR,
    APIFY_TIKTOK_ACTOR,
    APIFY_YOUTUBE_ACTOR,
    APIFY_LINKEDIN_POST_ACTOR,
    GUEST_POST_LOOKBACK_DAYS,
)
from ingestion.apify import run_actor, parse_ts
from ingestion.competitors import (
    _ig_normalise_post,
    _tt_normalise_post,
    _yt_normalise_post,
    _to_int,
)
from ingestion.storage import (
    get_client,
    log_source_run,
    store_competitor_image,
    upsert_self_content_stats,
)

logger = logging.getLogger(__name__)

_RUN_NAME = "Guest posts"
_RUN_CATEGORY = "guest_post"


def _li_normalise_post_detail(item: dict) -> dict | None:
    """Normalise one apimaestro~linkedin-post-detail item (shape in module doc)."""
    post = item.get("post") or {}
    post_id = post.get("id")
    if not post_id:
        return None
    stats = item.get("stats") or {}
    created = post.get("created_at") or {}
    ts_ms = created.get("timestamp")
    media = item.get("media") or []
    thumbnail = media[0].get("url") if media and isinstance(media[0], dict) else None
    return {
        "post_id": str(post_id),
        "caption": (post.get("text") or "").strip(),
        "url": post.get("url"),
        # created_at.timestamp is milliseconds; parse_ts expects seconds.
        "published_at": parse_ts(ts_ms / 1000) if isinstance(ts_ms, (int, float)) else None,
        "like_count": _to_int(stats.get("total_reactions")),
        "comment_count": _to_int(stats.get("comments")),
        "view_count": None,
        "share_count": _to_int(stats.get("shares")),
        "save_count": None,
        "hashtags": [],
        "duration_sec": None,
        "thumbnail_url": thumbnail,
    }


# platform (== the admin's channel value) → single-post actor spec.
# `extract` reduces the raw dataset items to post items for `normalise`.
_GUEST_PLATFORMS = {
    "instagram": {
        "actor": APIFY_INSTAGRAM_ACTOR,
        "input": lambda url: {"directUrls": [url], "resultsType": "posts", "resultsLimit": 1},
        "extract": lambda items: items,
        "normalise": _ig_normalise_post,
    },
    "tiktok": {
        "actor": APIFY_TIKTOK_ACTOR,
        "input": lambda url: {
            "postURLs": [url],
            "shouldDownloadVideos": False,
            "shouldDownloadCovers": False,
            "shouldDownloadSubtitles": False,
        },
        "extract": lambda items: items,
        "normalise": _tt_normalise_post,
    },
    "linkedin": {
        "actor": APIFY_LINKEDIN_POST_ACTOR,
        "input": lambda url: {"post_urls": [url]},
        "extract": lambda items: items,
        "normalise": _li_normalise_post_detail,
    },
    "youtube": {
        "actor": APIFY_YOUTUBE_ACTOR,
        "input": lambda url: {"startUrls": [{"url": url}], "maxResults": 1},
        "extract": lambda items: [i for i in items if i.get("type") != "channel"],
        "normalise": _yt_normalise_post,
    },
}


def _fetch_guest_items(calendar_item_id: str | None) -> list[dict]:
    """Guest calendar items with a posted URL — one row, or the sweep window."""
    client = get_client()
    query = (
        client.table("content_calendar_items")
        .select("id, title, channel, post_url, publish_date")
        .eq("social_kind", "guest")
        .not_.is_("post_url", "null")
    )
    if calendar_item_id:
        query = query.eq("id", calendar_item_id)
    else:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=GUEST_POST_LOOKBACK_DAYS)).date().isoformat()
        # gte never matches NULL, so undated guest items are sweep-skipped by
        # design (the admin's per-item Refresh still reaches them via ?id=).
        query = query.gte("publish_date", cutoff)
    response = query.execute()
    return [r for r in (response.data or []) if (r.get("post_url") or "").strip()]


def _scrape_guest_item(item: dict) -> dict | None:
    """One Apify run for one guest item's post URL → a content_stats row (or None)."""
    channel = (item.get("channel") or "").strip()
    post_url = item["post_url"].strip()
    spec = _GUEST_PLATFORMS.get(channel)
    if spec is None:
        logger.warning("Guest item %s has unsupported channel '%s' — skipping", item["id"], channel)
        return None
    if not spec["actor"]:
        logger.warning("Guest item %s: no actor configured for %s — skipping", item["id"], channel)
        return None

    items = run_actor(spec["actor"], spec["input"](post_url))
    # Bad targets come back as a data item with an `error` key, not a non-2xx
    # (same convention the competitor run handles).
    error_item = next((i for i in items if isinstance(i, dict) and i.get("error")), None)
    if error_item:
        raise RuntimeError(
            f"{channel} actor error for '{post_url}': "
            f"{error_item.get('error')} {error_item.get('note') or ''}".strip()
        )

    post = next(
        (p for p in (spec["normalise"](i) for i in spec["extract"](items)) if p),
        None,
    )
    if post is None:
        raise RuntimeError(f"{channel} actor returned no usable post for '{post_url}'")

    likes = post["like_count"]
    comments = post["comment_count"]
    shares = post["share_count"]
    saves = post["save_count"]
    views = post["view_count"]
    interactions = (likes or 0) + (comments or 0) + (shares or 0) + (saves or 0)
    # Same proxy as the is_self scrape; engagement_audience stays None (no
    # follower count comes with a lone post).
    engagement_reach = round(interactions / views, 6) if views else None

    # Persist the thumbnail like the competitor run does — the platforms' CDN
    # URLs expire within days; same bucket, same deterministic path.
    thumbnail_url = store_competitor_image(
        post["thumbnail_url"], f"posts/{channel}_{post['post_id']}.jpg"
    )

    return {
        "platform": channel,
        "post_id": post["post_id"],
        "post_url": post["url"] or post_url,
        "posted_at": post["published_at"],
        "views": views,
        "likes": likes,
        "comments": comments,
        "shares": shares,
        "saves": saves,
        "caption": post["caption"] or None,
        "hashtags": post["hashtags"] or None,
        "duration_sec": post["duration_sec"],
        "engagement_rate": engagement_reach,
        "engagement_reach": engagement_reach,
        "engagement_audience": None,
        "thumbnail_url": thumbnail_url,
        "calendar_item_id": item["id"],
    }


def run_guest_post_stats(calendar_item_id: str | None = None) -> int:
    """
    Scrape stats for guest-post calendar items and upsert them into
    content_stats (linked to their item). Returns the number of rows written.
    Never raises — a total failure logs an 'error' source_runs row.
    """
    try:
        items = _fetch_guest_items(calendar_item_id)
    except Exception as exc:
        logger.error("Guest-post fetch failed: %s", exc, exc_info=True)
        log_source_run(_RUN_NAME, _RUN_CATEGORY, "error", 0, str(exc)[:500])
        return 0

    if not items:
        logger.info(
            "No guest posts to scrape (%s)",
            f"id={calendar_item_id}" if calendar_item_id else f"last {GUEST_POST_LOOKBACK_DAYS} days",
        )
        if calendar_item_id:
            log_source_run(_RUN_NAME, _RUN_CATEGORY, "error", 0, "Guest item not found or has no post URL")
        return 0

    rows = []
    failures = 0
    for item in items:
        try:
            row = _scrape_guest_item(item)
        except Exception as exc:
            failures += 1
            logger.warning(
                "Guest post scrape failed for item %s (%s): %s",
                item["id"], item.get("title") or "untitled", str(exc)[:300],
            )
            continue
        if row:
            rows.append(row)

    written = upsert_self_content_stats(rows)
    logger.info(
        "Guest posts: %d scraped / %d written / %d failed (of %d items)",
        len(rows), written, failures, len(items),
    )
    status = "ok" if written or not failures else "error"
    log_source_run(
        _RUN_NAME, _RUN_CATEGORY, status, written,
        f"{failures} of {len(items)} failed" if failures else None,
    )
    return written
