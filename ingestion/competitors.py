"""
Competitor run — a parallel Apify flow (separate from the news social fetch) that
tracks competitors' follower counts and recent post engagement.

A competitor is ONE brand row that may carry an Instagram channel, a TikTok channel,
or both. For each channel present on the row it:
  1. captures the current follower count and per-platform stats,
  2. captures the <= COMPETITOR_POST_LIMIT most recent posts within the last
     COMPETITOR_LOOKBACK_DAYS (with likes / comments / views / caption / thumbnail),
     tagging each post with its channel (competitor_posts.platform),
  3. persists each avatar/thumbnail into the public `competitor-thumbnails` Storage
     bucket and writes the stable public URL back (CDN URLs expire within a day),
  4. writes the per-platform stat columns back onto the competitors row.

The single is_self ("The Curve") competitor additionally has its posts upserted into
content_stats (deduped on (platform, post_id)) over a wider window.

Reuses the existing Apify plumbing (ingestion/apify.run_actor) and APIFY_TOKEN /
actor-id config — it does NOT touch the news flow or news_articles.

Resilience: an Apify failure for one channel logs an 'error' source_runs row
(category 'competitor') for that channel and continues — it never aborts the run,
and a failed/absent channel never blanks the other channel's columns.
"""

import logging
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

from config import (
    APIFY_TOKEN,
    APIFY_INSTAGRAM_ACTOR,
    APIFY_TIKTOK_ACTOR,
    COMPETITOR_POST_LIMIT,
    COMPETITOR_LOOKBACK_DAYS,
    SELF_CONTENT_STATS_LOOKBACK_DAYS,
    SELF_CONTENT_STATS_LIMIT,
)
from ingestion.apify import run_actor, parse_ts
from ingestion.storage import (
    get_competitors,
    update_competitor_stats,
    upsert_competitor_posts,
    upsert_self_content_stats,
    store_competitor_image,
    get_existing_post_thumbnails,
    log_source_run,
    get_self_social_accounts,
    upsert_follower_snapshot,
    update_social_account_follower_count,
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
        "display_name": (profile.get("fullName") or "").strip() or None,
        "avatar_url": profile.get("profilePicUrlHD") or profile.get("profilePicUrl") or None,
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
    hashtags = [h for h in (item.get("hashtags") or []) if isinstance(h, str)]
    return {
        "post_id": str(post_id),
        "caption": (item.get("caption") or "").strip(),
        "url": url,
        "published_at": parse_ts(item.get("timestamp")),
        "like_count": _to_int(item.get("likesCount")),
        "comment_count": _to_int(item.get("commentsCount")),
        "view_count": _to_int(item.get("videoViewCount") or item.get("videoPlayCount")),
        # Instagram's public scrape does not expose shares/saves (owner-only insights).
        "share_count": None,
        "save_count": None,
        "hashtags": hashtags,
        "duration_sec": _to_int(item.get("videoDuration")),
        "thumbnail_url": item.get("displayUrl") or None,
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
        "display_name": (author.get("nickName") or author.get("name") or "").strip() or None,
        "avatar_url": author.get("avatar") or None,
    }


def _tt_posts(items: list[dict]) -> list[dict]:
    return items or []


def _tt_normalise_post(item: dict) -> dict | None:
    post_id = item.get("id")
    url = item.get("webVideoUrl") or item.get("url")
    if not post_id:
        return None
    video_meta = item.get("videoMeta") or {}
    covers = item.get("covers")
    thumbnail = video_meta.get("coverUrl") or video_meta.get("originalCoverUrl")
    if not thumbnail and isinstance(covers, list) and covers:
        thumbnail = covers[0]
    hashtags = [
        h.get("name") for h in (item.get("hashtags") or [])
        if isinstance(h, dict) and h.get("name")
    ]
    return {
        "post_id": str(post_id),
        "caption": (item.get("text") or item.get("caption") or "").strip(),
        "url": url,
        "published_at": parse_ts(item.get("createTimeISO") or item.get("createTime")),
        "like_count": _to_int(item.get("diggCount")),
        "comment_count": _to_int(item.get("commentCount")),
        "view_count": _to_int(item.get("playCount")),
        "share_count": _to_int(item.get("shareCount")),
        "save_count": _to_int(item.get("collectCount")),
        "hashtags": hashtags,
        "duration_sec": _to_int(video_meta.get("duration")),
        "thumbnail_url": thumbnail or None,
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


def _handle_from_url(url: str | None) -> str | None:
    """Parse a username off an IG/TikTok profile URL (e.g. .../@thecurve → thecurve)."""
    if not url:
        return None
    try:
        path = urlparse(url).path
    except (TypeError, ValueError):
        return None
    segment = next((part for part in path.split("/") if part), "")
    return segment.lstrip("@").strip() or None


def _resolve_channels(competitor: dict) -> list[dict]:
    """
    Resolve the channels present on a competitor row to a list of
    {"platform", "handle"} — instagram if it carries an instagram handle/url,
    tiktok likewise. The handle comes from *_handle, falling back to parsing *_url.
    """
    channels = []
    for platform in ("instagram", "tiktok"):
        raw_handle = (competitor.get(f"{platform}_handle") or "").lstrip("@").strip()
        handle = raw_handle or _handle_from_url(competitor.get(f"{platform}_url"))
        if handle:
            channels.append({"platform": platform, "handle": handle})
    return channels


def _normalise_posts(spec: dict, items: list[dict], name: str, platform: str) -> list[tuple]:
    """Normalise + parse-date all posts from an actor run → [(published_dt, post)]."""
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
        if published_dt is None:
            continue
        normalised.append((published_dt, post))
    normalised.sort(key=lambda pair: pair[0], reverse=True)
    return normalised


_self_social_accounts: dict[str, str] | None = None


def _snapshot_self_follower(platform: str, follower_count) -> None:
    """
    Append a daily follower snapshot for The Curve's own channel and refresh the
    current count on its social_accounts row. follower_snapshots links to
    social_accounts (not competitors). Best-effort — never aborts the run.
    """
    if follower_count is None:
        return
    global _self_social_accounts
    try:
        if _self_social_accounts is None:
            _self_social_accounts = get_self_social_accounts()
        social_account_id = _self_social_accounts.get(platform)
        if not social_account_id:
            logger.warning("No social_accounts row for self %s — skipping follower snapshot", platform)
            return
        upsert_follower_snapshot(social_account_id, platform, follower_count)
        update_social_account_follower_count(social_account_id, follower_count)
        logger.info("Self %s follower snapshot: %s", platform, follower_count)
    except Exception as exc:
        logger.warning("Self %s follower snapshot failed: %s", platform, str(exc)[:200])


def _run_channel(competitor_id, name: str, platform: str, handle: str, is_self: bool,
                 stats: dict, content_stats_rows: list) -> int:
    """
    Scrape one channel (instagram or tiktok) of a competitor. Accumulates the
    per-platform stat columns into `stats` and, for the is_self row, appends the
    wider content_stats post set into `content_stats_rows`. Returns the number of
    competitor_posts rows written. Raises on actor failure so the caller can log an
    error source_run for just this channel.
    """
    spec = _PLATFORMS[platform]

    # Fetch a generous window; the 14-day / 10-post cap is applied below. The
    # is_self row fetches more so its wider content_stats window has posts to draw on.
    fetch_limit = SELF_CONTENT_STATS_LIMIT if is_self else max(COMPETITOR_POST_LIMIT * 5, 50)

    items = run_actor(spec["actor"], spec["input"](handle, fetch_limit))

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=COMPETITOR_LOOKBACK_DAYS)

    profile = spec["profile"](items)
    follower_count = profile.get("follower_count")

    normalised = _normalise_posts(spec, items, name, platform)

    # competitor_posts: 14-day cutoff, cap to COMPETITOR_POST_LIMIT.
    selected = [post for dt, post in normalised if dt >= cutoff][:COMPETITOR_POST_LIMIT]

    # Persist the avatar; omit the field on failure so skip-None preserves the prior value.
    avatar_url = store_competitor_image(
        profile.get("avatar_url"), f"avatars/{competitor_id}_{platform}.jpg"
    )

    # Preserve already-stored thumbnails when a re-fetch of an existing post fails.
    existing_thumbs = get_existing_post_thumbnails(
        competitor_id, [post["post_id"] for post in selected]
    )

    rows = []
    engagements = []
    for post in selected:
        likes = post["like_count"]
        comments = post["comment_count"]
        post_id = post["post_id"]
        thumbnail_url = store_competitor_image(
            post["thumbnail_url"], f"posts/{platform}_{post_id}.jpg"
        )
        if thumbnail_url is None:
            # Failed (or absent) fetch: keep an existing post's prior thumbnail; a
            # brand-new post falls through to null.
            thumbnail_url = existing_thumbs.get(post_id)
        rows.append({
            "competitor_id": competitor_id,
            "platform": platform,
            "post_id": post_id,
            "post_url": post["url"],
            "posted_at": post["published_at"],
            "caption": post["caption"],
            "likes": likes,
            "comments": comments,
            "views": post["view_count"],
            "thumbnail_url": thumbnail_url,
        })
        if follower_count:
            engagements.append(((likes or 0) + (comments or 0)) / follower_count)

    written = upsert_competitor_posts(rows)

    # engagement_rate — fraction (e.g. 0.043) averaged over the selected posts.
    engagement_rate = round(sum(engagements) / len(engagements), 6) if engagements else None

    # Accumulate per-platform columns only (skip-None preserves the other channel).
    stats[f"{platform}_avatar_url"] = avatar_url
    stats[f"{platform}_follower_count"] = follower_count
    stats[f"{platform}_engagement_rate"] = engagement_rate
    stats[f"{platform}_post_count"] = profile.get("post_count")

    # is_self → record a follower snapshot for this channel and accumulate
    # content_stats over a wider window (decoupled from the card cap).
    if is_self:
        _snapshot_self_follower(platform, follower_count)
        self_cutoff = now - timedelta(days=SELF_CONTENT_STATS_LOOKBACK_DAYS)
        for dt, post in normalised:
            if dt < self_cutoff:
                continue
            likes = post["like_count"]
            comments = post["comment_count"]
            shares = post["share_count"]
            saves = post["save_count"]
            views = post["view_count"]
            # Engagement-by-reach proxy: true reach/unique-views needs the platform
            # owner analytics API; with the public scrape we divide by views.
            interactions = (likes or 0) + (comments or 0) + (shares or 0) + (saves or 0)
            engagement_rate = round(interactions / views, 6) if views else None
            content_stats_rows.append({
                "platform": platform,
                "post_id": post["post_id"],
                "post_url": post["url"],
                "views": views,
                "likes": likes,
                "comments": comments,
                "shares": shares,
                "saves": saves,
                "caption": post["caption"] or None,
                "hashtags": post["hashtags"] or None,
                "duration_sec": post["duration_sec"],
                "engagement_rate": engagement_rate,
            })

    logger.info(
        "Competitor '%s' (@%s, %s): follower_count=%s / %d posts / engagement=%s",
        name, handle, platform, follower_count, len(rows), engagement_rate,
    )
    log_source_run(name, _RUN_CATEGORY, "ok", len(rows))
    return written


def _run_competitor(competitor: dict) -> int:
    """Scrape one competitor (up to two channels). Never raises — logs and returns
    the total competitor_posts written across its channels."""
    competitor_id = competitor.get("id")
    is_self = bool(competitor.get("is_self"))
    base_name = (competitor.get("display_name") or "competitor").strip()

    channels = _resolve_channels(competitor)
    if not channels:
        logger.warning("Competitor '%s' has no instagram/tiktok channel — skipping", base_name)
        log_source_run(base_name, _RUN_CATEGORY, "error", 0, "No channel configured")
        return 0

    stats: dict = {}
    content_stats_rows: list = []
    written = 0

    for channel in channels:
        platform = channel["platform"]
        handle = channel["handle"]
        name = f"{base_name} ({platform})"
        try:
            written += _run_channel(
                competitor_id, name, platform, handle, is_self, stats, content_stats_rows
            )
        except Exception as exc:
            logger.warning(
                "Apify %s fetch for competitor '%s' (@%s) failed: %s",
                platform, base_name, handle, exc,
            )
            log_source_run(name, _RUN_CATEGORY, "error", 0, str(exc)[:500])
            # Continue to the other channel; skip-None preserves this channel's columns.

    # Single stats write — mark the refresh complete (last_refreshed_at is what the
    # admin Refresh card polls for). Per-platform columns only; skip-None means an
    # absent/failed channel never blanks the other's data.
    update_competitor_stats(competitor_id, {
        **stats,
        "refresh_status": "idle",
        "last_refreshed_at": datetime.now(timezone.utc).isoformat(),
    })

    if is_self and content_stats_rows:
        upsert_self_content_stats(content_stats_rows)

    return written


def run_competitors(competitor_id: str | None = None) -> None:
    """
    Run the competitor scrape via Apify. Pass `competitor_id` to refresh a single
    competitor (a manual Refresh from the admin card); omit it for the daily job
    that refreshes everyone.
    """
    if not APIFY_TOKEN:
        logger.warning("APIFY_TOKEN not set — skipping competitor run")
        return

    competitors = get_competitors(competitor_id)
    if not competitors:
        scope = f" for id={competitor_id}" if competitor_id else ""
        logger.info("No competitors to scrape%s — skipping competitor run", scope)
        return

    total_posts = 0
    for competitor in competitors:
        total_posts += _run_competitor(competitor)

    logger.info(
        "Competitor run: %d posts across %d competitors", total_posts, len(competitors)
    )
