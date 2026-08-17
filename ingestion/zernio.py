"""
Zernio (zernio.com) — OAuth-connected self-account Insights, run hourly and
independent of the Apify competitor flow (ingestion/competitors.py). Replaces the
retired Outstand integration wholesale; the reasons for the move were comments and
DMs (see ingestion/inbox.py), but Zernio also covers everything Outstand did plus
audience demographics, story insights, a daily follower series and per-post Reels
watch time.

This is the SOLE source for The Curve's self-Instagram channel — Apify's public
scrape for that one channel stays retired (see competitors.py's _resolve_channels);
it never had shares/saves/reach/accounts_engaged/total_interactions anyway.
Competitor Instagram tracking is unaffected and stays on Apify — Zernio can only
read accounts we've connected.

Writes exactly what the Outstand flow wrote, so nothing downstream regresses:
  - content_stats (real Insights, not the old proxy/null fields), now also with
    impressions/clicks/follows and Reels watch time (migration 037)
  - follower_snapshots + social_accounts.follower_count (plus the account-level
    trailing-30-day engagement aggregate into the *_30d columns, migration 034 —
    rolling-window totals, never SUM/diff across days)
  - competitors.instagram_{avatar_url,follower_count,post_count,engagement_rate} +
    competitor_posts (the leaderboard/comparison card Admin renders The Curve's row
    alongside competitors in) — skipped if no is_self competitors row exists.
  - audience_demographics (new — Outstand had no demographics endpoint at all)
  - EXCEPT transcripts: Zernio has no transcript field either, so new self-Instagram
    posts still don't get one (previously stored transcripts on old posts remain).

The two-step import model is GONE. Outstand billed per imported post and needed a
watermark (social_accounts.outstand_last_imported_at) to keep that cost bounded;
Zernio syncs each connected account's external posts on its own background cycle
(~90 minutes, ~12 months retained) and analytics reads are plain GETs. So there is
no job to poll, no watermark to advance, and no window we can accidentally pay for
twice. POST /v1/posts/sync-external exists to force a sync for a just-published
post, and is deliberately not called here — the hourly cadence is soon enough, and
the admin's publish path is where "confirm this post exists" belongs.

Row identity is unchanged: content_stats/competitor_posts key posts by the platform's
own media id, which Zernio returns as platforms[].platformPostId — the same 18-digit
Instagram id Outstand wrote. A post with no platformPostId is SKIPPED rather than
falling back to Zernio's internal _id: a row keyed on a vendor id would never
reconcile with the one already there, and a silent duplicate is worse than a gap.

Zernio's own `engagementRate` is deliberately NOT stored. It is a percentage whose
denominator is the first non-zero of impressions, reach, views — so it changes basis
from post to post and is not comparable with the values already in the column. Every
rate here is computed locally on a fixed basis instead (see _content_stats_row).

Never raises — failures log and skip, matching ingestion/competitors.py's convention.
"""

import logging
import time
from datetime import date, datetime, timedelta, timezone

import httpx

from config import (
    ZERNIO_API_KEY,
    ZERNIO_API_BASE,
    ZERNIO_FOLLOWER_HISTORY_DAYS,
    COMPETITOR_POST_LIMIT,
    COMPETITOR_LOOKBACK_DAYS,
    SELF_CONTENT_STATS_LOOKBACK_DAYS,
)
from ingestion.storage import (
    get_follower_snapshot_history,
    get_zernio_connected_accounts,
    get_self_competitor_id,
    update_social_account_follower_count,
    upsert_audience_demographics,
    upsert_follower_snapshot,
    upsert_self_content_stats,
    update_competitor_stats,
    upsert_competitor_posts,
    get_existing_post_thumbnails,
    get_existing_content_stats_thumbnails,
    get_recent_story_rows,
    store_competitor_image,
    log_source_run,
)

logger = logging.getLogger(__name__)

_RUN_CATEGORY = "zernio"
# Scope: Instagram only, as the Outstand pilot was. The account-insights, demographics
# and stories endpoints below are Instagram-specific by design (Zernio exposes separate
# per-platform equivalents); the post-analytics and follower-stats paths are generic and
# will work unchanged for tiktok/linkedin/youtube once those channels are connected.
_PLATFORMS = ("instagram",)
# Stories are a different surface from feed posts — different metrics, 24h life, and an
# order-of-magnitude different reach — so they get their own platform value rather than
# polluting feed-post averages on every chart that groups by platform.
_STORY_PLATFORM = "instagram_story"
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
# Media URLs in the payload are signed CDN links that expire within days — only attempt
# thumbnail downloads for posts young enough that the URL can still be live.
_THUMBNAIL_FETCH_MAX_AGE_DAYS = 14
_ANALYTICS_PAGE_LIMIT = 100  # endpoint maximum
_ANALYTICS_MAX_PAGES = 20    # backstop against a pagination bug looping forever
_MAX_RETRIES = 3
_ACCOUNT_INSIGHT_DAYS = 30   # matches the *_30d column semantics
# How far back the story settle pass re-asks for final metrics. Meta expires a story at
# 24h; 48 gives a full day of slack for a webhook that lands late without re-walking
# history every hour.
_STORY_SETTLE_HOURS = 48
_DEMOGRAPHIC_BREAKDOWNS = ("age", "city", "country", "gender")
# The account-level metrics we ask for, mapped 1:1 onto follower_snapshots' *_30d
# columns. All are total_value-only on Meta's side except reach, which is the sole
# metric supporting time_series — an Instagram Graph API limitation, not Zernio's.
_ACCOUNT_INSIGHT_METRICS = (
    "reach", "views", "accounts_engaged", "total_interactions",
    "likes", "comments", "shares", "saves",
)


# ── HTTP ───────────────────────────────────────────────────────────────────────

def _headers() -> dict:
    return {"Authorization": f"Bearer {ZERNIO_API_KEY}"}


def _retry_wait(resp: httpx.Response, fallback: float) -> float:
    """Seconds to wait before retrying, preferring the server's own instruction.

    Retry-After is authoritative and immune to clock skew, so it wins. X-RateLimit-Reset
    is a unix timestamp and only usable if our clock is roughly right, so it is the
    second choice. Capped at 60s: this runs hourly, and a longer sleep would just hold a
    worker open for a window the next run covers anyway.
    """
    header = resp.headers.get("Retry-After")
    if header:
        try:
            return min(float(header), 60.0)
        except ValueError:
            pass
    reset = resp.headers.get("X-RateLimit-Reset")
    if reset:
        try:
            return max(0.0, min(float(reset) - time.time(), 60.0))
        except ValueError:
            pass
    return min(fallback, 60.0)


def _request(method: str, path: str, *, params: dict | None = None,
             json: dict | None = None) -> dict:
    """
    One Zernio call, retrying 429s and 5xx with backoff. Outstand's client had no
    retry at all, which meant a single transient failure silently dropped a post (or a
    whole account) for the hour. Zernio's limits are far above our usage — 600 req/min,
    10 analytics req/s — so a 429 here means something unusual, not routine pressure.

    4xx other than 429 raise immediately: they are contract errors, not weather. 402
    (analytics_addon_required) in particular means the plan lacks analytics, and no
    amount of retrying fixes that.
    """
    url = f"{ZERNIO_API_BASE}{path}"
    delay = 2.0
    for attempt in range(_MAX_RETRIES):
        resp = httpx.request(
            method, url, headers=_headers(), params=params, json=json, timeout=30,
        )
        if resp.status_code == 429 or resp.status_code >= 500:
            if attempt == _MAX_RETRIES - 1:
                resp.raise_for_status()
            wait = _retry_wait(resp, delay)
            logger.info(
                "Zernio %s %s → %s, retrying in %.1fs", method, path, resp.status_code, wait,
            )
            time.sleep(wait)
            delay *= 2
            continue
        resp.raise_for_status()
        return resp.json()
    return {}


def _get(path: str, params: dict | None = None) -> dict:
    return _request("GET", path, params=params)


# Shared transport for ingestion/inbox.py — same key, same retry policy, same base URL.
# Exported rather than duplicated so there is one place where Zernio's rate-limit
# behaviour is handled.
zernio_get = _get
zernio_request = _request


# ── helpers ────────────────────────────────────────────────────────────────────

def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _iso_date(dt: datetime | date) -> str:
    """Zernio's date-range params are plain YYYY-MM-DD."""
    return dt.strftime("%Y-%m-%d")


def _count(value) -> int | None:
    """A non-negative count, or None.

    Negatives map to None: every call site is a count, and a negative one is a vendor
    sentinel, not data. That was a real incident on the Apify path — Instagram returns
    likesCount = -1 when the poster has HIDDEN their like count, which sailed through as
    a negative interaction total and printed a negative engagement rate.
    """
    if value is None:
        return None
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    return number if number >= 0 else None


def _nz(value) -> int | None:
    """A count, with 0 treated as ABSENT rather than as a real zero.

    Zernio returns 0 — not null — for a metric it could not obtain: follows on a reel,
    watch time on a non-video, any insight still inside Meta's ~24h delay, anything on a
    post whose sync hasn't completed. upsert_self_content_stats only skips None, so a 0
    would be written, and a stored 0 reads as "this post reached nobody" while also
    permanently overwriting the good value from the previous hour.

    Only for metrics where 0 genuinely cannot be distinguished from unavailable:
    views, reach, impressions, clicks, follows, the watch times, video duration.

    NOT for likes/comments/shares/saves — a real post can honestly have zero of those,
    and mapping them to None would make a genuine zero unwritable forever (skip-None
    would preserve a stale non-zero count indefinitely).
    """
    number = _count(value)
    return number or None


_follower_history: dict[str, list[tuple[str, int]]] = {}


def reset_follower_cache() -> None:
    """Drop the per-process followers-at-time cache.

    Called at the start of every run. The Outstand version loaded this once and never
    invalidated it, so in the long-lived scheduler process the denominator behind every
    engagement_audience froze at whatever the history looked like when the container
    last started — and quietly drifted further from the truth the longer it stayed up.
    """
    _follower_history.clear()


def _followers_at(platform: str, published_at: str | None) -> int | None:
    """
    Follower count from the latest follower_snapshots row on or before the post date.

    engagement_audience is defined as interactions / followers-AT-TIME, so an older
    post must not be divided by today's audience (we grew from ~45.3k in April to
    ~48.8k in August, which would understate every older post). Falls back to the
    earliest snapshot for a post that predates our history, and returns None when we
    have no snapshots for the platform at all.
    """
    if platform not in _follower_history:
        rows = get_follower_snapshot_history(platform)
        _follower_history[platform] = [(str(recorded)[:10], count) for recorded, count in rows]
    history = _follower_history[platform]
    if not history:
        return None
    if not published_at:
        return history[-1][1]
    stamp = str(published_at)[:10]
    prior = [count for day, count in history if day <= stamp]
    return prior[-1] if prior else history[0][1]


# ── fetchers ───────────────────────────────────────────────────────────────────

def _fetch_account_insights(zernio_account_id: str) -> dict:
    """
    GET /v1/analytics/instagram/account-insights — account-level totals over a trailing
    30 days, which is exactly the window follower_snapshots' *_30d columns describe.

    These are whole-account figures across every surface (feed, stories, explore,
    profile) and are fundamentally different from the sum of post-level metrics — never
    reconcile the two. Meta delays them up to 48 hours.

    Returns {metric: total} shaped for upsert_follower_snapshot's engagement_30d.
    """
    until = datetime.now(timezone.utc).date()
    since = until - timedelta(days=_ACCOUNT_INSIGHT_DAYS)
    payload = _get("/v1/analytics/instagram/account-insights", {
        "accountId": zernio_account_id,
        "metrics": ",".join(_ACCOUNT_INSIGHT_METRICS),
        "since": _iso_date(since),
        "until": _iso_date(until),
        "metricType": "total_value",
    })
    metrics = payload.get("metrics") or {}
    return {
        key: block.get("total")
        for key, block in metrics.items()
        if isinstance(block, dict) and block.get("total") is not None
    }


def _fetch_follower_series(zernio_account_id: str) -> tuple[dict, list[tuple[date, int]]]:
    """
    GET /v1/accounts/follower-stats — the account's current profile AND its daily
    follower series, in one call, for any platform.

    Chosen over /v1/analytics/instagram/follower-history, which returns the same
    underlying data (both are served from Zernio's cross-platform daily snapshotter,
    which exists because Meta removed follower_count from /insights in Graph API v22+
    and never exposed a historical daily series). follower-stats is not Instagram-only,
    and its account entries extend the full SocialAccount schema — so the current
    follower count, the profile picture and the total media count all arrive here too.
    That is one call where Outstand needed a separate account-metrics fetch, and it
    keeps working unchanged when TikTok/LinkedIn/YouTube are connected.

    Refreshed once per day upstream, so calling it hourly costs nothing new.

    Returns ({follower_count, avatar_url, post_count}, [(day, followers)]).
    """
    until = datetime.now(timezone.utc).date()
    since = until - timedelta(days=ZERNIO_FOLLOWER_HISTORY_DAYS)
    payload = _get("/v1/accounts/follower-stats", {
        "accountIds": zernio_account_id,
        "fromDate": _iso_date(since),
        "toDate": _iso_date(until),
        "granularity": "daily",
    })
    profile: dict = {}
    for account in payload.get("accounts") or []:
        if account.get("_id") != zernio_account_id:
            continue
        stats = account.get("accountStats") or {}
        profile = {
            "follower_count": _count(
                account.get("currentFollowers") or account.get("followersCount")
            ),
            "avatar_url": account.get("profilePicture"),
            # mediaCount is Instagram's lifetime post total. The equivalents on other
            # platforms are named differently (videoCount, postsCount, pinCount), so
            # this stays Instagram-shaped until another channel is actually connected.
            "post_count": _count(stats.get("mediaCount")),
            "needs_reconnection": bool(account.get("needsReconnection")),
            "username": account.get("username"),
        }
        break
    series: list[tuple[date, int]] = []
    for point in (payload.get("stats") or {}).get(zernio_account_id) or []:
        day = _parse_dt(point.get("date"))
        followers = _count(point.get("followers"))
        if day and followers:
            series.append((day.date(), followers))
    series.sort()
    return profile, series


def _fetch_post_analytics(zernio_account_id: str, platform: str) -> list[dict]:
    """
    GET /v1/analytics?source=external — every post the platform published for this
    account inside the content_stats lookback, with its metrics.

    source=external is the important parameter: it selects posts published directly on
    the platform (which is all of ours today) rather than only those authored through
    Zernio. Paginated; each page is at most 100.
    """
    until = datetime.now(timezone.utc).date()
    since = until - timedelta(days=SELF_CONTENT_STATS_LOOKBACK_DAYS)
    posts: list[dict] = []
    page = 1
    while page <= _ANALYTICS_MAX_PAGES:
        payload = _get("/v1/analytics", {
            "source": "external",
            "accountId": zernio_account_id,
            "platform": platform,
            "fromDate": _iso_date(since),
            "toDate": _iso_date(until),
            "limit": _ANALYTICS_PAGE_LIMIT,
            "page": page,
        })
        batch = payload.get("posts") or []
        posts.extend(batch)
        pagination = payload.get("pagination") or {}
        pages = pagination.get("pages")
        if not batch or not pages or page >= pages:
            break
        page += 1
    else:
        logger.warning(
            "Zernio post analytics hit the %d-page cap for %s — results truncated",
            _ANALYTICS_MAX_PAGES, platform,
        )
    return posts


def _fetch_demographics(zernio_account_id: str) -> dict[str, list[dict]]:
    """
    GET /v1/analytics/instagram/demographics — follower audience by age, city, country
    and gender. Requires 100+ followers (we have ~48.8k) and returns the top 45 entries
    per dimension, so buckets do NOT sum to the follower total. Delayed up to 48 hours.
    """
    payload = _get("/v1/analytics/instagram/demographics", {
        "accountId": zernio_account_id,
        "metric": "follower_demographics",
        "breakdown": ",".join(_DEMOGRAPHIC_BREAKDOWNS),
        "timeframe": "this_month",
    })
    if payload.get("success") is False:
        logger.warning("Zernio demographics unavailable: %s", payload.get("error"))
        return {}
    return payload.get("demographics") or {}


def _fetch_stories(zernio_account_id: str) -> list[dict]:
    """GET /v1/accounts/{id}/instagram/stories — currently-ACTIVE stories only."""
    payload = _get(f"/v1/accounts/{zernio_account_id}/instagram/stories")
    return payload.get("data") or []


def _fetch_story_insights(zernio_account_id: str, story_id: str) -> dict:
    """
    GET /v1/accounts/{id}/instagram/stories/{storyId}/insights.

    `source` discriminates three states: `live` (still active), `cached` (expired, but
    Zernio holds the final-state metrics Meta pushed on its story webhook) and
    `unavailable` (expired and no webhook was ever received). Meta reports an expired
    story as an empty success rather than an error, so `unavailable` is a normal
    outcome, not a fault.
    """
    payload = _get(f"/v1/accounts/{zernio_account_id}/instagram/stories/{story_id}/insights")
    return payload.get("data") or {}


# ── normalisers ────────────────────────────────────────────────────────────────

def _match_platform_entry(post: dict, zernio_account_id: str, platform: str) -> dict | None:
    """
    The per-account slice of a post's analytics.

    Exact accountId first; only then a platform match, and only when the entry carries
    no accountId of its own to contradict it. The Outstand version of this check was
    `id != wanted and network != platform`, which passes an entry belonging to a
    DIFFERENT account of the same network — harmless with one connected account, wrong
    the moment there are two.
    """
    entries = post.get("platforms") or []
    for entry in entries:
        if entry.get("accountId") == zernio_account_id:
            return entry
    for entry in entries:
        if entry.get("platform") == platform and not entry.get("accountId"):
            return entry
    return None


def _thumbnail_source(post: dict) -> str | None:
    """First usable image URL for the post.

    Prefers the post-level thumbnail; falls back to the first media item's thumbnail
    (never its `url`, which for a reel is the mp4 itself). mediaStatus is present only
    when the platform withheld the file — Instagram does this for reels it flags as
    carrying copyrighted material — and retrying never helps.
    """
    if post.get("thumbnailUrl"):
        return post["thumbnailUrl"]
    for item in post.get("mediaItems") or []:
        if item.get("mediaStatus"):
            continue
        if item.get("thumbnail"):
            return item["thumbnail"]
    return None


def _content_stats_row(post: dict, zernio_account_id: str, platform: str) -> dict | None:
    """One post's analytics, normalised to an upsert_self_content_stats row."""
    entry = _match_platform_entry(post, zernio_account_id, platform)
    if entry is None:
        return None
    post_id = entry.get("platformPostId")
    if not post_id:
        # No native id means we cannot key this against the row that already exists for
        # the same post. Skipping loses an hour of freshness; guessing would mint a
        # permanent duplicate keyed on a vendor id.
        logger.warning("Zernio post %s has no platformPostId — skipped", post.get("_id"))
        return None
    if entry.get("status") == "failed":
        return None
    if entry.get("syncStatus") in ("pending", "unavailable"):
        # An unsynced post comes back with a fully zero-filled analytics object rather
        # than an error. Writing it would blank every metric we already hold for the
        # post. This is the direct analogue of the Outstand flow's token_expired guard.
        logger.info(
            "Zernio post %s not synced yet (%s) — skipped", post_id, entry.get("syncStatus"),
        )
        return None

    # The per-platform entry, never post["analytics"] — that block is the aggregate
    # ACROSS platforms, so on a cross-posted item it would double-count.
    metrics = entry.get("analytics") or {}
    likes = _count(metrics.get("likes"))
    comments = _count(metrics.get("comments"))
    shares = _count(metrics.get("shares"))
    saves = _count(metrics.get("saves"))
    views = _nz(metrics.get("views"))
    reach = _nz(metrics.get("reach"))
    interactions = (likes or 0) + (comments or 0) + (shares or 0) + (saves or 0)
    posted_at = post.get("publishedAt")
    followers = _followers_at(platform, posted_at)

    return {
        "platform": platform,
        "post_id": post_id,
        "post_url": entry.get("platformPostUrl") or post.get("platformPostUrl"),
        "posted_at": posted_at,
        "caption": post.get("content"),
        "views": views,
        "likes": likes,
        "comments": comments,
        "shares": shares,
        "saves": saves,
        "reach": reach,
        "impressions": _nz(metrics.get("impressions")),
        "clicks": _nz(metrics.get("clicks")),
        # Instagram feed posts and stories only — Meta returns 0 for reels, so a 0 here
        # is "not measured on this media type" as often as it is a real zero.
        "follows": _nz(metrics.get("follows")),
        "duration_sec": _nz(metrics.get("videoDurationSeconds")),
        "avg_watch_time_ms": _nz(metrics.get("igReelsAvgWatchTime")),
        "total_watch_time_ms": _nz(metrics.get("igReelsVideoViewTotalTime")),
        # Percent on a FIXED basis: interactions ÷ REACH × 100. That is empirically what
        # every one of the 100 Outstand-written rows in this column already encodes
        # (checked against the live table — total_interactions / (engagement_rate/100)
        # equals reach exactly on each of them), so keeping the same basis is what makes
        # the column mean one thing either side of the vendor swap.
        #
        # Zernio's own engagementRate is deliberately NOT stored: its denominator is
        # whichever of impressions/reach/views is first non-zero, so it can silently
        # change basis from post to post with nothing in the row to say which was used,
        # and it arrives rounded to 2dp where this column holds full precision. The raw
        # vendor value is kept in platform_specific for reconciliation.
        "engagement_rate": (
            round(interactions / reach * 100, 6)
            if reach and reach >= interactions else None
        ),
        # engagement_reach is interactions / VIEWS despite the name (migration 026's
        # proxy, kept because it's the only engagement figure every source can produce).
        # engagement_on_reach is the real thing — interactions over the unique accounts
        # that saw the post. Both stay 0-1 fractions.
        "engagement_reach": round(interactions / views, 6) if views else None,
        # reach < interactions is impossible (an account can't interact without being
        # reached), so treat it as a broken denominator and store nothing rather than a
        # >100% rate. It bit the Outstand backfill: a YouTube row carried an
        # admin-entered reach of 8 against 201 views and 10 interactions.
        "engagement_on_reach": (
            round(interactions / reach, 6) if reach and reach >= interactions else None
        ),
        # interactions / followers-at-time. The payload carries no follower count per
        # post, so the denominator comes from our own follower_snapshots series.
        "engagement_audience": (
            round(interactions / followers, 6) if followers and interactions else None
        ),
        "accounts_engaged": None,  # account-level only; no per-post equivalent exists
        "total_interactions": interactions or None,
        "platform_specific": {
            "source": "zernio",
            "zernio_post_id": post.get("_id"),
            "media_type": post.get("mediaType"),
            "sync_status": entry.get("syncStatus"),
            "metrics_last_updated": metrics.get("lastUpdated"),
            # Kept for provenance/debugging only — see the engagement_rate note above
            # for why it is not the stored rate.
            "vendor_engagement_rate": metrics.get("engagementRate"),
        },
        "_thumb_src": _thumbnail_source(post),
    }


def _story_row(story: dict, insights: dict, platform: str = _STORY_PLATFORM) -> dict | None:
    """One story's insights, normalised to a content_stats row.

    Stored under a distinct platform value so stories never mix into feed-post averages.
    Meta applies a privacy floor to small audiences: counts below 5 come back as 0. That
    is the platform's answer, not a missing value, and it is stored as-is.
    """
    story_id = story.get("id")
    if not story_id:
        return None
    metrics = insights.get("metrics") or {}
    # An expired story with no cached webhook payload returns a fully ZERO-FILLED
    # metrics object rather than an error, so writing it would overwrite a good
    # earlier `live` read with zeros.
    if insights.get("source") == "unavailable" or not metrics:
        return None
    views = _nz(metrics.get("views"))
    reach = _nz(metrics.get("reach"))
    interactions = _count(metrics.get("totalInteractions")) or 0
    return {
        "platform": platform,
        "post_id": story_id,
        "post_url": story.get("permalink"),
        "posted_at": story.get("timestamp"),
        "views": views,
        "reach": reach,
        "replies": _count(metrics.get("replies")),
        "shares": _count(metrics.get("shares")),
        "follows": _nz(metrics.get("follows")),
        "total_interactions": interactions or None,
        # sticker_taps is DELIBERATELY not written. Meta exposes no sticker-tap metric
        # for stories — `navigation` is tap-forward/back/exit/swipe, a different thing —
        # and the column is hand-entered by the operator in the admin's Socials tab. A 0
        # here would destroy typed-in data with no audit trail. The navigation detail
        # lives in platform_specific below instead.
        #
        # No engagement_* rates either: a story's interactions-over-views is a different
        # question from a feed post's (a tap-forward is not an interaction), so reusing
        # the feed formula would put two incomparable numbers in one column.
        "platform_specific": {
            "source": "zernio",
            "insights_source": insights.get("source"),
            "media_type": story.get("mediaType"),
            "navigation": metrics.get("navigation"),
            "taps_forward": metrics.get("tapsForward"),
            "taps_back": metrics.get("tapsBack"),
            "exits": metrics.get("exits"),
            "swipes_forward": metrics.get("swipesForward"),
            "profile_visits": metrics.get("profileVisits"),
            "reposts": metrics.get("reposts"),
        },
        "_thumb_src": story.get("thumbnailUrl") or story.get("mediaUrl"),
    }


# ── writers ────────────────────────────────────────────────────────────────────

def _attach_thumbnails(platform: str, rows: list[dict]) -> None:
    """
    Persist each post's image into the competitor-thumbnails bucket
    (posts/{platform}_{post_id}.jpg — the same object the competitor card writes) and
    stamp the public URL onto the row. Posts that already have a content_stats
    thumbnail are skipped, so the hourly run doesn't re-download the whole 90-day
    window; a failed fetch leaves None, which the skip-None update ignores and the next
    run retries while the post is still fresh.

    Media URLs are signed CDN links that expire after a few days, so posts older than
    _THUMBNAIL_FETCH_MAX_AGE_DAYS are never attempted — their URLs are guaranteed dead
    and retrying them hourly is pure churn. Thumbnails therefore only accrue forward,
    captured while each post is new. store_competitor_image rejects non-image content,
    so a video URL that slips through leaves None rather than a broken .jpg.

    Best-effort throughout (it never raises).
    """
    existing = get_existing_content_stats_thumbnails(platform, [r["post_id"] for r in rows])
    fetch_cutoff = datetime.now(timezone.utc) - timedelta(days=_THUMBNAIL_FETCH_MAX_AGE_DAYS)
    for row in rows:
        thumb = existing.get(row["post_id"])
        if not thumb and (_parse_dt(row.get("posted_at")) or _EPOCH) >= fetch_cutoff:
            thumb = store_competitor_image(
                row.get("_thumb_src"), f"posts/{platform}_{row['post_id']}.jpg",
            )
        row["thumbnail_url"] = thumb


def _write_competitor_card(competitor_id: str, platform: str, rows: list[dict],
                           follower_count: int | None, post_count: int | None,
                           avatar_source: str | None) -> int:
    """
    Populate what the retired Apify self-Instagram channel used to write onto the
    competitors row + competitor_posts (the Admin leaderboard/comparison card) —
    capped the same way as competitor rows (COMPETITOR_POST_LIMIT/LOOKBACK_DAYS) so
    The Curve's card stays comparable. Best-effort; never raises (caller catches).
    """
    avatar_url = store_competitor_image(
        avatar_source, f"avatars/{competitor_id}_{platform}.jpg",
    )
    cutoff = datetime.now(timezone.utc) - timedelta(days=COMPETITOR_LOOKBACK_DAYS)
    selected = [
        row for row in rows
        if (_parse_dt(row.get("posted_at")) or _EPOCH) >= cutoff
    ][:COMPETITOR_POST_LIMIT]

    existing_thumbs = get_existing_post_thumbnails(
        competitor_id, [row["post_id"] for row in selected],
    )

    post_rows = []
    engagements = []
    for row in selected:
        post_id = row["post_id"]
        likes, comments = row.get("likes"), row.get("comments")
        post_rows.append({
            "competitor_id": competitor_id,
            "platform": platform,
            "post_id": post_id,
            "post_url": row.get("post_url"),
            "posted_at": row.get("posted_at"),
            "caption": row.get("caption"),
            "likes": likes,
            "comments": comments,
            "views": row.get("views"),
            # _attach_thumbnails already persisted this post's image (same storage
            # path) and stamped the row; fall back to the prior competitor_posts value
            # so a failed fetch never blanks an existing thumbnail.
            "thumbnail_url": row.get("thumbnail_url") or existing_thumbs.get(post_id),
            "transcript": None,  # Zernio has no transcript field
        })
        if follower_count:
            engagements.append(((likes or 0) + (comments or 0)) / follower_count)

    written = upsert_competitor_posts(post_rows)
    # Fraction (e.g. 0.043), matching the same formula every Apify-scraped competitor
    # channel uses — kept consistent so The Curve's card is comparable to competitors'.
    engagement_rate = round(sum(engagements) / len(engagements), 6) if engagements else None
    update_competitor_stats(competitor_id, {
        "instagram_avatar_url": avatar_url,
        "instagram_follower_count": follower_count,
        "instagram_post_count": post_count,
        "instagram_engagement_rate": engagement_rate,
    })
    return written


def _backfill_follower_series(social_account_id: str, platform: str,
                              series: list[tuple[date, int]]) -> int:
    """
    Fill gaps in follower_snapshots from Zernio's daily series.

    Gap-fill only (insert_only): much of this series was typed in by hand, roughly
    monthly, going back to 2021, and it is the denominator behind every
    engagement_audience value. Replacing a hand-entered count with a vendor's
    reconstruction would rewrite history nobody could audit. Today's row is written by
    the main run, not here.
    """
    today = datetime.now(timezone.utc).date()
    filled = 0
    for day, followers in series:
        if day >= today:
            continue
        try:
            if upsert_follower_snapshot(
                social_account_id, platform, followers, day=day, insert_only=True,
            ):
                filled += 1
        except Exception as exc:
            logger.warning(
                "Follower backfill failed for %s %s: %s", platform, day, str(exc)[:200],
            )
    if filled:
        logger.info("Backfilled %d %s follower snapshots from Zernio", filled, platform)
    return filled


# Meta reports gender as single letters. audience_demographics is shared with
# hand-entered and podcast-sourced rows whose documented convention is the spelled-out
# word, and the table's natural key includes `bucket` — so writing "F" where the rest of
# the system writes "female" would split one audience across two buckets permanently,
# with nothing in the row to say which convention produced it.
_GENDER_BUCKETS = {"M": "male", "F": "female", "U": "unknown"}


def _bucket_name(dimension: str, raw: str) -> str:
    """Normalise a Zernio demographic bucket to this table's conventions.

    country: ISO-2 verbatim — the existing rows already use ISO-2 (AU/GB/NZ/US), so
    Zernio's format matches as-is.
    city: verbatim, including the region suffix ("New York, New York"). Stripping it
    would merge genuinely different cities that share a name, and the admin can format
    on read; what matters is that we are consistent, since the format is part of the key.
    """
    if dimension == "gender":
        return _GENDER_BUCKETS.get(str(raw).upper(), str(raw).lower())
    return str(raw)


def _write_demographics(social_account_id: str, platform: str,
                        demographics: dict[str, list[dict]]) -> int:
    """
    Flatten Zernio's per-dimension demographic lists into audience_demographics.

    Only follower_demographics is written. The table has no column distinguishing the
    follower audience from the engaged audience, so writing both would collide on the
    natural key and the second would silently overwrite the first. Engaged-audience
    demographics need an admin-side column first.

    snapshot_date is the scrape day, per the table's existing writer contract — even
    though Meta's demographics lag by up to 48 hours. Shifting the date to compensate
    would break idempotency against the admin's manually entered rows.
    """
    snapshot_date = _iso_date(datetime.now(timezone.utc).date())
    rows = []
    for dimension, buckets in demographics.items():
        for bucket in buckets or []:
            raw = bucket.get("dimension")
            value = bucket.get("value")
            if raw is None or value is None:
                continue
            rows.append({
                "social_account_id": social_account_id,
                "platform": platform,
                "dimension": dimension,
                "bucket": _bucket_name(dimension, raw),
                "value": value,
                "value_type": "count",
                "snapshot_date": snapshot_date,
            })
    return upsert_audience_demographics(rows)


# ── run ────────────────────────────────────────────────────────────────────────

def _story_insights_or_none(zernio_account_id: str, story_id: str) -> dict | None:
    try:
        return _fetch_story_insights(zernio_account_id, story_id)
    except Exception as exc:
        logger.warning("Zernio story insights failed for %s: %s", story_id, str(exc)[:200])
        return None


def _run_stories(zernio_account_id: str, platform: str) -> int:
    """
    Capture insights for Instagram stories, in two passes.

    ACTIVE pass — every story the account currently has live. This is why the job has
    to run hourly: Meta serves a story for 24 hours and this endpoint returns active
    ones only.

    SETTLE pass — stories we already stored that have since dropped off the active
    list. A story's numbers keep climbing until it expires, so whatever the last poll
    before expiry captured is systematically an undercount; Zernio can serve the
    FINAL-state metrics afterwards from the story_insights webhook Meta pushed it
    (source="cached"). Without this pass every story in the table is a mid-life
    snapshot. Stories already carrying a cached read are skipped — final is final.

    Still not exhaustive by construction: a story posted and expired entirely between
    two hourly runs is only recoverable if Zernio got that webhook, and live videos and
    reshared stories are never returned at all.
    """
    stories = _fetch_stories(zernio_account_id)
    rows = []
    active_ids = set()
    for story in stories:
        story_id = story.get("id")
        if not story_id:
            continue
        active_ids.add(story_id)
        insights = _story_insights_or_none(zernio_account_id, story_id)
        row = _story_row(story, insights or {})
        if row:
            rows.append(row)

    settle_cutoff = datetime.now(timezone.utc) - timedelta(hours=_STORY_SETTLE_HOURS)
    for stored in get_recent_story_rows(_STORY_PLATFORM, settle_cutoff.isoformat()):
        story_id = stored.get("post_id")
        provenance = stored.get("platform_specific") or {}
        if not story_id or story_id in active_ids:
            continue
        if provenance.get("insights_source") == "cached":
            continue
        insights = _story_insights_or_none(zernio_account_id, story_id)
        if not insights:
            continue
        # The stored row already holds the story's identity; only the metrics move.
        row = _story_row(
            {"id": story_id, "timestamp": stored.get("posted_at")}, insights,
        )
        if row:
            # Nothing to re-fetch — a thumbnail was captured (or permanently missed)
            # while the story was live, and its media URL is dead once it expires.
            row.pop("_thumb_src", None)
            rows.append(row)

    if not rows:
        return 0
    _attach_thumbnails(_STORY_PLATFORM, [r for r in rows if "_thumb_src" in r])
    return upsert_self_content_stats(rows)


def _run_account(platform: str, account: dict, competitor_id: str | None,
                 with_daily: bool) -> None:
    zernio_account_id = account["zernio_account_id"]
    social_account_id = account["social_account_id"]
    name = f"The Curve ({platform}, zernio)"
    content_rows_written = 0
    follower_count = None
    try:
        # 1. Account level — profile + follower count + the trailing-30-day engagement
        #    block.
        profile, series = _fetch_follower_series(zernio_account_id)
        if not profile:
            # The id in social_accounts.account_id doesn't match any account Zernio
            # will serve. Almost always the cutover step was missed and it still holds
            # the old provider's id. Never fall back to matching on username: that would
            # quietly write a different account's metrics into ours.
            raise RuntimeError(
                f"Zernio account {zernio_account_id} not found for {platform} — "
                "check social_accounts.account_id"
            )
        if profile.get("needs_reconnection"):
            # Zernio keeps SERVING analytics for an account whose OAuth token died —
            # they are simply frozen. Writing them would re-stamp stale numbers as fresh
            # every hour and make stats_synced_at a lie.
            logger.error(
                "Zernio %s account needs reconnection — skipping to avoid re-stamping "
                "stale metrics as fresh", platform,
            )
            log_source_run(name, _RUN_CATEGORY, "error", 0, "account needs reconnection")
            return
        follower_count = profile.get("follower_count")
        insights = {}
        if platform == "instagram":
            try:
                insights = _fetch_account_insights(zernio_account_id)
            except Exception as exc:
                logger.warning(
                    "Zernio account insights failed for %s: %s", platform, str(exc)[:200],
                )
        if follower_count is not None:
            upsert_follower_snapshot(
                social_account_id, platform, follower_count, engagement_30d=insights,
            )
            update_social_account_follower_count(social_account_id, follower_count)
        if series:
            _backfill_follower_series(social_account_id, platform, series)

        # 2. Post level.
        posts = _fetch_post_analytics(zernio_account_id, platform)
        rows = []
        for post in posts:
            try:
                row = _content_stats_row(post, zernio_account_id, platform)
            except Exception as exc:
                logger.warning(
                    "Zernio row build failed for post %s: %s", post.get("_id"), str(exc)[:200],
                )
                continue
            if row:
                rows.append(row)

        if rows:
            _attach_thumbnails(platform, rows)
            content_rows_written = upsert_self_content_stats(rows)

        # 3. Stories (Instagram only, hourly — they expire in 24h).
        if platform == "instagram":
            try:
                story_rows = _run_stories(zernio_account_id, platform)
                if story_rows:
                    logger.info("Zernio: %d story rows refreshed", story_rows)
            except Exception as exc:
                logger.warning("Zernio story run failed: %s", str(exc)[:300])

        # 4. Daily-only work: demographics move slowly and Meta delays them 48h, so
        #    there is nothing to gain from asking hourly.
        if with_daily and platform == "instagram":
            try:
                written = _write_demographics(
                    social_account_id, platform, _fetch_demographics(zernio_account_id),
                )
                logger.info("Zernio: %d audience_demographics rows written", written)
            except Exception as exc:
                logger.warning("Zernio demographics run failed: %s", str(exc)[:300])

        # 5. The Curve's own competitor card.
        if competitor_id and platform == "instagram":
            try:
                _write_competitor_card(
                    competitor_id, platform, rows, follower_count,
                    post_count=profile.get("post_count"),
                    avatar_source=profile.get("avatar_url"),
                )
            except Exception as exc:
                logger.warning(
                    "Zernio %s competitor-card write failed: %s", platform, str(exc)[:300],
                )

        logger.info(
            "Zernio %s: followers=%s / %d content_stats rows refreshed",
            platform, follower_count, content_rows_written,
        )
        log_source_run(name, _RUN_CATEGORY, "ok", content_rows_written)
    except Exception as exc:
        logger.warning("Zernio %s run failed: %s", platform, str(exc)[:300])
        log_source_run(name, _RUN_CATEGORY, "error", 0, str(exc)[:500])


def _run(with_daily: bool) -> None:
    if not ZERNIO_API_KEY:
        logger.warning("ZERNIO_API_KEY not set — skipping Zernio run")
        return
    accounts = get_zernio_connected_accounts()
    if not accounts:
        logger.info("No Zernio-connected self accounts yet — skipping")
        return
    reset_follower_cache()
    competitor_id = get_self_competitor_id()
    for platform in _PLATFORMS:
        account = accounts.get(platform)
        if account:
            _run_account(platform, account, competitor_id, with_daily)


def run_zernio_hourly() -> None:
    """
    Entry point for the hourly scheduled job (and the manual POST /run/zernio endpoint
    / --stage zernio). Post analytics, account insights, follower series and stories.

    Hourly is the natural cadence: Zernio caches post analytics for 60 minutes
    server-side, so a faster poll returns the same numbers, and stories expire in 24
    hours so a slower one loses them. Never raises — each connected platform is
    best-effort independent.
    """
    _run(with_daily=False)


def run_zernio_daily() -> None:
    """
    The hourly work plus the once-a-day extras (audience demographics). Called from the
    daily pipeline; safe to run on demand.
    """
    _run(with_daily=True)
