"""
Outstand (outstand.so) — OAuth-connected self-account Instagram Insights, run hourly
and independent of the Apify competitor flow (ingestion/competitors.py). This is now
the SOLE source for The Curve's self-Instagram channel — Apify's public scrape for
that one channel is retired (see competitors.py's _resolve_channels); it never had
shares/saves/reach/accounts_engaged/total_interactions anyway (live-verified
2026-08-06, and flagged unsolved since migrations/025_content_stats_enrichment.sql).
Competitor Instagram tracking is unaffected and stays on Apify — Outstand can't reach
accounts we don't own.

Writes everything the retired self-Instagram Apify channel used to, so nothing
downstream regresses:
  - content_stats (real Insights, not the old proxy/null fields)
  - follower_snapshots + social_accounts.follower_count (plus the account-level
    trailing-30-day engagement aggregate into the *_30d columns, migration 034 —
    rolling-window totals, never SUM/diff across days)
  - competitors.instagram_{avatar_url,follower_count,post_count,engagement_rate} +
    competitor_posts (the leaderboard/comparison card Admin renders The Curve's row
    alongside competitors in) — skipped if no is_self competitors row exists.
  - EXCEPT transcripts: Outstand has no transcript field, so new self-Instagram posts
    won't get one going forward (previously stored transcripts on old posts remain).

Two-step data model:
  1. import organic posts into Outstand (POST /social-accounts/{id}/imports, billed
     per post) incrementally off a stored watermark
     (social_accounts.outstand_last_imported_at) — the watermark only ever advances
     to the newest post's published_at we've *confirmed exists* in Outstand's own
     post list, never to "now" or a job's self-reported status, so a later run's
     `since` can't re-request a window we've already paid for even if the import
     job's own dedup is unreliable (live-tested: it ignores the idempotency-key
     header, and a same-window rerun sat queued behind the first job rather than
     proving anything about dedup).
  2. pull analytics (GET /posts/{id}/analytics, GET /social-accounts/{id}/metrics) —
     pure reads, safe to refresh hourly for any post still within the lookback window.

Import jobs were observed (live pilot, 2026-08-06) to serialize one-at-a-time per
account and can sit at status="running" indefinitely — imported count stops moving —
rather than ever reaching "completed" for a small backfill. Polling is bounded by
OUTSTAND_IMPORT_POLL_TIMEOUT_SEC; a plateaued count is treated as "done enough" for
this run rather than waiting for "completed", which may never arrive.

content_stats/competitor_posts key posts by Outstand's platform_post_id (the native
Instagram media id) going forward. A handful of older Apify-scraped rows for the same
real posts used a different id format and are left as-is — harmless, stale duplicates
that age out of the lookback windows naturally.

Never raises — failures log and skip, matching ingestion/competitors.py's convention.
"""

import logging
import time
from datetime import datetime, timedelta, timezone

import httpx

from config import (
    OUTSTAND_API_KEY,
    OUTSTAND_API_BASE,
    OUTSTAND_IMPORT_LIMIT,
    OUTSTAND_IMPORT_POLL_TIMEOUT_SEC,
    OUTSTAND_INITIAL_BACKFILL_DAYS,
    OUTSTAND_INITIAL_BACKFILL_LIMIT,
    COMPETITOR_POST_LIMIT,
    COMPETITOR_LOOKBACK_DAYS,
    SELF_CONTENT_STATS_LOOKBACK_DAYS,
)
from ingestion.storage import (
    get_follower_snapshot_history,
    get_outstand_connected_accounts,
    get_self_competitor_id,
    update_outstand_watermark,
    update_social_account_follower_count,
    upsert_follower_snapshot,
    upsert_self_content_stats,
    update_competitor_stats,
    upsert_competitor_posts,
    get_existing_post_thumbnails,
    get_existing_content_stats_thumbnails,
    store_competitor_image,
    log_source_run,
)

logger = logging.getLogger(__name__)

_RUN_CATEGORY = "outstand"
_PILOT_PLATFORMS = ("instagram",)  # scope: Instagram only for the pilot
_POLL_INTERVAL_SEC = 5
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
# Outstand media URLs are signed IG CDN links that expire within days — only
# attempt thumbnail downloads for posts young enough that the URL can be live.
_THUMBNAIL_FETCH_MAX_AGE_DAYS = 14


def _headers() -> dict:
    return {"Authorization": f"Bearer {OUTSTAND_API_KEY}"}


def _get(path: str, params: dict | None = None) -> dict:
    resp = httpx.get(f"{OUTSTAND_API_BASE}{path}", headers=_headers(), params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _post(path: str, payload: dict) -> dict:
    resp = httpx.post(f"{OUTSTAND_API_BASE}{path}", headers=_headers(), json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _iso_z(dt: datetime) -> str:
    """Outstand's `since`/`until` validator only accepts a Z-suffixed UTC datetime —
    Python's default isoformat() emits '+00:00', which it live-tested as a 400
    'Invalid datetime' (2026-08-06)."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _extract_media_url(post: dict) -> str | None:
    containers = post.get("containers") or []
    media = (containers[0].get("media") or []) if containers else []
    return media[0].get("url") if media else None


def _extract_caption(post: dict) -> str | None:
    containers = post.get("containers") or []
    return containers[0].get("content") if containers else None


def _fetch_account_metrics(outstand_account_id: str) -> dict:
    """GET /social-accounts/{id}/metrics — account-level Insights aggregate."""
    payload = _get(f"/social-accounts/{outstand_account_id}/metrics")
    return payload.get("data") or {}


_follower_history: dict[str, list[tuple[str, int]]] = {}


def _followers_at(platform: str, published_at: str | None) -> int | None:
    """
    Follower count from the latest follower_snapshots row on or before the post date.

    engagement_audience is defined as interactions / followers-AT-TIME, so an older
    post must not be divided by today's audience (we grew from ~45.3k in April to
    ~48.7k in August, which would understate every older post). Loaded once per
    process and reused; falls back to the earliest snapshot for a post that predates
    our history, and returns None when we have no snapshots for the platform at all.
    """
    if platform not in _follower_history:
        rows = get_follower_snapshot_history(platform)
        _follower_history[platform] = [
            (str(recorded)[:10], count) for recorded, count in rows
        ]
    history = _follower_history[platform]
    if not history:
        return None
    if not published_at:
        return history[-1][1]
    stamp = str(published_at)[:10]
    prior = [count for day, count in history if day <= stamp]
    return prior[-1] if prior else history[0][1]


def _list_posts(outstand_account_id: str) -> list[dict]:
    """GET /posts?social_account_id=... — every post Outstand currently knows about
    for this account (imported organic posts + anything published through Outstand).
    Paginates (default page size 50) — live-checked 2026-08-06: total=91 but a single
    unpaginated call only returned 50, silently truncating everything older, which is
    exactly what caused the initial-backfill gap to look "fixed" at the import layer
    but still be missing from content_stats."""
    posts: list[dict] = []
    offset = 0
    while True:
        payload = _get("/posts", params={"social_account_id": outstand_account_id, "offset": offset})
        page = payload.get("data") or payload.get("posts") or []
        posts.extend(page)
        pagination = payload.get("pagination") or {}
        total = pagination.get("total")
        if not page or total is None or len(posts) >= total:
            break
        offset += len(page)
    return posts


def _start_import(outstand_account_id: str, since: datetime, limit: int) -> str | None:
    payload = _post(
        f"/social-accounts/{outstand_account_id}/imports",
        {"since": _iso_z(since), "limit": limit},
    )
    job = payload.get("data") or {}
    return job.get("id")


def _wait_for_import(outstand_account_id: str, import_id: str) -> dict:
    """
    Poll the import job, bounded by OUTSTAND_IMPORT_POLL_TIMEOUT_SEC. Returns the last
    known job dict. Treats a plateaued (imported+skipped+failed) count as done — jobs
    have been observed to never reach status="completed" for a small backfill.
    """
    deadline = time.monotonic() + OUTSTAND_IMPORT_POLL_TIMEOUT_SEC
    last_total = -1
    job: dict = {}
    while time.monotonic() < deadline:
        payload = _get(f"/social-accounts/{outstand_account_id}/imports/{import_id}")
        job = payload.get("data") or {}
        if job.get("status") in ("completed", "failed"):
            return job
        total = (job.get("imported") or 0) + (job.get("skipped") or 0) + (job.get("failed") or 0)
        if total == last_total and total > 0:
            return job  # plateaued — good enough, stop waiting on a job that may never flip
        last_total = total
        time.sleep(_POLL_INTERVAL_SEC)
    return job


def _import_new_posts(outstand_account_id: str, since: datetime, limit: int) -> None:
    """Kick off and wait (bounded) for an incremental import job. Best-effort — a
    failure here just means this run's post list won't include anything new."""
    import_id = _start_import(outstand_account_id, since, limit)
    if not import_id:
        return
    job = _wait_for_import(outstand_account_id, import_id)
    logger.info(
        "Outstand import %s: status=%s imported=%s skipped=%s failed=%s",
        import_id, job.get("status"), job.get("imported"), job.get("skipped"), job.get("failed"),
    )


def _fetch_post_analytics_row(post: dict, outstand_account_id: str, platform: str) -> dict | None:
    """GET /posts/{id}/analytics, normalised to an upsert_self_content_stats row."""
    payload = _get(f"/posts/{post['id']}/analytics")
    for entry in payload.get("metrics_by_account") or []:
        social_account = entry.get("social_account") or {}
        if social_account.get("id") != outstand_account_id and social_account.get("network") != platform:
            continue
        metrics = entry.get("metrics") or {}
        if metrics.get("token_expired") or entry.get("metrics_error"):
            logger.warning(
                "Outstand analytics error for post %s: %s", post["id"], entry.get("metrics_error"),
            )
            return None
        interactions = (
            (metrics.get("likes") or 0) + (metrics.get("comments") or 0)
            + (metrics.get("shares") or 0) + (metrics.get("saves") or 0)
        )
        views = metrics.get("views")
        reach = metrics.get("reach")
        published_at = entry.get("published_at") or post.get("publishedAt")
        followers = _followers_at(platform, published_at)
        return {
            "platform": platform,
            "post_id": entry.get("platform_post_id") or post["id"],
            "post_url": entry.get("platform_post_url"),
            "posted_at": entry.get("published_at") or post.get("publishedAt"),
            "caption": _extract_caption(post),
            "views": views,
            "likes": metrics.get("likes"),
            "comments": metrics.get("comments"),
            "shares": metrics.get("shares"),
            "saves": metrics.get("saves"),
            "reach": reach,
            # Outstand's own engagement_rate is on a percent-like scale (e.g. 7.78),
            # NOT the 0-1 fraction convention engagement_reach/engagement_audience use
            # below — stored as-is (the real source value); see instagram_engagement_
            # rate on competitors, which is deliberately recomputed to match that
            # fraction convention instead, for apples-to-apples brand comparison.
            "engagement_rate": metrics.get("engagement_rate"),
            # engagement_reach is interactions / VIEWS despite the name (migration
            # 026's proxy, kept because it's the only engagement figure every source
            # can produce). engagement_on_reach is the real thing — interactions over
            # the unique accounts that saw the post. Outstand is the ONLY source that
            # gives us reach, so it's the only writer here; both stay 0-1 fractions.
            "engagement_reach": round(interactions / views, 6) if views else None,
            # reach < interactions is impossible (an account can't interact without
            # being reached), so treat it as a broken denominator and store nothing
            # rather than a >100% rate. It bit the backfill: a YouTube row carried an
            # admin-entered reach of 8 against 201 views and 10 interactions.
            "engagement_on_reach": (
                round(interactions / reach, 6)
                if reach and reach >= interactions
                else None
            ),
            # interactions / followers-at-time. Outstand's payload has no follower
            # count per post, but follower_snapshots is our own daily series, so the
            # denominator comes from there rather than being left unset.
            "engagement_audience": (
                round(interactions / followers, 6)
                if followers and interactions else None
            ),
            "accounts_engaged": None,
            "total_interactions": interactions or None,
            "platform_specific": metrics.get("platform_specific"),
        }
    return None


def _attach_thumbnails(platform: str, in_window: list[dict], analytics: dict[str, dict]) -> None:
    """Persist each post's first media image into the competitor-thumbnails bucket
    (posts/{platform}_{post_id}.jpg — the same object the competitor card writes)
    and stamp the public URL onto the analytics row for content_stats. Posts that
    already have a content_stats thumbnail are skipped, so the hourly run doesn't
    re-download the whole 90-day window; a failed fetch leaves None, which the
    skip-None update ignores and the next run retries while the post is still
    fresh. Outstand's media URLs are signed Instagram CDN links that expire after
    a few days (live-verified 2026-08-11: the 90-day backfill 403'd on all but the
    2 newest posts), so posts older than _THUMBNAIL_FETCH_MAX_AGE_DAYS are never
    attempted — their URLs are guaranteed dead and retrying them hourly is pure
    churn. Thumbnails therefore only accrue forward, captured while each post is
    new. For reels the media URL may be the mp4 itself; store_competitor_image
    rejects non-image content, leaving None. Best-effort throughout (it never
    raises)."""
    existing = get_existing_content_stats_thumbnails(
        platform, [row["post_id"] for row in analytics.values()]
    )
    posts_by_id = {p.get("id"): p for p in in_window}
    fetch_cutoff = datetime.now(timezone.utc) - timedelta(days=_THUMBNAIL_FETCH_MAX_AGE_DAYS)
    for outstand_id, row in analytics.items():
        thumb = existing.get(row["post_id"])
        if not thumb:
            post = posts_by_id.get(outstand_id) or {}
            if (_parse_dt(post.get("publishedAt")) or _EPOCH) >= fetch_cutoff:
                thumb = store_competitor_image(
                    _extract_media_url(post),
                    f"posts/{platform}_{row['post_id']}.jpg",
                )
        row["thumbnail_url"] = thumb


def _write_competitor_card(competitor_id: str, platform: str, posts: list[dict],
                            analytics: dict[str, dict], metrics: dict) -> int:
    """
    Populate what the retired Apify self-Instagram channel used to write onto the
    competitors row + competitor_posts (the Admin leaderboard/comparison card) —
    capped the same way as competitor rows (COMPETITOR_POST_LIMIT/LOOKBACK_DAYS) so
    The Curve's card stays comparable. Best-effort; never raises (caller catches).
    """
    follower_count = metrics.get("followers_count")
    avatar_url = store_competitor_image(
        (metrics.get("platform_specific") or {}).get("profile_picture_url"),
        f"avatars/{competitor_id}_{platform}.jpg",
    )

    cutoff = datetime.now(timezone.utc) - timedelta(days=COMPETITOR_LOOKBACK_DAYS)
    selected = [p for p in posts if (_parse_dt(p.get("publishedAt")) or _EPOCH) >= cutoff][:COMPETITOR_POST_LIMIT]
    selected_rows = [(p, analytics[p["id"]]) for p in selected if p.get("id") in analytics]

    existing_thumbs = get_existing_post_thumbnails(competitor_id, [row["post_id"] for _, row in selected_rows])

    post_rows = []
    engagements = []
    for post, row in selected_rows:
        post_id = row["post_id"]
        # _attach_thumbnails already persisted this post's image (same storage
        # path) and stamped the row; fall back to the prior competitor_posts value
        # so a failed fetch never blanks an existing thumbnail.
        thumbnail_url = row.get("thumbnail_url") or existing_thumbs.get(post_id)
        likes, comments = row.get("likes"), row.get("comments")
        post_rows.append({
            "competitor_id": competitor_id,
            "platform": platform,
            "post_id": post_id,
            "post_url": row.get("post_url"),
            "posted_at": row.get("posted_at"),
            "caption": _extract_caption(post),
            "likes": likes,
            "comments": comments,
            "views": row.get("views"),
            "thumbnail_url": thumbnail_url,
            "transcript": None,  # Outstand has no transcript field
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
        "instagram_post_count": metrics.get("posts_count"),
        "instagram_engagement_rate": engagement_rate,
    })
    return written


def _run_account(platform: str, account: dict, competitor_id: str | None) -> None:
    outstand_account_id = account["outstand_account_id"]
    social_account_id = account["social_account_id"]
    name = f"The Curve ({platform}, outstand)"
    content_rows_written = 0
    try:
        metrics = _fetch_account_metrics(outstand_account_id)
        follower_count = metrics.get("followers_count")
        if follower_count is not None:
            # engagement block = trailing ~30-day rolling totals (period.since/until),
            # stored in follower_snapshots *_30d columns (migration 034).
            upsert_follower_snapshot(
                social_account_id, platform, follower_count,
                engagement_30d=metrics.get("engagement"),
            )
            update_social_account_follower_count(social_account_id, follower_count)

        watermark = _parse_dt(account.get("last_imported_at"))
        since = watermark or (datetime.now(timezone.utc) - timedelta(days=OUTSTAND_INITIAL_BACKFILL_DAYS))
        # No watermark yet = first-ever run for this account: the import endpoint
        # returns the MOST RECENT posts within [since, now], not a chronological page,
        # so a small limit here would silently truncate to only the newest few and
        # never reach the rest of the target window (see OUTSTAND_INITIAL_BACKFILL_LIMIT).
        import_limit = OUTSTAND_INITIAL_BACKFILL_LIMIT if watermark is None else OUTSTAND_IMPORT_LIMIT
        _import_new_posts(outstand_account_id, since, import_limit)

        posts = _list_posts(outstand_account_id)

        newest = max((_parse_dt(p.get("publishedAt")) for p in posts if p.get("publishedAt")), default=None)
        if newest and (watermark is None or newest > watermark):
            update_outstand_watermark(social_account_id, newest.isoformat())

        stats_cutoff = datetime.now(timezone.utc) - timedelta(days=SELF_CONTENT_STATS_LOOKBACK_DAYS)
        in_window = [p for p in posts if (_parse_dt(p.get("publishedAt")) or _EPOCH) >= stats_cutoff]

        analytics: dict[str, dict] = {}
        for post in in_window:
            try:
                row = _fetch_post_analytics_row(post, outstand_account_id, platform)
            except Exception as exc:
                logger.warning("Outstand analytics fetch failed for post %s: %s", post.get("id"), str(exc)[:200])
                continue
            if row:
                analytics[post["id"]] = row

        if analytics:
            _attach_thumbnails(platform, in_window, analytics)
            content_rows_written = upsert_self_content_stats(list(analytics.values()))

        if competitor_id:
            try:
                _write_competitor_card(competitor_id, platform, posts, analytics, metrics)
            except Exception as exc:
                logger.warning("Outstand %s competitor-card write failed: %s", platform, str(exc)[:300])

        logger.info(
            "Outstand %s: followers=%s / %d content_stats rows refreshed",
            platform, follower_count, content_rows_written,
        )
        log_source_run(name, _RUN_CATEGORY, "ok", content_rows_written)
    except Exception as exc:
        logger.warning("Outstand %s run failed: %s", platform, str(exc)[:300])
        log_source_run(name, _RUN_CATEGORY, "error", 0, str(exc)[:500])


def run_outstand_hourly() -> None:
    """
    Entry point for the hourly scheduled job (and the manual POST /run/outstand
    endpoint / --stage outstand). Never raises — each connected platform is
    best-effort independent.
    """
    if not OUTSTAND_API_KEY:
        logger.warning("OUTSTAND_API_KEY not set — skipping Outstand run")
        return
    accounts = get_outstand_connected_accounts()
    if not accounts:
        logger.info("No Outstand-connected self accounts yet — skipping")
        return
    competitor_id = get_self_competitor_id()
    for platform in _PILOT_PLATFORMS:
        account = accounts.get(platform)
        if account:
            _run_account(platform, account, competitor_id)
