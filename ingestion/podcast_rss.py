"""
RSS download analytics from OP3 (op3.dev).

OP3 is the open-source prefix analytics service already wrapped around every enclosure in
the Flightcast feed (`https://op3.dev/e/episode.flightcast.com/<ulid>.mp3`). It is a
plain public REST API with a bearer token — no OAuth, no browser extension, no
reverse-engineering — so unlike the Spotify path this runs server-side on the scheduler.

WHAT IT PROVIDES, AND WHAT IT CANNOT
    A prefix sees a request for the audio file and nothing after it. So there is no
    completion, no retention and no listen time here, and OP3 by design never collects
    age or gender. What it gives that nothing else does per episode is COUNTRY, APP and
    DEVICE.

⚠️ NOT A SOURCE OF TRUTH FOR PLAYS
    OP3 only counts downloads made after the prefix was added to the feed, so an episode
    published before that shows its back-catalogue trickle rather than its real total —
    understated, and plausibly so. Rows like that are flagged `rss_partial`. Flightcast
    is the master for play counts (it reports every platform on one basis for the show's
    whole history); this module is for the dimensions only.

⚠️ SPOTIFY AND YOUTUBE ARE ABSENT, ON PURPOSE
    Both ingest the feed once to their own CDN and serve listeners from that copy, so
    their plays never touch the prefix (verified 2026-09-07: zero Spotify entries across
    32 apps and 77,911 downloads; Apple Podcasts is 87.5%). The three sources therefore
    measure disjoint audiences and add rather than overlap.

THE JOIN IS IN TWO PARTS
    The aggregate endpoint returns `itemGuid` matching podcast_episodes.guid exactly, so
    totals join directly. The RAW download rows identify the episode by audio URL, and
    the file's ULID is NOT the guid's ULID — they share a timestamp prefix and differ in
    the random suffix on 422 of 424 episodes. The RSS feed is the only mapping, so the
    geo/app breakdown parses it.

Never raises: a failure returns status='error' and logs one source_runs row, matching the
discipline of every other stage here.
"""

from __future__ import annotations

import base64
import logging
import time
import re
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any

import httpx

from config import (
    OP3_TOKEN,
    PODCAST_RSS_FEED_URL,
    OP3_MAX_PAGES,
)
from ingestion.storage import (
    get_client,
    get_social_account_by_platform,
    log_source_run,
    upsert_audience_demographics,
)

logger = logging.getLogger(__name__)

_API = "https://op3.dev/api/1"
_RUN_NAME = "OP3 RSS downloads"
_RUN_CATEGORY = "podcast_rss"
_TIMEOUT = 45
_PAGE = 1000

# Only these four have columns; everything else folds into ROW, matching the hand-entered
# podcast country rows rather than Instagram's full ISO-2 set.
_GEO_COLUMNS = {
    "NZ": "rss_geo_downloads_nz",
    "AU": "rss_geo_downloads_au",
    "GB": "rss_geo_downloads_gb",
    "US": "rss_geo_downloads_us",
}


def _get(path: str, params: dict[str, Any], attempts: int = 3) -> dict[str, Any]:
    """One OP3 call, retried on transient failures.

    Deep pages get slower as the offset grows and do time out occasionally — seen live on
    a full-history sweep — so a single slow page must not decide the run.
    """
    params = {**params, "token": OP3_TOKEN, "format": "json"}
    last: Exception | None = None
    for attempt in range(attempts):
        try:
            resp = httpx.get(f"{_API}{path}", params=params, timeout=_TIMEOUT)
            resp.raise_for_status()
            return resp.json() or {}
        except Exception as exc:
            last = exc
            if attempt < attempts - 1:
                time.sleep(2 * (attempt + 1))
    raise last if last else RuntimeError("OP3 request failed")


def resolve_show_uuid(feed_url: str) -> dict[str, Any] | None:
    """OP3 identifies a show by uuid; look it up from the feed URL as urlsafe base64."""
    b64 = base64.urlsafe_b64encode(feed_url.encode()).decode().rstrip("=")
    try:
        body = _get(f"/shows/{b64}", {})
    except Exception as exc:
        logger.warning("OP3 show lookup failed: %s", str(exc)[:200])
        return None
    return body if body.get("showUuid") else None


def _feed_file_map(feed_url: str) -> dict[str, str]:
    """{audio file ULID -> item guid}, parsed from the feed.

    Required because OP3's raw rows key on the audio URL and the file ULID differs from
    the guid ULID on all but 2 of 424 episodes. Regex rather than an XML parser: the feed
    is 3.6MB and we want two fields per item, not a DOM.
    """
    resp = httpx.get(feed_url, timeout=_TIMEOUT, follow_redirects=True)
    resp.raise_for_status()
    xml = resp.text
    out: dict[str, str] = {}
    for item in re.findall(r"<item>(.*?)</item>", xml, re.S):
        guid = re.search(r"<guid[^>]*>(.*?)</guid>", item, re.S)
        enc = re.search(r'<enclosure[^>]*url="([^"]+)"', item)
        if not (guid and enc):
            continue
        ulid = re.search(r"/([0-9A-Za-z]{20,32})\.mp3", enc.group(1))
        if ulid:
            out[ulid.group(1)] = guid.group(1).strip()
    return out


def _month_bounds(d: date) -> tuple[str, str]:
    first = d.replace(day=1)
    nxt = (first.replace(year=first.year + 1, month=1) if first.month == 12
           else first.replace(month=first.month + 1))
    return first.isoformat(), nxt.isoformat()


def _tracking_start(show_uuid: str, max_months: int = 60) -> date | None:
    """The date OP3 first saw a download, found by stepping back month by month.

    ⚠️ EVERY OP3 QUERY MUST BE DATE-BOUNDED. An unbounded `start=2015-01-01` makes OP3
    scan the whole range and times out even after retries (seen live), while the same
    query with an `end` returns in under five seconds. So we probe bounded months
    backwards until one comes back empty, rather than asking for "the earliest row".

    Not taken from the sweep window: a bounded run (say -30d) would otherwise report its
    own window edge as the tracking start and mislabel every episode's `rss_partial`.
    """
    cursor = date.today()
    earliest: date | None = None
    for _ in range(max_months):
        start, end = _month_bounds(cursor)
        try:
            body = _get(f"/downloads/show/{show_uuid}",
                        {"limit": 1, "start": start, "end": end, "bots": "exclude"})
        except Exception as exc:
            logger.warning("OP3 tracking-start probe failed at %s: %s", start, str(exc)[:150])
            return earliest
        rows = body.get("rows", []) or []
        if not rows:
            # An empty month before any data has been seen just means the show was quiet
            # that month; once we HAVE seen data, an empty month is the boundary.
            if earliest:
                return earliest
        else:
            try:
                earliest = datetime.fromisoformat(rows[0]["time"].replace("Z", "+00:00")).date()
            except Exception:
                pass
        cursor = (cursor.replace(day=1) - timedelta(days=1))
    return earliest


def _episode_totals(show_uuid: str) -> dict[str, dict[str, Any]]:
    """{guid -> {downloadsAll, downloads7, pubdate}} from OP3's aggregate endpoint."""
    body = _get("/queries/episode-download-counts", {"showUuid": show_uuid})
    out: dict[str, dict[str, Any]] = {}
    for ep in body.get("episodes", []) or []:
        guid = ep.get("itemGuid")
        if guid:
            out[guid] = ep
    return out


def _aggregate_downloads(show_uuid: str, start: str) -> tuple[dict, dict, str | None, bool]:
    """Page the raw download rows, aggregating per episode and show-wide.

    Returns (per_file, show_totals, earliest_time, complete). `per_file` is keyed by the
    audio ULID, since that is what the rows carry; the caller maps it to guids via the
    feed.

    ⚠️ `complete` is load-bearing. These are TOTALS, written by overwrite, so a sweep that
    stopped early would replace a correct figure with a smaller one — a silent undercount
    that looks like a real drop in downloads. The caller must not write when it is False.
    """
    per_file: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"total": 0, "geo": defaultdict(int), "app": defaultdict(int)}
    )
    show: dict[str, Any] = {
        "country": defaultdict(int), "app": defaultdict(int), "device": defaultdict(int)
    }
    earliest: str | None = None
    pages = 0

    # PAGING IS BY `startAfter`, NOT a continuation token. OP3 returns rows in ASCENDING
    # time order from `start`, capped at `limit`, and returns NO continuationToken despite
    # the parameter existing — a loop waiting for one stops after a single page and
    # silently under-counts (verified: `-7d` returns exactly 1000 rows covering only the
    # oldest ~24h of that window). So we walk forward from the last row's timestamp.
    #
    # `startAfter` is exclusive on time, so rows sharing the last row's exact millisecond
    # would be skipped. OP3 orders by time plus uuid to break ties, so this can drop a
    # row in the rare case of a page boundary landing mid-millisecond; at 1000-row pages
    # that is a handful of downloads across a full history, and the alternative (overlap
    # and dedupe on uuid) costs a re-fetch of every page.
    # Walk MONTH BY MONTH. An open-ended query times out (OP3 scans from `start` with no
    # upper bound); the same range split into months returns in seconds each.
    months: list[tuple[str, str]] = []
    probe = date.fromisoformat(start[:10]) if start[:1].isdigit() else date.today()
    today = date.today()
    while probe <= today:
        m_start, m_next = _month_bounds(probe)
        months.append((m_start, m_next))
        probe = date.fromisoformat(m_next)
    month_idx = 0
    cursor_in_month: str | None = None

    while pages < OP3_MAX_PAGES and month_idx < len(months):
        m_start, m_end = months[month_idx]
        params: dict[str, Any] = {"limit": _PAGE, "bots": "exclude", "end": m_end}
        params["startAfter" if cursor_in_month else "start"] = cursor_in_month or m_start
        try:
            body = _get(f"/downloads/show/{show_uuid}", params)
        except Exception as exc:
            logger.warning("OP3 page %s failed after retries: %s", pages, str(exc)[:200])
            return per_file, show, earliest, False
        rows = body.get("rows", []) or []
        if not rows:
            month_idx += 1
            cursor_in_month = None
            continue
        last_time = None
        for row in rows:
            t = row.get("time")
            if t:
                last_time = t
                if earliest is None or t < earliest:
                    earliest = t
            country = (row.get("countryCode") or "??").upper()
            app = row.get("agentName") or "Unknown"
            device = row.get("deviceType") or "unknown"
            show["country"][country] += 1
            show["app"][app] += 1
            show["device"][device] += 1
            m = re.search(r"/([0-9A-Za-z]{20,32})\.mp3", row.get("url") or "")
            if not m:
                continue
            bucket = per_file[m.group(1)]
            bucket["total"] += 1
            bucket["geo"][country] += 1
            bucket["app"][app] += 1
        pages += 1
        if len(rows) < _PAGE or not last_time:
            month_idx += 1
            cursor_in_month = None
        else:
            cursor_in_month = last_time

    if pages >= OP3_MAX_PAGES:
        logger.warning("OP3 page cap (%s) hit — raise OP3_MAX_PAGES", OP3_MAX_PAGES)
        return per_file, show, earliest, False
    return per_file, show, earliest, True


def _episode_rows(guid_totals: dict, per_guid: dict, tracking_start: date | None,
                  warnings: list[str]) -> list[dict[str, Any]]:
    """One podcast_episodes update per episode we can key, or []."""
    client = get_client()
    try:
        resp = client.table("podcast_episodes").select("id,guid,pub_date").execute()
        episodes = resp.data or []
    except Exception as exc:
        warnings.append(f"podcast_episodes read failed: {str(exc)[:200]}")
        return []

    by_guid = {e["guid"]: e for e in episodes if e.get("guid")}
    now = datetime.now(timezone.utc).isoformat()
    updates: list[dict[str, Any]] = []

    for guid, agg in per_guid.items():
        episode = by_guid.get(guid)
        if not episode:
            continue
        total = agg["total"]
        if not total:
            continue
        geo = agg["geo"]
        row: dict[str, Any] = {
            "id": episode["id"],
            "rss_downloads_all": total,
            "rss_updated_at": now,
        }
        row.update({col: geo.get(code, 0) for code, col in _GEO_COLUMNS.items()})
        row["rss_geo_downloads_row"] = sum(
            n for code, n in geo.items() if code not in _GEO_COLUMNS
        )
        apple = agg["app"].get("Apple Podcasts", 0)
        row["rss_apple_pct"] = round(apple * 100 / total, 1)

        # Partial when the episode predates tracking: OP3 then holds only what it saw
        # after the prefix went on, which is not the episode's real download total.
        pub = episode.get("pub_date")
        if tracking_start and pub:
            try:
                pub_date = datetime.fromisoformat(str(pub).replace("Z", "+00:00")).date()
                row["rss_partial"] = pub_date < tracking_start
            except Exception:
                pass

        official = guid_totals.get(guid, {})
        if official.get("downloads7") is not None:
            row["rss_downloads_7d"] = official["downloads7"]
        updates.append(row)
    return updates


def _write_episodes(updates: list[dict[str, Any]], warnings: list[str]) -> int:
    client = get_client()
    written = 0
    for update in updates:
        episode_id = update.pop("id")
        try:
            client.table("podcast_episodes").update(update).eq("id", episode_id).execute()
            written += 1
        except Exception as exc:
            warnings.append(f"podcast_episodes update failed for {episode_id}: {str(exc)[:200]}")
    return written


def _write_show_demographics(show: dict, warnings: list[str]) -> int:
    """Show-level country / app / device into audience_demographics at platform='rss'.

    A distinct platform value keeps these disjoint from the Spotify rows, the Instagram
    rows and the hand-entered podcast rows, all of which share this table. Note these
    count DOWNLOADS, not people — the same caution as the Spotify rows.
    """
    account = get_social_account_by_platform("spotify")
    social_account_id = account["id"] if account else None
    today = date.today().isoformat()
    rows: list[dict[str, Any]] = []
    for dimension in ("country", "app", "device"):
        for bucket, value in (show.get(dimension) or {}).items():
            if not value:
                continue
            rows.append({
                "social_account_id": social_account_id,
                "platform": "rss",
                "dimension": dimension,
                "bucket": str(bucket)[:100],
                "value": value,
                "value_type": "count",
                "snapshot_date": today,
            })
    if not rows:
        return 0
    return upsert_audience_demographics(rows)


def run_podcast_rss(start: str = "2020-01-01") -> dict[str, Any]:
    """Pull OP3 download analytics into podcast_episodes + audience_demographics."""
    warnings: list[str] = []
    if not OP3_TOKEN:
        logger.warning("OP3_TOKEN not set — RSS stage skipped")
        return {"status": "skipped", "reason": "OP3_TOKEN not set"}
    if not PODCAST_RSS_FEED_URL:
        return {"status": "skipped", "reason": "PODCAST_RSS_FEED_URL not set"}

    try:
        show = resolve_show_uuid(PODCAST_RSS_FEED_URL)
        if not show:
            log_source_run(_RUN_NAME, _RUN_CATEGORY, "error", 0, "show not found in OP3")
            return {"status": "error", "reason": "show not found in OP3"}
        show_uuid = show["showUuid"]

        file_map = _feed_file_map(PODCAST_RSS_FEED_URL)
        guid_totals = _episode_totals(show_uuid)
        per_file, show_agg, earliest, complete = _aggregate_downloads(show_uuid, start)

        # Roll the per-file aggregates up to guids via the feed map.
        per_guid: dict[str, dict[str, Any]] = {}
        unmapped = 0
        for ulid, agg in per_file.items():
            guid = file_map.get(ulid)
            if not guid:
                unmapped += 1
                continue
            per_guid[guid] = agg
        if unmapped:
            warnings.append(f"{unmapped} audio file ids not present in the feed (deleted episodes?)")

        tracking_start = _tracking_start(show_uuid)
        if tracking_start is None and earliest:
            # Fall back to the window edge, but only when the dedicated lookup failed —
            # and warn, because rss_partial computed from a window edge is wrong.
            warnings.append("tracking-start lookup failed; rss_partial derived from the sweep window")
            try:
                tracking_start = datetime.fromisoformat(earliest.replace("Z", "+00:00")).date()
            except Exception:
                pass

        if not complete:
            # Refuse to write rather than overwrite good totals with an undercount.
            reason = "sweep incomplete (page failure or page cap) — nothing written"
            warnings.append(reason)
            logger.warning("OP3: %s", reason)
            log_source_run(_RUN_NAME, _RUN_CATEGORY, "error", 0, reason)
            return {"status": "incomplete", "reason": reason,
                    "episodes_seen": len(per_guid), "warnings": warnings}

        updates = _episode_rows(guid_totals, per_guid, tracking_start, warnings)
        written = _write_episodes(updates, warnings)
        demographics = _write_show_demographics(show_agg, warnings)

        for warning in warnings:
            logger.warning("OP3: %s", warning)
        message = (
            f"{written} episodes, {demographics} demographic rows, "
            f"tracking since {tracking_start or 'unknown'}"
        )
        log_source_run(_RUN_NAME, _RUN_CATEGORY, "ok", written, message)
        return {
            "status": "ok",
            "episodes_written": written,
            "demographic_rows": demographics,
            "tracking_since": str(tracking_start) if tracking_start else None,
            "warnings": warnings,
        }
    except Exception as exc:
        logger.exception("OP3 RSS stage failed")
        log_source_run(_RUN_NAME, _RUN_CATEGORY, "error", 0, str(exc)[:300])
        return {"status": "error", "reason": str(exc)[:300], "warnings": warnings}
