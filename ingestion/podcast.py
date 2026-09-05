"""
Spotify for Creators episode analytics — pushed by the Chrome extension.

Spotify has no public analytics API; the Curve Auth extension's "Send podcast stats"
button runs inside the operator's logged-in creators.spotify.com session, calls the
dashboard's own JSON endpoints, and POSTs the collected payload to /podcast/import,
which lands here. This module never talks to Spotify — everything arrives in the
payload, and everything it doesn't understand is kept verbatim in
platform_specific.vendor_raw so a field-name fix is a re-parse, not a re-fetch.

The vendor field names are UNVERIFIED until the first live run (the Zernio/Apify
lesson: every guessed spelling was wrong until checked), so every reader here accepts
the plausible spellings via _pick() and the collector ships the raw blobs regardless.

Where the data lands:
  • content_stats, platform='spotify', post_id = Spotify's episode id. views=plays,
    reach=listeners (both _nz-guarded: Spotify reports 0 for a metric it can't serve
    yet, and a written 0 would overwrite a real count and read as "nobody listened").
    Spotify-only vocabulary (starts/streams/completion/retention curve) goes in
    platform_specific. engagement_*, likes/comments/shares and calendar_item_id are
    never written — podcast metrics aren't that vocabulary, and podcast calendar
    items carry planning titles that can't be matched safely.
  • podcast_episodes (Admin-owned, matched by exact normalised title + pub_date
    within PODCAST_EPISODE_MATCH_WINDOW_DAYS): plays_spotify,
    spotify_avg_listen_minutes, spotify_completion_pct — OVERWRITTEN when a real
    value is present (operator-confirmed: plays is a monotone count, so the stale
    hand-entered figure is systematically an undercount). None/0 is never written.
    No match or an ambiguous match skips the link with a warning, never guesses.
  • audience_demographics (show-level; the table has no episode column):
    platform='spotify' + the spotify social_accounts row's id — keys disjoint from
    the hand-entered podcast rows, which sit at platform=NULL. Rows are only written
    when the payload declares value_type ('count' or 'percent'): percentages written
    as counts would corrupt the table's semantics, so an undeclared payload is
    skipped with a warning until the collector is told which Spotify returns.
  • follower_snapshots: today's row only, via the standard one-row-per-UTC-day
    upsert — the 12 hand-entered monthly spotify rows are untouchable by
    construction. Any future historical import must use insert_only=True.

Per-episode failures log a warning and the run continues; one source_runs row
(category 'podcast') per request. Never raises.
"""

import html as html_lib
import json
import logging
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any

from config import PODCAST_EPISODE_MATCH_WINDOW_DAYS
from ingestion.storage import (
    get_client,
    get_social_account_by_platform,
    log_source_run,
    update_social_account_follower_count,
    upsert_audience_demographics,
    upsert_follower_snapshot,
    upsert_self_content_stats,
)

logger = logging.getLogger(__name__)

_RUN_NAME = "Spotify for Creators"
_RUN_CATEGORY = "podcast"
_PLATFORM = "spotify"

# A stored vendor blob bigger than this is dropped (the parsed fields are kept):
# platform_specific is read back whole by the Admin, and one pathological episode
# must not bloat every listing query.
_VENDOR_RAW_MAX_BYTES = 50_000

_GENDER_MAP = {
    "m": "male", "male": "male",
    "f": "female", "female": "female",
    "not_specified": "unknown", "unknown": "unknown", "unspecified": "unknown",
    "u": "unknown",
    "non_binary": "non_binary", "nonbinary": "non_binary", "non-binary": "non_binary",
}


def _pick(source: dict | None, *keys: str) -> Any:
    """First non-None value among the plausible vendor spellings of one field."""
    if not source:
        return None
    for key in keys:
        value = source.get(key)
        if value is not None:
            return value
    return None


def _to_int(value: Any) -> int | None:
    try:
        if value is None or isinstance(value, bool):
            return None
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _to_float(value: Any) -> float | None:
    try:
        if value is None or isinstance(value, bool):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _nz(value: Any) -> int | None:
    """0 → None: Spotify reports 0 for a metric it can't serve (too-new episode),
    and skip-None is the only thing standing between that and a real count being
    overwritten. A genuine all-zero episode loses nothing — there is nothing to lose."""
    parsed = _to_int(value)
    return parsed if parsed else None


def _norm_title(title: str | None) -> str:
    """HTML-unescape (live podcast_episodes titles carry entities like &#39;),
    lowercase, collapse whitespace — the join key for episode matching."""
    if not title:
        return ""
    return re.sub(r"\s+", " ", html_lib.unescape(title)).strip().lower()


def _parse_date(value: Any) -> date | None:
    """ISO string / epoch seconds / epoch milliseconds → date."""
    if value is None:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        ts = float(value)
        if ts > 1e12:  # milliseconds
            ts /= 1000.0
        try:
            return datetime.fromtimestamp(ts, tz=timezone.utc).date()
        except (OverflowError, OSError, ValueError):
            return None
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).date()
        except ValueError:
            return None
    return None


# ── Episode matching against the Admin's podcast_episodes table ────────────────

def _fetch_admin_episodes() -> dict[str, list[dict[str, Any]]]:
    """{normalised title: [{id, pub_date}]} for every Admin podcast_episodes row.

    Paginated — the table is 441 rows today, and the day it passes PostgREST's
    1000-row cap must not silently halve the match rate."""
    client = get_client()
    by_title: dict[str, list[dict[str, Any]]] = {}
    page_size = 1000
    offset = 0
    while True:
        response = (
            client.table("podcast_episodes")
            .select("id, title, pub_date")
            .order("id")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        rows = response.data or []
        for row in rows:
            key = _norm_title(row.get("title"))
            if key:
                by_title.setdefault(key, []).append(
                    {"id": row["id"], "pub_date": _parse_date(row.get("pub_date"))}
                )
        if len(rows) < page_size:
            break
        offset += page_size
    return by_title


def _match_episode(
    by_title: dict[str, list[dict[str, Any]]],
    title: str | None,
    release_date: date | None,
    warnings: list[str],
) -> str | None:
    """podcast_episodes uuid for one Spotify episode, or None.

    Exact normalised-title match AND pub_date within the window — a miss is an
    unlinked content_stats row plus a warning, never a guessed link."""
    key = _norm_title(title)
    if not key:
        return None
    candidates = by_title.get(key) or []
    if release_date is not None:
        window = timedelta(days=PODCAST_EPISODE_MATCH_WINDOW_DAYS)
        candidates = [
            c for c in candidates
            if c["pub_date"] is not None and abs(c["pub_date"] - release_date) <= window
        ]
    if len(candidates) == 1:
        return candidates[0]["id"]
    if len(candidates) > 1:
        warnings.append(f"ambiguous title match ({len(candidates)} candidates): {title!r}")
    elif key in by_title:
        warnings.append(f"title matched but pub_date outside window: {title!r}")
    else:
        warnings.append(f"no podcast_episodes match: {title!r}")
    return None


# ── Normalisers (vendor shape unverified — every field via _pick) ──────────────

def _episode_metrics(episode: dict) -> dict[str, Any]:
    """Flatten the plausible metric spellings from an episode entry. The collector
    puts analytics under `metrics`; tolerate them at the top level too."""
    metrics = episode.get("metrics") if isinstance(episode.get("metrics"), dict) else {}
    merged = {**episode, **metrics}
    return {
        "plays": _pick(merged, "plays", "playCount", "totalPlays", "allTimePlays"),
        "streams": _pick(merged, "streams", "streamCount", "totalStreams"),
        "starts": _pick(merged, "starts", "startCount", "totalStarts", "impressions"),
        "listeners": _pick(merged, "listeners", "uniqueListeners", "listenerCount", "totalListeners"),
        "saves": _pick(merged, "saves", "saveCount"),
        "follows_gained": _pick(merged, "follows", "followsGained", "newFollowers"),
        "completion_pct": _pick(merged, "completion_pct", "completionRate", "medianCompletion", "percentListened"),
        "avg_listen_sec": _pick(merged, "avg_listen_sec", "averageListenSeconds", "avgListenTime", "medianListenSeconds"),
    }


def _retention_curve(episode: dict) -> Any:
    """The listen-through curve, verbatim — whatever shape Spotify serves is what
    the Admin gets; re-shaping an unverified structure only adds a place to be wrong."""
    retention = episode.get("retention")
    if retention is None:
        return None
    if isinstance(retention, dict):
        return _pick(retention, "samples", "counts", "curve", "percentiles", "data") or retention
    return retention


def _vendor_raw(blob: Any) -> Any:
    """The raw vendor payload, size-capped. Oversize → dropped with a warning (the
    parsed fields still land)."""
    if blob is None:
        return None
    try:
        if len(json.dumps(blob, default=str)) > _VENDOR_RAW_MAX_BYTES:
            return {"_dropped": f"vendor_raw over {_VENDOR_RAW_MAX_BYTES} bytes"}
    except (TypeError, ValueError):
        return None
    return blob


def _episode_row(
    episode: dict,
    by_title: dict[str, list[dict[str, Any]]],
    warnings: list[str],
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """(content_stats row, podcast_episodes update) for one collected episode.
    Either half can be None; a collector-side failure marker skips the episode."""
    if episode.get("error"):
        warnings.append(
            f"collector failed on {episode.get('episode_id') or episode.get('title') or '?'}: "
            f"{str(episode['error'])[:200]}"
        )
        return None, None
    episode_id = _pick(episode, "episode_id", "episodeId", "id")
    if not episode_id:
        warnings.append(f"episode with no id skipped: {episode.get('title')!r}")
        return None, None
    episode_id = str(episode_id)

    title = _pick(episode, "title", "name")
    release_date = _parse_date(_pick(episode, "release_date", "releaseDate", "publishOn", "published_at"))
    metrics = _episode_metrics(episode)
    matched_id = _match_episode(by_title, title, release_date, warnings)

    plays = _nz(metrics["plays"])
    listeners = _nz(metrics["listeners"])
    completion_pct = _to_float(metrics["completion_pct"])
    avg_listen_sec = _to_float(metrics["avg_listen_sec"])

    platform_specific = {
        k: v
        for k, v in {
            "starts": _nz(metrics["starts"]),
            "streams": _nz(metrics["streams"]),
            "saves": _to_int(metrics["saves"]),
            "follows_gained": _to_int(metrics["follows_gained"]),
            "completion_pct": completion_pct,
            "avg_listen_sec": avg_listen_sec,
            "retention_curve": _retention_curve(episode),
            "vendor_raw": _vendor_raw(episode.get("raw")),
        }.items()
        if v is not None
    }

    duration_sec = _to_int(_pick(episode, "duration_sec", "durationSeconds"))
    if duration_sec is None:
        duration_ms = _to_int(_pick(episode, "duration_ms", "durationMs", "duration"))
        duration_sec = round(duration_ms / 1000) if duration_ms else None

    stats_row = {
        "platform": _PLATFORM,
        "post_id": episode_id,
        "post_url": _pick(episode, "url", "episodeUrl", "shareUrl")
        or f"https://open.spotify.com/episode/{episode_id}",
        "posted_at": release_date.isoformat() if release_date else None,
        "views": plays,
        "reach": listeners,
        "duration_sec": duration_sec,
        "caption": _pick(episode, "description", "summary") or title,
        "platform_specific": platform_specific or None,
        "podcast_episode_id": matched_id,
    }

    episode_update = None
    if matched_id:
        # Overwrite policy is deliberate (see module doc); 0/None is never written.
        fields = {
            "plays_spotify": plays,
            "spotify_avg_listen_minutes": round(avg_listen_sec / 60, 2) if avg_listen_sec else None,
            "spotify_completion_pct": completion_pct if completion_pct else None,
        }
        fields = {k: v for k, v in fields.items() if v is not None}
        # Demographics land in the spotify_-prefixed columns (044). The unprefixed ones
        # are ALSO written for now: the Admin app still reads those, and this repo cannot
        # see that repo to know when it has switched over. Drop this second write — and
        # the old columns — only once Admin reads spotify_*.
        # Demographics are LAST-30-DAYS (the only window Spotify serves per episode), so
        # they are NOT interchangeable with the hand-entered lifetime values already in
        # this table. The collector only sends them for episodes under 90 days old, where
        # 30 days is most of the episode's listening; anything older is skipped there.
        # Everything below the extension's gate is therefore a recent episode, and the
        # write is a fill, not a correction.
        demographics = _demographic_columns(episode, warnings)
        fields.update(demographics)
        for column, value in demographics.items():
            legacy = column[len("spotify_"):]
            if legacy.startswith("geo_plays_") or legacy.startswith("age_") or legacy.startswith("gender_"):
                fields[legacy] = value
        if fields:
            episode_update = {"id": matched_id, **fields}
    return stats_row, episode_update



# ── Per-episode demographics -> podcast_episodes.spotify_* (migration 044) ────

_AGE_COLUMNS = {
    "age_18_22": "spotify_age_18_22_pct",
    "age_23_27": "spotify_age_23_27_pct",
    "age_28_34": "spotify_age_28_34_pct",
    "age_35_44": "spotify_age_35_44_pct",
    "age_45_59": "spotify_age_45_59_pct",
    "age_60_plus": "spotify_age_60plus_pct",
    "60plus": "spotify_age_60plus_pct",
    "18-22": "spotify_age_18_22_pct",
    "23-27": "spotify_age_23_27_pct",
    "28-34": "spotify_age_28_34_pct",
    "35-44": "spotify_age_35_44_pct",
    "45-59": "spotify_age_45_59_pct",
    "60+": "spotify_age_60plus_pct",
}
# Spotify sends MALE / FEMALE / NON_BINARY / NOT_SPECIFIED. Aliases are listed because
# an unmapped bucket is DROPPED and the rest renormalise — which produces a plausible
# wrong number rather than a visible failure: missing NOT_SPECIFIED once turned a true
# 90.1% female into 94.0%. _pct_columns now warns on any bucket it cannot place.
_GENDER_COLUMNS = {
    "female": "spotify_gender_female_pct",
    "male": "spotify_gender_male_pct",
    "non_binary": "spotify_gender_non_binary_pct",
    "nonbinary": "spotify_gender_non_binary_pct",
    "not_specified": "spotify_gender_not_specified_pct",
    "notspecified": "spotify_gender_not_specified_pct",
    "unknown": "spotify_gender_not_specified_pct",
    "unspecified": "spotify_gender_not_specified_pct",
}
_GEO_COLUMNS = {"NZ": "spotify_geo_plays_nz", "AU": "spotify_geo_plays_au",
                "GB": "spotify_geo_plays_gb", "US": "spotify_geo_plays_us"}


def _pct_columns(rows: Any, mapping: dict[str, str], key: str,
                 warnings: list[str] | None = None) -> dict[str, float]:
    """Counts -> percentages over the MAPPED buckets only.

    Spotify returns eight age brackets; podcast_episodes has six columns — no home for
    '0-17' or 'unknown'. The existing 378 hand-entered rows sum to exactly 100.0 across
    the six, so the established convention is to renormalise over what fits rather than
    leave the row summing to 99.75. Unmapped buckets are dropped, not folded into a
    neighbour: putting 0-17 into 18-22 would invent listeners in a bracket.
    """
    if not isinstance(rows, list):
        return {}
    totals: dict[str, float] = {}
    unmapped: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        bucket = str(row.get(key) or "").strip().lower()
        count = _to_float(row.get("count"))
        column = mapping.get(bucket) or mapping.get(bucket.replace(" ", ""))
        if column and count is not None:
            totals[column] = totals.get(column, 0.0) + count
        elif count:
            # Never drop a populated bucket in silence: the renormalisation below would
            # quietly redistribute its share across the others.
            unmapped.append(f"{key}={bucket!r} ({count:g})")
    if unmapped and warnings is not None:
        warnings.append(f"unmapped demographic buckets dropped: {', '.join(unmapped)}")
    base = sum(totals.values())
    if base <= 0:
        return {}
    return {col: round(val * 100 / base, 2) for col, val in totals.items()}


def _geo_columns(rows: Any) -> dict[str, int]:
    """ISO-2 play counts -> the five geo columns; everything unlisted becomes ROW."""
    if not isinstance(rows, list):
        return {}
    out: dict[str, int] = {}
    row_total = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        code = str(row.get("country") or "").upper()
        count = _to_int(row.get("count"))
        if count is None:
            continue
        column = _GEO_COLUMNS.get(code)
        if column:
            out[column] = out.get(column, 0) + count
        else:
            row_total += count
    if not out and not row_total:
        return {}
    out["spotify_geo_plays_row"] = row_total
    return out


def _demographic_columns(episode: dict, warnings: list[str]) -> dict[str, Any]:
    """The spotify_* demographic columns for one episode, or {} when unavailable."""
    demo = episode.get("demographics")
    if not isinstance(demo, dict):
        return {}
    fields: dict[str, Any] = {}
    fields.update(_pct_columns(demo.get("gender"), _GENDER_COLUMNS, "gender", warnings))
    fields.update(_pct_columns(demo.get("age"), _AGE_COLUMNS, "age", warnings))
    fields.update(_geo_columns(demo.get("geo")))
    return fields

# ── Show-level writers ─────────────────────────────────────────────────────────

def _demographic_entries(block: Any) -> list[tuple[str, float]]:
    """[(bucket, value)] from either vendor shape: {bucket: value} or
    [{label/name/bucket/age/gender/country, value/count/percentage}]."""
    entries: list[tuple[str, float]] = []
    if isinstance(block, dict):
        for bucket, value in block.items():
            parsed = _to_float(value)
            if bucket and parsed is not None:
                entries.append((str(bucket), parsed))
    elif isinstance(block, list):
        for item in block:
            if not isinstance(item, dict):
                continue
            bucket = _pick(item, "bucket", "label", "name", "age", "gender", "country", "countryCode")
            parsed = _to_float(_pick(item, "value", "count", "percentage", "percent", "plays"))
            if bucket and parsed is not None:
                entries.append((str(bucket), parsed))
    return entries


def _normalise_bucket(dimension: str, bucket: str) -> str:
    if dimension == "gender":
        # Match the system-wide male/female/unknown convention — "F" beside
        # "female" would split one audience across two buckets permanently.
        return _GENDER_MAP.get(bucket.strip().lower(), bucket.strip().lower())
    if dimension == "country":
        # Existing rows are ISO-2 (AU/GB/NZ/US); uppercase a 2-letter code and
        # pass anything longer through verbatim rather than guessing a mapping.
        code = bucket.strip()
        return code.upper() if len(code) == 2 else code
    return bucket.strip()


def _write_show_demographics(
    show: dict, social_account_id: str, warnings: list[str]
) -> int:
    demographics = show.get("demographics")
    if not isinstance(demographics, dict):
        return 0
    value_type = demographics.get("value_type")
    if value_type not in ("count", "percent"):
        # Deliberate refusal, not an oversight: whether Spotify serves counts or
        # percentages is a Step-0 discovery fact, and writing one as the other
        # corrupts the table. The collector declares it once verified.
        warnings.append(
            "demographics skipped: payload does not declare value_type "
            "('count' or 'percent') — set it in spotify-collector.js once the "
            "live response shape is confirmed"
        )
        return 0
    snapshot_date = date.today().isoformat()
    rows = []
    for dimension in ("gender", "age", "country"):
        for bucket, value in _demographic_entries(demographics.get(dimension)):
            rows.append({
                "social_account_id": social_account_id,
                "platform": _PLATFORM,
                "dimension": dimension,
                "bucket": _normalise_bucket(dimension, bucket),
                "value": value,
                "value_type": value_type,
                "snapshot_date": snapshot_date,
            })
    return upsert_audience_demographics(rows)


def _write_followers(show: dict, account: dict, warnings: list[str]) -> bool:
    followers = _nz(_pick(show, "followers", "followerCount", "totalFollowers"))
    if followers is None:
        return False
    try:
        written = upsert_follower_snapshot(account["id"], _PLATFORM, followers)
        update_social_account_follower_count(account["id"], followers)
        return written
    except Exception as exc:
        warnings.append(f"follower snapshot failed: {str(exc)[:200]}")
        return False


def _update_podcast_episodes(updates: list[dict[str, Any]], warnings: list[str]) -> int:
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


# ── Public entry ───────────────────────────────────────────────────────────────

def import_podcast_payload(payload: dict) -> dict:
    """
    Land one /podcast/import batch. Returns the summary the popup renders:
    {status, episodes_written, episodes_matched, demographics_rows,
     follower_written, warnings}. Never raises — a total failure returns
    status='error' and logs the source_runs row, per-episode failures warn and
    continue (server upserts are idempotent, so the extension retries freely).
    """
    warnings: list[str] = []
    batch = payload.get("batch") or {}
    batch_note = (
        f"batch {batch.get('index')}/{batch.get('total')} ({batch.get('mode')})"
        if batch.get("total") else None
    )
    try:
        episodes = payload.get("episodes") or []
        show = payload.get("show") or {}

        by_title = _fetch_admin_episodes() if episodes else {}
        stats_rows: list[dict[str, Any]] = []
        episode_updates: list[dict[str, Any]] = []
        for episode in episodes:
            try:
                stats_row, episode_update = _episode_row(episode, by_title, warnings)
            except Exception as exc:
                warnings.append(
                    f"episode normalise failed ({episode.get('episode_id') or '?'}): {str(exc)[:200]}"
                )
                continue
            if stats_row:
                stats_rows.append(stats_row)
            if episode_update:
                episode_updates.append(episode_update)

        episodes_written = upsert_self_content_stats(stats_rows)
        episodes_matched = _update_podcast_episodes(episode_updates, warnings)

        demographics_rows = 0
        follower_written = False
        if show:
            account = get_social_account_by_platform(_PLATFORM)
            if account is None:
                warnings.append("no spotify social_accounts row — show-level data skipped")
            else:
                demographics_rows = _write_show_demographics(show, account["id"], warnings)
                follower_written = _write_followers(show, account, warnings)

        for warning in warnings:
            logger.warning("Podcast import: %s", warning)
        logger.info(
            "Podcast import%s: %d episodes written, %d matched, %d demographics rows, followers=%s",
            f" ({batch_note})" if batch_note else "",
            episodes_written, episodes_matched, demographics_rows, follower_written,
        )
        message = "; ".join(filter(None, [batch_note, f"{len(warnings)} warnings" if warnings else None]))
        log_source_run(_RUN_NAME, _RUN_CATEGORY, "ok", episodes_written, message or None)
        return {
            "status": "ok",
            "episodes_written": episodes_written,
            "episodes_matched": episodes_matched,
            "demographics_rows": demographics_rows,
            "follower_written": follower_written,
            "warnings": warnings,
        }
    except Exception as exc:
        logger.error("Podcast import failed: %s", exc, exc_info=True)
        log_source_run(
            _RUN_NAME, _RUN_CATEGORY, "error", 0,
            "; ".join(filter(None, [batch_note, str(exc)[:400]])),
        )
        return {
            "status": "error",
            "episodes_written": 0,
            "episodes_matched": 0,
            "demographics_rows": 0,
            "follower_written": False,
            "warnings": warnings + [str(exc)[:400]],
        }
