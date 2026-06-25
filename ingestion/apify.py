"""
Shared Apify plumbing used by both the news social fetch (ingestion/social.py)
and the competitor run (ingestion/competitors.py).

`run_actor` runs an Apify actor synchronously and returns its dataset items;
`parse_ts` normalises a post timestamp (unix or ISO) to an ISO-8601 string.
"""

from datetime import datetime, timezone

import httpx

from config import APIFY_TOKEN

APIFY_BASE = "https://api.apify.com/v2/actors"
# Apify sync runs cap at 300s.
APIFY_TIMEOUT = 300


def run_actor(actor_id: str, run_input: dict) -> list[dict]:
    """
    Run an Apify actor synchronously and return its dataset items.
    Raises on any non-2xx response so the caller can log an error source_run.
    """
    url = f"{APIFY_BASE}/{actor_id}/run-sync-get-dataset-items"
    resp = httpx.post(
        url,
        params={"token": APIFY_TOKEN},
        json=run_input,
        timeout=APIFY_TIMEOUT,
    )
    resp.raise_for_status()
    data = resp.json()
    return data if isinstance(data, list) else []


def parse_ts(value) -> str | None:
    """Normalise a post timestamp to an ISO-8601 string. Accepts unix or ISO."""
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc).isoformat()
    return str(value)
