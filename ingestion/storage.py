"""
Supabase storage layer. Upserts articles using guid as the conflict key
so re-runs never create duplicates.
"""

import logging
from typing import Any

from supabase import create_client, Client

from config import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY

logger = logging.getLogger(__name__)

TABLE = "news_articles"

_client: Client | None = None


def get_client() -> Client:
    global _client
    if _client is None:
        _client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
    return _client


def upsert_articles(articles: list[dict[str, Any]]) -> int:
    """
    Insert new articles; skip any whose guid already exists.
    Returns number of rows actually inserted.
    """
    if not articles:
        return 0

    client = get_client()

    # Supabase upsert with on_conflict=guid ignores duplicates
    response = (
        client.table(TABLE)
        .upsert(articles, on_conflict="guid", ignore_duplicates=True)
        .execute()
    )

    inserted = len(response.data) if response.data else 0
    logger.info("Upserted %d new articles into %s", inserted, TABLE)
    return inserted


VALID_STATUSES = {"new", "included", "excluded", "accepted", "briefed", "published"}


def get_sources(source_type: str | None = None, enabled_only: bool = True) -> list[dict]:
    """Return configured sources from the DB. Filters by type and enabled flag."""
    client = get_client()
    query = client.table("sources").select("id, name, url, category, source_type, enabled")
    if enabled_only:
        query = query.eq("enabled", True)
    if source_type:
        query = query.eq("source_type", source_type)
    response = query.order("name").execute()
    return response.data or []


def get_social_sources() -> list[dict]:
    """
    Return enabled Instagram/TikTok sources for the scan stage.
    Includes `handle` (the scrape target — username without @); for social
    sources `url` is the display profile URL, not the scrape target.
    """
    client = get_client()
    response = (
        client.table("sources")
        .select("id, name, handle, url, category, source_type, enabled")
        .eq("enabled", True)
        .in_("source_type", ["instagram", "tiktok"])
        .order("name")
        .execute()
    )
    return response.data or []


def log_source_run(
    source_name: str,
    source_category: str,
    status: str,
    article_count: int = 0,
    error_message: str | None = None,
) -> None:
    """Record one source fetch attempt. status must be 'ok' or 'error'."""
    from datetime import date
    client = get_client()
    try:
        client.table("source_runs").insert({
            "run_date": date.today().isoformat(),
            "source_name": source_name,
            "source_category": source_category,
            "status": status,
            "article_count": article_count,
            "error_message": error_message,
        }).execute()
    except Exception as exc:
        logger.warning("Could not log source run (table may not exist): %s", exc)


def get_pipeline_settings() -> dict:
    """
    Fetch the single pipeline_settings row from Supabase.
    Returns a dict with tov_doc, audience_doc, similarity_threshold,
    score_threshold, and max_articles_per_source.
    Falls back to hardcoded defaults if the row is missing.
    """
    client = get_client()
    response = (
        client.table("pipeline_settings")
        .select("tov_doc, audience_doc, similarity_threshold, score_threshold, max_articles_per_source, custom_cluster_prompt, daily_brief_prompt, available_tags, available_geo_tags, research_score_threshold")
        .eq("id", 1)
        .single()
        .execute()
    )
    if response.data:
        return response.data
    # Defaults if table hasn't been migrated yet
    return {
        "tov_doc": "",
        "audience_doc": "",
        "similarity_threshold": 0.65,
        "score_threshold": 0.4,
        "max_articles_per_source": 50,
    }


def set_article_status(guid: str, status: str, reason: str | None = None) -> None:
    """Transition a single article to a new status."""
    if status not in VALID_STATUSES:
        raise ValueError(f"Unknown status '{status}'. Must be one of {VALID_STATUSES}")
    client = get_client()
    payload: dict = {"status": status}
    if reason is not None:
        payload["status_reason"] = reason
    client.table(TABLE).update(payload).eq("guid", guid).execute()


def get_existing_guids(guids: list[str]) -> set[str]:
    """Return subset of guids that already exist in the table."""
    if not guids:
        return set()
    client = get_client()
    response = (
        client.table(TABLE)
        .select("guid")
        .in_("guid", guids)
        .execute()
    )
    return {row["guid"] for row in (response.data or [])}
