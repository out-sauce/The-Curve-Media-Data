"""
Daily Brief — synthesises scored story clusters into a single
flowing HTML summary for the day, saved to content_calendar_items.

Only includes clusters with relevance_score >= score_threshold (default 0.5).

Mirrors the generateDailyDigest action in the admin Next.js app.
"""

import logging
from datetime import date, timedelta
from typing import Any

import anthropic

from config import ANTHROPIC_API_KEY
from ingestion.storage import get_client, get_pipeline_settings

logger = logging.getLogger(__name__)

CLUSTERS_TABLE = "story_clusters"
CALENDAR_TABLE = "content_calendar_items"
MODEL = "claude-sonnet-4-6"

HTML_FORMAT_INSTRUCTION = (
    "\nFormat your response as HTML. Use <p> for paragraphs, <h2> and <h3> for headings, "
    "<ul><li> for bullet points, <ol><li> for numbered lists, <strong> for bold, and <em> for italic. "
    "Do not use markdown. Do not include <html>, <head>, or <body> tags — return the content fragment only."
)


def _fetch_scored_clusters(run_date: str, score_threshold: float) -> list[dict[str, Any]]:
    client = get_client()
    resp = (
        client.table(CLUSTERS_TABLE)
        .select("name, description, score_reason")
        .eq("date", run_date)
        .eq("cluster_status", "scored")
        .gte("relevance_score", score_threshold)
        .execute()
    )
    return resp.data or []


def _upsert_calendar_item(run_date: str, summary: str) -> None:
    supabase = get_client()
    existing = (
        supabase.table(CALENDAR_TABLE)
        .select("id")
        .eq("publish_date", run_date)
        .eq("title", "Daily Briefing")
        .maybe_single()
        .execute()
    )
    if existing.data:
        supabase.table(CALENDAR_TABLE).update({"notes": summary}).eq("id", existing.data["id"]).execute()
    else:
        supabase.table(CALENDAR_TABLE).insert({
            "publish_date":  run_date,
            "content_type":  "daily_briefing",
            "title":         "Daily Briefing",
            "status":        "draft",
            "notes":         summary,
        }).execute()


DEFAULT_SCORE_THRESHOLD = 0.5


def run_daily_brief(run_date: str | None = None) -> None:
    target_date = run_date or (date.today() - timedelta(days=1)).isoformat()
    logger.info("Daily brief started for %s", target_date)

    settings = get_pipeline_settings()
    daily_brief_prompt = (settings.get("daily_brief_prompt") or "").strip()
    score_threshold = float(settings.get("score_threshold") or DEFAULT_SCORE_THRESHOLD)
    system_prompt = daily_brief_prompt + HTML_FORMAT_INSTRUCTION

    clusters = _fetch_scored_clusters(target_date, score_threshold)
    if not clusters:
        logger.info("Daily brief: no scored clusters above %.1f for %s", score_threshold, target_date)
        return

    stories_block = "\n\n---\n\n".join(
        f"{c.get('name') or 'Untitled'}\n{c.get('description') or c.get('score_reason') or ''}"
        for c in clusters
    )

    logger.info("Generating daily brief from %d stories", len(clusters))
    ai = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    msg = ai.messages.create(
        model=MODEL,
        max_tokens=1024,
        system=system_prompt,
        messages=[{"role": "user", "content": stories_block}],
    )
    summary = msg.content[0].text.strip()

    _upsert_calendar_item(target_date, summary)
    logger.info("Daily brief complete (%d chars)", len(summary))
