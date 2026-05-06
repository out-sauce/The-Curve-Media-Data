"""
Stage 4 — Scoring.

Scores all pending story clusters in a single Claude call.
All cluster summaries are sent together; Claude returns a JSON array
with a score and reason for each.

Transitions:
  story_clusters:  pending → scored  (all stories, regardless of score)
  news_articles:   unchanged (article status is managed independently)
"""

import json
import logging
from collections import defaultdict
from typing import Any

import anthropic

from config import ANTHROPIC_API_KEY
from ingestion.storage import get_client, get_pipeline_settings, TABLE

logger = logging.getLogger(__name__)

CLUSTERS_TABLE = "story_clusters"
SCORING_MODEL = "claude-haiku-4-5-20251001"


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _fetch_pending_clusters(run_date: str) -> list[dict[str, Any]]:
    client = get_client()
    response = (
        client.table(CLUSTERS_TABLE)
        .select("id, cluster_id")
        .eq("cluster_status", "pending")
        .eq("date", run_date)
        .execute()
    )
    return response.data or []


def _fetch_articles_for_clusters(cluster_ids: list[str]) -> dict[str, list[dict[str, Any]]]:
    """
    Fetch all articles for a set of cluster_ids in one DB query.
    Returns a dict of cluster_id -> list of articles.
    """
    client = get_client()
    grouped: dict[str, list] = defaultdict(list)

    for i in range(0, len(cluster_ids), 50):
        chunk = cluster_ids[i: i + 50]
        response = (
            client.table(TABLE)
            .select("cluster_id, title, summary, source_name")
            .in_("cluster_id", chunk)
            .execute()
        )
        for article in (response.data or []):
            grouped[article["cluster_id"]].append(article)

    return grouped


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

def _build_batch_prompt(clusters: list[dict[str, Any]], articles_by_cluster: dict[str, list]) -> str:
    """
    Build a single prompt covering all clusters, numbered 1..N.
    """
    parts = [
        "Score each of the following story clusters for relevance to the Curve audience.",
        "Return a JSON array with one object per cluster in the same order.",
        'Each object must have: "index" (int), "score" (float 0.0-1.0), "reason" (one sentence).',
        'Example: [{"index": 1, "score": 0.82, "reason": "..."}]',
        "Return only the JSON array. No preamble or markdown fences.",
        "",
    ]

    for i, cluster in enumerate(clusters, 1):
        cluster_id = cluster["cluster_id"]
        articles = articles_by_cluster.get(cluster_id, [])
        parts.append(f"--- Cluster {i} ---")
        for article in articles:
            source = article.get("source_name") or "Unknown"
            title = article.get("title") or ""
            summary = article.get("summary") or ""
            parts.append(f"[{source}] {title}: {summary}")
        parts.append("")

    return "\n".join(parts)


def _call_claude_batch(clusters: list[dict[str, Any]], articles_by_cluster: dict[str, list],
                       audience_doc: str) -> dict[str, tuple[float, str]]:
    """
    Score all clusters in one Claude call.
    Returns dict of cluster_id -> (score, reason).
    Falls back to score=0.0 for any cluster that can't be parsed.
    """
    prompt = _build_batch_prompt(clusters, articles_by_cluster)
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    try:
        message = client.messages.create(
            model=SCORING_MODEL,
            max_tokens=20000,
            system=audience_doc,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = message.content[0].text.strip()
        start = raw.find("[")
        if start != -1:
            raw = raw[start:].removesuffix("```").strip()
        data = json.loads(raw)

        if not isinstance(data, list):
            raise ValueError(f"Expected JSON array, got {type(data)}")

        results: dict[str, tuple[float, str]] = {}
        for item in data:
            idx = int(item["index"]) - 1  # convert to 0-based
            if 0 <= idx < len(clusters):
                cluster_id = clusters[idx]["cluster_id"]
                score = min(max(float(item["score"]), 0.0), 1.0)
                reason = str(item["reason"])
                results[cluster_id] = (score, reason)

        # Any cluster missing from Claude's response gets a fallback
        for cluster in clusters:
            if cluster["cluster_id"] not in results:
                logger.warning("Cluster %s missing from scoring response", cluster["cluster_id"])
                results[cluster["cluster_id"]] = (0.0, "Scoring failed — missing from response")

        return results

    except (json.JSONDecodeError, KeyError, ValueError, TypeError) as exc:
        logger.warning("Could not parse batch scoring response: %s", exc)
    except Exception as exc:
        logger.warning("Scoring API error: %s", exc)

    # Full fallback — mark everything as failed
    return {c["cluster_id"]: (0.0, "Scoring failed") for c in clusters}


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_scoring(run_date: str | None = None) -> None:
    """
    Stage 4 scoring. Run after clustering, before tagging.

    Sends all pending clusters for run_date to Claude in a single call.
    Claude returns a score and reason for each; results are written back to the DB.
    All stories transition to 'scored' regardless of their score.
    """
    from datetime import date, timedelta
    target_date = run_date or (date.today() - timedelta(days=1)).isoformat()
    logger.info("Scoring started for %s", target_date)

    pipeline_settings = get_pipeline_settings()
    audience_doc = pipeline_settings["audience_doc"]

    clusters = _fetch_pending_clusters(target_date)
    if not clusters:
        logger.info("Scoring: no pending clusters to process")
        return

    logger.info("Scoring %d clusters in a single Claude call", len(clusters))

    cluster_ids = [c["cluster_id"] for c in clusters]
    articles_by_cluster = _fetch_articles_for_clusters(cluster_ids)

    results = _call_claude_batch(clusters, articles_by_cluster, audience_doc)

    supabase = get_client()
    for cluster_id, (score, reason) in results.items():
        supabase.table(CLUSTERS_TABLE).update({
            "relevance_score": score,
            "score_reason":    reason,
            "cluster_status":  "scored",
        }).eq("cluster_id", cluster_id).execute()

    logger.info("Scoring complete -- %d clusters scored", len(results))
