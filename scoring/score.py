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
SCORING_MODEL = "claude-sonnet-4-6"

# Score clusters in bounded batches. A single Claude call over every cluster of the
# day overflowed max_tokens once the day grew past ~500 clusters (the JSON was
# truncated mid-string, json.loads failed, and *every* cluster fell back to 0.0 —
# see the 2026-07-22/23 daily run logs). Batching caps the response size so it
# can't truncate, and isolates a bad batch from the rest. Same fix as tagging.
SCORE_BATCH_SIZE = 100
SCORE_MAX_TOKENS = 20000

# Schema for structured outputs. Note: structured outputs don't support
# numerical bounds (minimum/maximum), so score is validated as a plain number
# and clamped to 0.0-1.0 client-side below.
SCORING_SCHEMA = {
    "type": "object",
    "properties": {
        "scores": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "index": {"type": "integer"},
                    "score": {"type": "number"},
                    "reason": {"type": "string"},
                },
                "required": ["index", "score", "reason"],
                "additionalProperties": False,
            },
        },
    },
    "required": ["scores"],
    "additionalProperties": False,
}


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
        'Return a JSON object {"scores": [...]} with one entry per cluster in the same order.',
        'Each entry must have: "index" (int), "score" (float 0.0-1.0), "reason" (one sentence).',
        'Example: {"scores": [{"index": 1, "score": 0.82, "reason": "..."}]}',
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
            max_tokens=SCORE_MAX_TOKENS,
            system=audience_doc,
            messages=[{"role": "user", "content": prompt}],
            # Structured outputs: the API constrains the response to this schema,
            # so the JSON is guaranteed parseable. Without it we were parsing
            # Claude's free text, which intermittently produced invalid JSON
            # (e.g. an unescaped quote in a "reason" string).
            output_config={
                "format": {
                    "type": "json_schema",
                    "schema": SCORING_SCHEMA,
                }
            },
        )
        if message.stop_reason == "refusal":
            raise ValueError("Scoring request was refused")
        if message.stop_reason == "max_tokens":
            # Truncated JSON is unparseable — fail the batch loudly instead of
            # trying to parse it.
            raise ValueError(
                f"Scoring response truncated at max_tokens={SCORE_MAX_TOKENS} "
                f"({len(clusters)} clusters in batch)"
            )

        # With output_config.format the first text block is valid JSON matching
        # SCORING_SCHEMA.
        raw = next(b.text for b in message.content if b.type == "text")
        data = json.loads(raw)["scores"]

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

    logger.info(
        "Scoring %d clusters in batches of %d", len(clusters), SCORE_BATCH_SIZE
    )

    cluster_ids = [c["cluster_id"] for c in clusters]
    articles_by_cluster = _fetch_articles_for_clusters(cluster_ids)

    # One Claude call per batch so the JSON response can't overflow max_tokens.
    # A batch that fails only falls back to 0.0 for its own clusters.
    results: dict[str, tuple[float, str]] = {}
    for i in range(0, len(clusters), SCORE_BATCH_SIZE):
        batch = clusters[i: i + SCORE_BATCH_SIZE]
        results.update(_call_claude_batch(batch, articles_by_cluster, audience_doc))

    supabase = get_client()
    for cluster_id, (score, reason) in results.items():
        supabase.table(CLUSTERS_TABLE).update({
            "relevance_score": score,
            "score_reason":    reason,
            "cluster_status":  "scored",
        }).eq("cluster_id", cluster_id).execute()

    logger.info("Scoring complete -- %d clusters scored", len(results))
