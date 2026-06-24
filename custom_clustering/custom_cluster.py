"""
Custom Clustering — Stage 6.

Runs after brief generation. Sends today's briefed auto-clusters to Claude
and asks it to group related stories into roundup meta-clusters.

Each custom cluster gets its own brief written using CurveTOV (same format as
auto cluster briefs), then scored on that brief. Clusters below the score
threshold are rejected.
"""

import json
import logging
import uuid
from datetime import date, datetime, timezone
from typing import Any

import anthropic

from config import ANTHROPIC_API_KEY
from ingestion.storage import get_client, get_pipeline_settings

logger = logging.getLogger(__name__)

CLUSTERS_TABLE = "story_clusters"
ARTICLES_TABLE = "news_articles"
MODEL = "claude-sonnet-4-6"
SCORING_MODEL = "claude-sonnet-4-6"


# ── DB helpers ───────────────────────────────────────────────────────────────

def _fetch_rejected_single_clusters(run_date: str) -> list[dict[str, Any]]:
    """Return auto, single-article, rejected clusters for run_date."""
    client = get_client()
    response = (
        client.table(CLUSTERS_TABLE)
        .select("id, cluster_id, anchor_article_id, article_count, score_reason")
        .eq("date", run_date)
        .eq("cluster_type", "auto")
        .eq("cluster_status", "rejected")
        .eq("article_count", 1)
        .execute()
    )
    clusters = response.data or []
    if not clusters:
        return clusters

    # Enrich with anchor article title + summary
    article_ids = [c["anchor_article_id"] for c in clusters if c.get("anchor_article_id")]
    if article_ids:
        articles_resp = (
            get_client().table("news_articles")
            .select("id, title, summary")
            .in_("id", article_ids)
            .execute()
        )
        article_map = {a["id"]: a for a in (articles_resp.data or [])}
        for c in clusters:
            article = article_map.get(c.get("anchor_article_id"), {})
            c["anchor_title"] = article.get("title", "")
            c["anchor_summary"] = article.get("summary", "")

    return clusters


def _build_prompt(clusters: list[dict[str, Any]]) -> str:
    lines = []
    for c in clusters:
        lines.append(f"cluster_id: {c['cluster_id']}")
        lines.append(f"title: {c.get('anchor_title', '')}")
        lines.append(f"summary: {(c.get('anchor_summary') or '').strip()}")
        lines.append("")
    return "\n".join(lines)


def _fetch_articles_for_source_clusters(source_cluster_ids: list[str]) -> list[dict[str, Any]]:
    """Fetch all news articles belonging to the given auto-cluster IDs."""
    client = get_client()
    response = (
        client.table(ARTICLES_TABLE)
        .select("id, title, summary, source_name")
        .in_("cluster_id", source_cluster_ids)
        .execute()
    )
    return response.data or []


def _generate_roundup_brief(articles: list[dict[str, Any]], roundup_name: str, tov_doc: str) -> str | None:
    """
    Generate a brief for a custom roundup cluster using CurveTOV as the system prompt.
    Returns the brief text, or None on failure.
    """
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    article_lines = [
        f"- [{a.get('source_name', 'Unknown')}] {a.get('title', '')}: {(a.get('summary') or '').strip()}"
        for a in articles
    ]

    prompt = "\n".join([
        f'Write an editorial brief for the roundup "{roundup_name}".',
        "",
        "Return JSON only, with one field:",
        '  "brief": editorial brief following the Curve structure — headline fact, Curve angle, context, so what,'
        ' and watch this space (only if developing). 150–250 words, plain text, no markdown.',
        "",
        "STORIES IN THIS ROUNDUP:",
        *article_lines,
    ])

    try:
        message = client.messages.create(
            model=MODEL,
            max_tokens=600,
            system=tov_doc,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = message.content[0].text.strip()
        raw = raw.removeprefix("```json").removeprefix("```").removesuffix("```").strip()
        data = json.loads(raw)
        brief = data.get("brief", "").strip()
        if not brief:
            raise ValueError("Empty brief in response")
        return brief
    except Exception as exc:
        logger.warning("Roundup brief generation failed for '%s': %s", roundup_name, exc)
        return None


def _score_brief(brief: str, roundup_name: str, audience_doc: str) -> tuple[float, str]:
    """
    Score a custom cluster brief for relevance to the Curve audience.
    Returns (score, reason).
    """
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    prompt = (
        "Score this roundup brief for relevance to the Curve audience.\n"
        'Return JSON only: {"score": <float 0.0–1.0>, "reason": "<one sentence>"}.\n\n'
        f"Roundup: {roundup_name}\n\nBrief:\n{brief}"
    )
    try:
        message = client.messages.create(
            model=SCORING_MODEL,
            max_tokens=200,
            system=audience_doc,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = message.content[0].text.strip()
        raw = raw.removeprefix("```json").removeprefix("```").removesuffix("```").strip()
        data = json.loads(raw)
        score = min(max(float(data["score"]), 0.0), 1.0)
        reason = str(data["reason"])
        return score, reason
    except Exception as exc:
        logger.warning("Brief scoring failed for '%s': %s", roundup_name, exc)
        return 0.0, "Scoring failed"


# ── Claude ───────────────────────────────────────────────────────────────────

def _call_claude(clusters: list[dict[str, Any]], system_prompt: str) -> list[dict[str, Any]]:
    """
    Ask Claude to group clusters into roundups.
    Returns a list of {name, cluster_ids} dicts.
    """
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    user_content = _build_prompt(clusters)

    try:
        message = client.messages.create(
            model=MODEL,
            max_tokens=4096,
            system=system_prompt,
            messages=[{"role": "user", "content": user_content}],
        )
        raw = message.content[0].text.strip()
        logger.info("Claude raw response: %s", raw[:500])
        raw = raw.removeprefix("```json").removeprefix("```").removesuffix("```").strip()
        data = json.loads(raw)
        if not isinstance(data, list):
            raise ValueError(f"Expected JSON array, got {type(data)}")
        logger.info("Claude returned %d groupings", len(data))
        return data
    except Exception as exc:
        logger.warning("Custom clustering Claude call failed: %s", exc)
        return []


# ── Cluster creation ─────────────────────────────────────────────────────────

def _create_custom_clusters(
    groupings: list[dict[str, Any]],
    clusters_by_id: dict[str, dict[str, Any]],
    run_date: str,
    tov_doc: str,
    audience_doc: str,
    score_threshold: float,
) -> int:
    supabase = get_client()
    created = 0

    for group in groupings:
        name = group.get("name", "").strip()
        source_ids = group.get("cluster_ids", [])

        if not name or len(source_ids) < 2:
            logger.warning("Skipping malformed group: %s", group)
            continue

        # Validate all cluster_ids exist
        valid = [cid for cid in source_ids if cid in clusters_by_id]
        if len(valid) < 2:
            logger.warning("Group '%s' has fewer than 2 valid clusters — skipping", name)
            continue

        article_count = sum(
            clusters_by_id[cid].get("article_count", 1) for cid in valid
        )
        anchor_article_id = clusters_by_id[valid[0]].get("anchor_article_id")

        # Fetch articles from source clusters for brief generation
        articles = _fetch_articles_for_source_clusters(valid)
        if not articles:
            logger.warning("Group '%s' has no articles — skipping", name)
            continue

        # Generate brief using CurveTOV (same format as auto cluster briefs)
        brief = _generate_roundup_brief(articles, name, tov_doc)
        if brief is None:
            logger.warning("Brief generation failed for group '%s' — skipping", name)
            continue

        # Score the brief
        score, score_reason = _score_brief(brief, name, audience_doc)
        cluster_status = "briefed" if score >= score_threshold else "rejected"

        new_cluster_id = str(uuid.uuid4())

        supabase.table(CLUSTERS_TABLE).insert({
            "cluster_id":        new_cluster_id,
            "date":              run_date,
            "name":              name,
            "anchor_article_id": anchor_article_id,
            "article_count":     article_count,
            "cluster_status":    cluster_status,
            "cluster_type":      "custom",
            "brief":             brief,
            "briefed_at":        datetime.now(timezone.utc).isoformat() if cluster_status == "briefed" else None,
            "relevance_score":   score,
            "score_reason":      score_reason,
            "source_cluster_ids": valid,
        }).execute()

        # Re-link articles to the custom cluster and remove the absorbed auto-cluster rows
        supabase.table(ARTICLES_TABLE).update({"cluster_id": new_cluster_id}).in_("cluster_id", valid).execute()
        supabase.table(CLUSTERS_TABLE).delete().in_("cluster_id", valid).execute()

        created += 1
        logger.info(
            "Created custom cluster '%s' from %d auto clusters — score=%.2f status=%s",
            name, len(valid), score, cluster_status,
        )

    return created


# ── Debug ────────────────────────────────────────────────────────────────────

def _debug_cluster_statuses(run_date: str) -> None:
    client = get_client()
    resp = (
        client.table(CLUSTERS_TABLE)
        .select("cluster_id, cluster_type, cluster_status")
        .eq("date", run_date)
        .execute()
    )
    rows = resp.data or []
    logger.info("DEBUG %s: %d total clusters", run_date, len(rows))
    from collections import Counter
    counts = Counter(f"{r['cluster_type']}/{r['cluster_status']}" for r in rows)
    for k, v in counts.items():
        logger.info("  %s: %d", k, v)


# ── Entry point ──────────────────────────────────────────────────────────────

def run_custom_clustering(run_date: str | None = None) -> None:
    """
    Stage 6 — custom clustering. Run after brief generation.

    Fetches briefed auto-clusters for run_date (defaults to today), asks Claude
    to group related ones into roundup meta-clusters, and writes to story_clusters.
    """
    target_date = run_date or date.today().isoformat()
    logger.info("Custom clustering started for %s", target_date)

    settings = get_pipeline_settings()
    prompt = (settings.get("custom_cluster_prompt") or "").strip()
    if not prompt:
        logger.info("Custom clustering: no prompt configured — skipping")
        return

    tov_doc = settings.get("tov_doc") or ""
    audience_doc = settings.get("audience_doc") or ""
    score_threshold = float(settings.get("score_threshold") or 0.4)

    _debug_cluster_statuses(target_date)

    clusters = _fetch_rejected_single_clusters(target_date)
    if len(clusters) < 2:
        logger.info("Custom clustering: fewer than 2 rejected single clusters for %s — skipping", target_date)
        return

    logger.info("Custom clustering: sending %d clusters to Claude", len(clusters))

    groupings = _call_claude(clusters, prompt)
    if not groupings:
        logger.info("Custom clustering: no groupings returned")
        return

    clusters_by_id = {c["cluster_id"]: c for c in clusters}
    created = _create_custom_clusters(
        groupings, clusters_by_id, target_date,
        tov_doc=tov_doc,
        audience_doc=audience_doc,
        score_threshold=score_threshold,
    )

    logger.info("Custom clustering complete — %d custom clusters created", created)
