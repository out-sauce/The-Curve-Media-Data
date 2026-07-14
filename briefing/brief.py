"""
Stage 5 — Brief Generation.

Generates an editorial brief for each researched story cluster that actually
produced a deep summary. One Claude call per cluster, using CurveTOV.md as the
system prompt. Clusters research marked 'researched' but for which no article
got a deep summary (all scrapes failed/paywalled, or Claude failed) are skipped.

Input:
  - Anchor article (title + summary) as the lead source
  - All other articles in the cluster as supporting context

Output:
  - name + brief written to story_clusters
  - briefed_at timestamp set
  - cluster_status is left at 'researched' (advancing the cluster is a
    downstream / manual decision)
"""

import logging
from datetime import datetime, timezone
from typing import Any

import anthropic

from config import ANTHROPIC_API_KEY
from ingestion.storage import get_client, get_pipeline_settings, TABLE

logger = logging.getLogger(__name__)

CLUSTERS_TABLE = "story_clusters"
BRIEFING_MODEL = "claude-sonnet-4-6"


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _fetch_briefable_clusters(run_date: str) -> list[dict[str, Any]]:
    client = get_client()
    response = (
        client.table(CLUSTERS_TABLE)
        .select("id, cluster_id, brief")
        .eq("cluster_status", "researched")
        .eq("date", run_date)
        .execute()
    )
    return response.data or []


def _fetch_cluster_articles(cluster_id: str) -> list[dict[str, Any]]:
    client = get_client()
    response = (
        client.table(TABLE)
        .select("id, guid, title, summary, source_name, deep_summary")
        .eq("cluster_id", cluster_id)
        .execute()
    )
    return response.data or []


# ---------------------------------------------------------------------------
# Brief generation
# ---------------------------------------------------------------------------

def _build_prompt(articles: list[dict[str, Any]], brief_instructions: str = "") -> str:
    lines = [
        "Generate a short editorial name and brief for the following story.",
        "",
        "Return JSON only, with exactly two fields:",
        '  "name": the editorial title',
        '  "brief": the editorial brief',
        "",
    ]

    if brief_instructions:
        lines.append(brief_instructions)
        lines.append("")

    lines.append("ARTICLES:")
    for article in articles:
        source = article.get("source_name", "Unknown")
        title = article.get("title", "")
        summary = article.get("summary", "")
        lines.append(f"- [{source}] {title}: {summary}")

    return "\n".join(lines)


def _generate_brief(articles: list[dict[str, Any]], tov_doc: str, brief_instructions: str = "") -> tuple[str, str] | None:
    """
    Call Claude to generate a name and brief.
    Returns (name, brief) or None on failure.
    """
    import json

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    prompt = _build_prompt(articles, brief_instructions)

    try:
        message = client.messages.create(
            model=BRIEFING_MODEL,
            max_tokens=600,
            system=tov_doc,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = message.content[0].text.strip()
        raw = raw.removeprefix("```json").removeprefix("```").removesuffix("```").strip()
        data = json.loads(raw)
        name = data.get("name", "").strip()
        brief = data.get("brief", "").strip()
        if not name or not brief:
            raise ValueError("Missing name or brief in response")
        return name, brief
    except Exception as exc:
        logger.warning("Brief generation failed: %s", exc)
        return None


def rebrief_cluster(cluster_id: str) -> bool:
    """
    Regenerate the name + brief for a single cluster, overwriting any existing brief.
    This is the explicit "redo" used after on-demand research (the Admin story-level
    Research button, or a late-arriving extension import) — unlike run_briefing it
    ignores date/status and does not skip clusters that already have a brief. Still
    requires at least one article with a deep summary. Returns True when a brief was
    written. Never raises.
    """
    try:
        settings = get_pipeline_settings()
        tov_doc = settings.get("tov_doc", "")
        brief_instructions = settings.get("brief_instructions", "")

        articles = _fetch_cluster_articles(cluster_id)
        if not any((a.get("deep_summary") or "").strip() for a in articles):
            logger.info("Re-brief: cluster %s has no deep summaries — skipping", cluster_id)
            return False

        for article in articles:
            article["summary"] = article.get("deep_summary") or article["summary"]

        result = _generate_brief(articles, tov_doc, brief_instructions)
        if result is None:
            return False

        name, brief = result
        get_client().table(CLUSTERS_TABLE).update({
            "name": name,
            "brief": brief,
            "briefed_at": datetime.now(timezone.utc).isoformat(),
        }).eq("cluster_id", cluster_id).execute()
        logger.info("Re-brief: cluster %s brief regenerated (%d chars)", cluster_id, len(brief))
        return True
    except Exception as exc:
        logger.warning("Re-brief: cluster %s failed: %s", cluster_id, exc)
        return False


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_briefing(run_date: str | None = None) -> None:
    """
    Stage 5 brief generation. Run after research.

    For each researched cluster that has at least one article with a deep
    summary:
      1. Skip clusters that already have a brief (idempotent re-runs)
      2. Fetch all cluster articles
      3. Skip the cluster if no article has a non-empty deep_summary
      4. Generate brief via Claude (tov_doc as system prompt)
      5. Write name + brief + briefed_at to story_clusters (status unchanged)
    """
    from datetime import date, timedelta
    target_date = run_date or (date.today() - timedelta(days=1)).isoformat()
    logger.info("Brief generation started for %s", target_date)

    _settings = get_pipeline_settings()
    _CURVE_TOV = _settings.get("tov_doc", "")
    _BRIEF_INSTRUCTIONS = _settings.get("brief_instructions", "")

    clusters = _fetch_briefable_clusters(target_date)
    if not clusters:
        logger.info("Briefing: no researched clusters to process")
        return

    logger.info("Generating briefs for %d clusters", len(clusters))

    supabase = get_client()
    briefed = 0
    failed = 0
    skipped = 0

    for cluster in clusters:
        cluster_id = cluster["cluster_id"]

        # Status stays 'researched' after briefing, so a re-run would otherwise
        # re-brief clusters that already have one. Skip those.
        if (cluster.get("brief") or "").strip():
            skipped += 1
            continue

        articles = _fetch_cluster_articles(cluster_id)
        if not articles:
            logger.warning("Cluster %s has no articles — skipping", cluster_id)
            continue

        # Only brief stories the research stage actually summarised: require at
        # least one article with a non-empty deep_summary. Clusters marked
        # 'researched' but with no deep summary (all scrapes failed/paywalled,
        # or Claude failed) are left untouched for a later re-run.
        if not any((a.get("deep_summary") or "").strip() for a in articles):
            skipped += 1
            logger.info("Cluster %s has no deep summary — skipping brief", cluster_id)
            continue

        # Prefer deep_summary (from research stage) over RSS summary where available
        for article in articles:
            article["summary"] = article.get("deep_summary") or article["summary"]

        result = _generate_brief(articles, _CURVE_TOV, _BRIEF_INSTRUCTIONS)

        if result is None:
            failed += 1
            continue

        name, brief = result
        # Attach the brief but leave cluster_status at 'researched' — advancing
        # a cluster past research (e.g. to ready_for_content) is a downstream /
        # manual decision, not something this stage does.
        try:
            supabase.table(CLUSTERS_TABLE).update({
                "name": name,
                "brief": brief,
                "briefed_at": datetime.now(timezone.utc).isoformat(),
            }).eq("cluster_id", cluster_id).execute()
        except Exception as exc:
            failed += 1
            logger.warning("Cluster %s — brief write failed: %s", cluster_id, exc)
            continue

        briefed += 1
        logger.debug("Cluster %s — brief generated (%d chars)", cluster_id, len(brief))

    logger.info(
        "Briefing complete — %d briefed, %d skipped (no deep summary), %d failed",
        briefed, skipped, failed,
    )
