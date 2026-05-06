"""
Research stage — scrapes full article text and generates deep summaries.

Runs after tagging. Only processes articles in scored clusters that score
>= research_score_threshold (default 0.60). Transitions cluster status
to 'researched' once all articles are processed.

Results stored directly on news_articles rows:
  full_text, word_count, scrape_status, scraped_at
  deep_summary, key_facts, relevance_notes, summarised_at

Idempotent: articles with scrape_status IS NOT NULL are skipped on re-run
(except 'failed' — those are retried).
"""

import json
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any

import anthropic

from config import ANTHROPIC_API_KEY
from ingestion.storage import get_client, get_pipeline_settings, TABLE
from .scraper import scrape_article

logger = logging.getLogger(__name__)

CLUSTERS_TABLE = "story_clusters"
MODEL = "claude-sonnet-4-6"
DEFAULT_RESEARCH_THRESHOLD = 0.60


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _fetch_research_articles(run_date: str, score_threshold: float) -> tuple[list[dict[str, Any]], list[str]]:
    """Fetch articles in scored clusters at or above score_threshold. Returns (articles, cluster_ids)."""
    client = get_client()

    cluster_resp = (
        client.table(CLUSTERS_TABLE)
        .select("cluster_id")
        .eq("date", run_date)
        .eq("cluster_status", "scored")
        .gte("relevance_score", score_threshold)
        .execute()
    )
    cluster_ids = [r["cluster_id"] for r in (cluster_resp.data or [])]
    if not cluster_ids:
        return [], []

    articles = []
    for i in range(0, len(cluster_ids), 50):
        chunk = cluster_ids[i: i + 50]
        resp = (
            client.table(TABLE)
            .select("id, url, title, summary, source_id, scrape_status")
            .in_("cluster_id", chunk)
            .execute()
        )
        articles.extend(resp.data or [])
    return articles, cluster_ids


def _fetch_cookies_by_source(source_ids: list[int]) -> dict[int, str]:
    """Return {source_id: cookie_string} for sources with non-null cookies."""
    if not source_ids:
        return {}
    client = get_client()
    resp = (
        client.table("sources")
        .select("id, cookies")
        .in_("id", source_ids)
        .not_.is_("cookies", "null")
        .execute()
    )
    return {row["id"]: row["cookies"] for row in (resp.data or [])}


# ---------------------------------------------------------------------------
# Claude deep summary
# ---------------------------------------------------------------------------

def _call_claude(article: dict[str, Any], full_text: str, audience_doc: str) -> dict | None:
    prompt = "\n".join([
        f"Article title: {article.get('title', '')}",
        "",
        "Full article text:",
        full_text[:8000],
        "",
        "Return JSON only with exactly these three fields:",
        '  "deep_summary": 3-5 sentence editorial summary for a financially-aware general audience',
        '  "key_facts": JSON array of 3-5 bullet-point strings with the most important factual claims',
        '  "relevance_notes": one sentence explaining why this story matters to Curve readers',
        "No preamble. No markdown fences. Return valid JSON only.",
    ])

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    try:
        msg = client.messages.create(
            model=MODEL,
            max_tokens=800,
            system=audience_doc,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = msg.content[0].text.strip()
        raw = raw.removeprefix("```json").removeprefix("```").removesuffix("```").strip()
        data = json.loads(raw)
        return {
            "deep_summary":    (data.get("deep_summary") or "").strip(),
            "key_facts":       data.get("key_facts") or [],
            "relevance_notes": (data.get("relevance_notes") or "").strip(),
        }
    except Exception as exc:
        logger.warning("Claude research failed for article %s: %s", article.get("id"), exc)
        return None


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_research(run_date: str | None = None) -> None:
    target_date = run_date or (date.today() - timedelta(days=1)).isoformat()
    logger.info("Research started for %s", target_date)

    settings = get_pipeline_settings()
    audience_doc     = settings.get("audience_doc") or ""
    score_threshold  = float(settings.get("research_score_threshold") or DEFAULT_RESEARCH_THRESHOLD)

    articles, research_cluster_ids = _fetch_research_articles(target_date, score_threshold)
    if not articles:
        logger.info("Research: no articles in scored clusters for %s", target_date)
        return

    # Skip already-processed articles (except failed — retry those)
    articles = [a for a in articles if a.get("scrape_status") is None or a.get("scrape_status") == "failed"]
    if not articles:
        logger.info("Research: all articles already processed")
        return
    logger.info("Research: processing %d articles", len(articles))

    source_ids = list({a["source_id"] for a in articles if a.get("source_id")})
    cookies_by_source = _fetch_cookies_by_source(source_ids)

    supabase = get_client()
    scraped = paywalled = failed = summarised = 0

    for article in articles:
        article_id   = article["id"]
        url          = article["url"]
        cookie_str   = cookies_by_source.get(article.get("source_id"))
        now          = datetime.now(timezone.utc).isoformat()

        result = scrape_article(url, cookie_string=cookie_str)

        if result.status != "scraped":
            supabase.table(TABLE).update({
                "scrape_status": result.status,
                "scraped_at":    now,
            }).eq("id", article_id).execute()
            paywalled += result.status == "paywalled"
            failed    += result.status == "failed"
            logger.debug("Article %s: %s — %s", article_id, result.status, result.error)
            continue

        scraped += 1
        supabase.table(TABLE).update({
            "scrape_status": "scraped",
            "full_text":     result.full_text,
            "word_count":    result.word_count,
            "scraped_at":    now,
        }).eq("id", article_id).execute()

        claude_result = _call_claude(article, result.full_text, audience_doc)
        if claude_result is None:
            continue

        supabase.table(TABLE).update({
            "deep_summary":    claude_result["deep_summary"],
            "key_facts":       claude_result["key_facts"],
            "relevance_notes": claude_result["relevance_notes"],
            "summarised_at":   datetime.now(timezone.utc).isoformat(),
        }).eq("id", article_id).execute()
        summarised += 1
        logger.debug("Article %s researched (%d words)", article_id, result.word_count)

    for cluster_id in research_cluster_ids:
        supabase.table(CLUSTERS_TABLE).update(
            {"cluster_status": "researched"}
        ).eq("cluster_id", cluster_id).execute()

    logger.info(
        "Research complete — %d scraped, %d summarised, %d paywalled, %d failed",
        scraped, summarised, paywalled, failed,
    )
