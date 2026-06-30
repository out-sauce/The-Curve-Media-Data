"""
Research stage — scrapes full article text and generates deep summaries.

Runs after tagging. Only processes articles in scored clusters that score
>= research_score_threshold (default 0.60). Transitions cluster status
to 'researched' once all articles are processed.

Each article URL is rendered in a real logged-in Chromium tab (Playwright,
research/browser_scraper.py), seeded with a per-publisher-domain login session
(site_auth.storage_state, written by the portal). A subscriber session is what
beats the paywall; extraction stays deterministic (trafilatura over rendered HTML).
Set RESEARCH_USE_BROWSER=false to fall back to the static httpx scraper.

Results stored directly on news_articles rows:
  full_text, word_count, scrape_status, scrape_method, scraped_at
  deep_summary, key_facts, relevance_notes, summarised_at

Stale-auth signal: after each scrape on a domain that had a stored storage_state,
this app writes site_auth.last_status / last_used_at so the portal can flag a session
that has gone stale (subscriber sessions expire at ~7 or 30 days) and prompt re-capture.

Idempotent: articles with scrape_status IS NOT NULL are skipped on re-run
(except 'failed' — those are retried).
"""

import json
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any

import anthropic

from config import (
    ANTHROPIC_API_KEY,
    MAX_BROWSER_SCRAPES_PER_RUN,
    RESEARCH_USE_BROWSER,
)
from ingestion.storage import get_client, get_pipeline_settings, TABLE
from .browser_scraper import scrape_article_with_browser
from .domains import _TWO_LABEL_TLDS, registrable_domain as _registrable_domain
from .scraper import scrape_article

logger = logging.getLogger(__name__)

SITE_AUTH_TABLE = "site_auth"

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


# Registrable-domain helpers (_registrable_domain, _TWO_LABEL_TLDS) now live in
# research/domains.py — the single source of truth shared with the site_auth capture
# path, so the write key and this read key cannot drift. Imported above.


def _fetch_auth_by_domain(domains: list[str]) -> dict[str, dict]:
    """Return {domain: storage_state} for domains with a stored site_auth row."""
    if not domains:
        return {}
    client = get_client()
    resp = (
        client.table(SITE_AUTH_TABLE)
        .select("domain, storage_state")
        .in_("domain", domains)
        .execute()
    )
    return {
        row["domain"]: row["storage_state"]
        for row in (resp.data or [])
        if row.get("storage_state")
    }


def _record_auth_usage(domain: str, status: str, now: str) -> None:
    """
    Write the stale-auth freshness signal back to site_auth so the portal can flag
    a session that has gone stale and prompt re-capture. Best-effort — never aborts
    the run if the write fails.
    """
    client = get_client()
    try:
        (
            client.table(SITE_AUTH_TABLE)
            .update({"last_status": status, "last_used_at": now, "updated_at": now})
            .eq("domain", domain)
            .execute()
        )
    except Exception as exc:
        logger.warning("Could not update site_auth for %s: %s", domain, exc)


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

    # Per-domain login sessions for the browser scraper (replaces per-source cookies).
    domains = list({_registrable_domain(a["url"]) for a in articles if a.get("url")})
    auth_by_domain = _fetch_auth_by_domain([d for d in domains if d])

    supabase = get_client()
    scraped = paywalled = failed = summarised = 0
    browser_scrapes = 0

    for article in articles:
        article_id   = article["id"]
        url          = article["url"]
        domain       = _registrable_domain(url)
        storage_state = auth_by_domain.get(domain)
        now          = datetime.now(timezone.utc).isoformat()

        # Render in a real logged-in browser tab when enabled and under the per-run
        # cap; otherwise fall back to the static httpx scraper (safe degrade).
        use_browser = RESEARCH_USE_BROWSER and browser_scrapes < MAX_BROWSER_SCRAPES_PER_RUN
        if use_browser:
            browser_scrapes += 1
            scrape_method = "browser"
            result = scrape_article_with_browser(url, storage_state=storage_state)
        else:
            scrape_method = "static"
            result = scrape_article(url, cookie_string=None)

        # Stale-auth write-back: any scrape on a domain that *has* a stored session
        # records its outcome so the portal can flag a stale (7/30-day) session.
        if storage_state is not None:
            _record_auth_usage(domain, result.status, now)

        if result.status != "scraped":
            supabase.table(TABLE).update({
                "scrape_status": result.status,
                "scrape_method": scrape_method,
                "scraped_at":    now,
            }).eq("id", article_id).execute()
            paywalled += result.status == "paywalled"
            failed    += result.status == "failed"
            logger.debug("Article %s: %s — %s", article_id, result.status, result.error)
            continue

        scraped += 1
        supabase.table(TABLE).update({
            "scrape_status": "scraped",
            "scrape_method": scrape_method,
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
        "Research complete — %d scraped, %d summarised, %d paywalled, %d failed "
        "(%d via browser path)",
        scraped, summarised, paywalled, failed, browser_scrapes,
    )
