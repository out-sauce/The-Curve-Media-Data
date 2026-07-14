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

Per-domain scrape policy (domain_scrape_settings, keyed by registrable domain):
'auto' domains are scraped by the daily batch; 'manual' domains are skipped here and
scraped only on manual initiation (run_research_article / the on-demand extension lane).
The batch auto-demotes a domain to 'manual' the first time an automated scrape returns
'bot_wall' or 'paywalled', so the subscriber login is sent to a hostile publisher at
most once — the guard against tying bot-flagged traffic to a paid account. The mode is
toggled on the Admin Sources page. Only run_research honours the policy; an explicit
single-article request overrides it.

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
from .browser_scraper import extract_article_html, scrape_article_with_browser
from .domains import _TWO_LABEL_TLDS, registrable_domain as _registrable_domain
from .scraper import scrape_article

logger = logging.getLogger(__name__)

SITE_AUTH_TABLE = "site_auth"
SCRAPE_SETTINGS_TABLE = "domain_scrape_settings"
QUEUE_TABLE = "research_queue"
# An automated scrape returning one of these means the batch cannot get this domain with
# the login — demote it to manual so a hostile publisher is not hit again (see run_research).
DEMOTE_STATUSES = {"bot_wall", "paywalled"}

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


def _fetch_scrape_modes(domains: list[str]) -> dict[str, str]:
    """Return {domain: scrape_mode} for domains with a domain_scrape_settings row.
    A domain with no row is treated as 'auto' by the caller."""
    if not domains:
        return {}
    client = get_client()
    resp = (
        client.table(SCRAPE_SETTINGS_TABLE)
        .select("domain, scrape_mode")
        .in_("domain", domains)
        .execute()
    )
    return {row["domain"]: row["scrape_mode"] for row in (resp.data or [])}


def _demote_domain(domain: str, reason: str, now: str) -> None:
    """
    Flip a domain to 'manual' after an automated scrape hit a wall, so the daily batch
    stops sending the subscriber login to a hostile publisher. Best-effort — never aborts
    the run if the write fails (mirrors _record_auth_usage).
    """
    client = get_client()
    try:
        (
            client.table(SCRAPE_SETTINGS_TABLE)
            .upsert(
                {
                    "domain": domain,
                    "scrape_mode": "manual",
                    "last_reason": reason,
                    "updated_at": now,
                },
                on_conflict="domain",
            )
            .execute()
        )
        logger.info("Domain %s demoted to manual scrape (%s)", domain, reason)
    except Exception as exc:
        logger.warning("Could not demote domain %s to manual: %s", domain, exc)


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
# Per-article processing
# ---------------------------------------------------------------------------

def _process_article(
    article: dict[str, Any],
    storage_state: dict | None,
    audience_doc: str,
    use_browser: bool,
    supabase,
) -> tuple[str, bool]:
    """
    Scrape + deep-summarise a single article, writing results to its news_articles row.

    Returns (scrape_status, summarised) where scrape_status is one of
    'scraped' | 'paywalled' | 'bot_wall' | 'failed' and summarised is True when a
    Claude deep summary was written. 'bot_wall' is an anti-bot / CAPTCHA challenge
    (distinct from a subscription paywall). Records the stale-auth freshness signal
    when the domain has a stored session. Never raises on scrape failure — those come
    back as a 'failed' status.
    """
    article_id = article["id"]
    url        = article["url"]
    domain     = _registrable_domain(url)
    now        = datetime.now(timezone.utc).isoformat()

    if use_browser:
        scrape_method = "browser"
        result = scrape_article_with_browser(url, storage_state=storage_state)
    else:
        scrape_method = "static"
        result = scrape_article(url, cookie_string=None)

    # Stale-auth write-back: any scrape on a domain that *has* a stored session
    # records its outcome so the portal can flag a stale (7/30-day) session.
    if storage_state is not None:
        _record_auth_usage(domain, result.status, now)

    return _persist_result(article, result, scrape_method, audience_doc, supabase)


def _persist_result(
    article: dict[str, Any],
    result,
    scrape_method: str,
    audience_doc: str,
    supabase,
) -> tuple[str, bool]:
    """
    Persist a ScrapeResult onto the article's news_articles row and, when the scrape
    succeeded, write a Claude deep summary. Shared by the server-side scrape path
    (_process_article) and the extension content-import path (research_from_html) so the
    write contract cannot drift. Returns (scrape_status, summarised).
    """
    article_id = article["id"]
    now = datetime.now(timezone.utc).isoformat()

    if result.status != "scraped":
        supabase.table(TABLE).update({
            "scrape_status": result.status,
            "scrape_method": scrape_method,
            "scraped_at":    now,
        }).eq("id", article_id).execute()
        logger.debug("Article %s: %s — %s", article_id, result.status, result.error)
        return result.status, False

    supabase.table(TABLE).update({
        "scrape_status": "scraped",
        "scrape_method": scrape_method,
        "full_text":     result.full_text,
        "word_count":    result.word_count,
        "scraped_at":    now,
    }).eq("id", article_id).execute()

    claude_result = _call_claude(article, result.full_text, audience_doc)
    if claude_result is None:
        return "scraped", False

    supabase.table(TABLE).update({
        "deep_summary":    claude_result["deep_summary"],
        "key_facts":       claude_result["key_facts"],
        "relevance_notes": claude_result["relevance_notes"],
        "summarised_at":   datetime.now(timezone.utc).isoformat(),
    }).eq("id", article_id).execute()
    logger.debug("Article %s researched (%d words)", article_id, result.word_count)
    return "scraped", True


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

    # Per-domain scrape policy. 'manual' domains are skipped here and handled only on
    # manual initiation (run_research_article) — this is the account-safety guard: the
    # subscriber login is never sent to a domain we already know walls us. A domain
    # auto-demotes to 'manual' the first time an automated scrape returns bot_wall or
    # paywalled, so a hostile publisher is hit with the login at most once.
    domains = list({_registrable_domain(a["url"]) for a in articles if a.get("url")})
    scrape_modes = _fetch_scrape_modes(domains)
    # Only fetch login sessions for domains we'll actually auto-scrape (avoids loading
    # 1-2 MB storage_state blobs for manual domains we won't touch).
    auto_domains = [d for d in domains if d and scrape_modes.get(d, "auto") == "auto"]
    auth_by_domain = _fetch_auth_by_domain(auto_domains)

    # Prioritise articles on auth-backed domains for the limited browser budget
    # (MAX_BROWSER_SCRAPES_PER_RUN): only the logged-in browser can beat their
    # paywall, whereas open domains usually scrape fine on the static fallback.
    # Stable sort keeps the original order within each group; False (0) < True (1),
    # so auth-backed domains come first.
    articles.sort(
        key=lambda a: _registrable_domain(a.get("url") or "") not in auth_by_domain
    )

    supabase = get_client()
    scraped = paywalled = bot_wall = failed = summarised = manual_skipped = 0
    browser_scrapes = 0
    demoted: set[str] = set()  # domains demoted mid-run — skip their remaining articles

    for article in articles:
        domain = _registrable_domain(article["url"])
        mode   = "manual" if domain in demoted else scrape_modes.get(domain, "auto")
        if mode == "manual":
            # Left untouched for the on-demand / extension lane. No scrape, no cookies.
            manual_skipped += 1
            continue

        storage_state = auth_by_domain.get(domain)

        # Render in a real logged-in browser tab when enabled and under the per-run
        # cap; otherwise fall back to the static httpx scraper (safe degrade).
        use_browser = RESEARCH_USE_BROWSER and browser_scrapes < MAX_BROWSER_SCRAPES_PER_RUN
        if use_browser:
            browser_scrapes += 1

        status, did_summarise = _process_article(
            article, storage_state, audience_doc, use_browser, supabase
        )
        scraped    += status == "scraped"
        paywalled  += status == "paywalled"
        bot_wall   += status == "bot_wall"
        failed     += status == "failed"
        summarised += did_summarise

        # Wall hit on an automated read → demote the domain and skip its remaining
        # articles this run, so we don't send the login to it again.
        if domain and status in DEMOTE_STATUSES:
            _demote_domain(domain, status, datetime.now(timezone.utc).isoformat())
            demoted.add(domain)

    for cluster_id in research_cluster_ids:
        supabase.table(CLUSTERS_TABLE).update(
            {"cluster_status": "researched"}
        ).eq("cluster_id", cluster_id).execute()

    logger.info(
        "Research complete — %d scraped, %d summarised, %d paywalled, %d bot_wall, "
        "%d failed, %d manual-skipped (%d via browser path)",
        scraped, summarised, paywalled, bot_wall, failed, manual_skipped, browser_scrapes,
    )


def run_research_article(article_id: str) -> None:
    """
    Research a single article on demand, regardless of its cluster's score or
    prior scrape_status (an explicit request overrides the batch's skip/threshold
    rules). Scrapes full text + writes a deep summary onto the news_articles row,
    exactly as the batch path does. Does not touch cluster_status — one article
    does not mean the whole cluster is researched.
    """
    logger.info("Research (single) started for article %s", article_id)

    client = get_client()
    resp = (
        client.table(TABLE)
        .select("id, url, title, summary, source_id, scrape_status")
        .eq("id", article_id)
        .limit(1)
        .execute()
    )
    rows = resp.data or []
    if not rows:
        logger.warning("Research (single): article %s not found", article_id)
        return
    article = rows[0]
    if not article.get("url"):
        logger.warning("Research (single): article %s has no url", article_id)
        return

    settings     = get_pipeline_settings()
    audience_doc = settings.get("audience_doc") or ""

    domain        = _registrable_domain(article["url"])
    storage_state = _fetch_auth_by_domain([domain] if domain else []).get(domain)

    status, summarised = _process_article(
        article, storage_state, audience_doc, RESEARCH_USE_BROWSER, client
    )
    logger.info(
        "Research (single) complete — article %s: %s (summarised=%s)",
        article_id, status, summarised,
    )


# ---------------------------------------------------------------------------
# Extension content-grab lane (manual initiation)
# ---------------------------------------------------------------------------
# For domains that bot-wall automated reads (scrape_mode='manual'), the article is
# fetched inside a human's real, logged-in browser by the Curve Auth Chrome extension —
# nothing for a bot detector to flag — and its rendered HTML is POSTed back here. The
# Admin "Research" button enqueues the article; the extension polls claim_pending, opens
# the page, and posts the HTML to complete_from_html. No server-side fetch is involved.

def research_from_html(article_id, html: str) -> tuple[str, bool]:
    """
    Research an article from HTML captured by the extension in a logged-in browser.
    Extracts with the same trafilatura pass as the scrapers, then persists the result and
    (when scraped) a Claude deep summary via the shared _persist_result. Returns
    (scrape_status, summarised). Never raises on extraction failure.
    """
    client = get_client()
    rows = (
        client.table(TABLE)
        .select("id, url, title, summary, source_id, scrape_status")
        .eq("id", article_id)
        .limit(1)
        .execute()
        .data
    ) or []
    if not rows:
        logger.warning("Research (extension): article %s not found", article_id)
        return "failed", False
    article = rows[0]

    audience_doc = get_pipeline_settings().get("audience_doc") or ""
    result = extract_article_html(html or "")
    logger.info(
        "Research (extension) article %s: %s (%d words)",
        article_id, result.status, result.word_count or 0,
    )
    return _persist_result(article, result, "extension", audience_doc, client)


def enqueue_article(article_id) -> dict:
    """
    Add an article to the research_queue for the extension to grab. Idempotent: if the
    article already has a pending/claimed request, returns that instead of duplicating.
    Raises ValueError if the article is unknown or has no URL.
    """
    client = get_client()
    rows = client.table(TABLE).select("id, url").eq("id", article_id).limit(1).execute().data or []
    if not rows:
        raise ValueError(f"article {article_id} not found")
    url = rows[0].get("url")
    if not url:
        raise ValueError(f"article {article_id} has no url")

    outstanding = (
        client.table(QUEUE_TABLE)
        .select("id")
        .eq("article_id", article_id)
        .in_("status", ["pending", "claimed"])
        .limit(1)
        .execute()
        .data
    ) or []
    if outstanding:
        return {"status": "already_queued", "queue_id": outstanding[0]["id"], "article_id": article_id}

    try:
        inserted = (
            client.table(QUEUE_TABLE)
            .insert({"article_id": article_id, "url": url, "status": "pending"})
            .execute()
            .data
        ) or []
    except Exception as exc:
        # Partial unique index backstop lost a race — treat as already queued.
        logger.info("Research queue: enqueue race for article %s (%s)", article_id, exc)
        return {"status": "already_queued", "article_id": article_id}

    qid = inserted[0]["id"] if inserted else None
    logger.info("Research queue: enqueued article %s (queue %s)", article_id, qid)
    return {"status": "queued", "queue_id": qid, "article_id": article_id}


def claim_pending(limit: int = 10) -> list[dict]:
    """
    Return up to `limit` pending queue items and mark them 'claimed' so a subsequent poll
    won't hand them out again. Each item is {queue_id, article_id, url}.
    """
    client = get_client()
    pending = (
        client.table(QUEUE_TABLE)
        .select("id, article_id, url")
        .eq("status", "pending")
        .order("requested_at")
        .limit(max(1, min(limit, 50)))
        .execute()
        .data
    ) or []
    ids = [r["id"] for r in pending]
    if ids:
        client.table(QUEUE_TABLE).update(
            {"status": "claimed", "claimed_at": datetime.now(timezone.utc).isoformat()}
        ).in_("id", ids).execute()
    return [{"queue_id": r["id"], "article_id": r["article_id"], "url": r["url"]} for r in pending]


def complete_from_html(queue_id, article_id, html: str) -> dict:
    """
    Research an enqueued article from the extension-supplied HTML and close out its queue
    row ('done' when scraped, else 'failed'). Best-effort on the queue write. Returns
    {status, summarised, queue_status}.
    """
    now = datetime.now(timezone.utc).isoformat()
    status, summarised, error = "failed", False, None
    try:
        status, summarised = research_from_html(article_id, html)
        if status != "scraped":
            error = f"extraction status={status}"
    except Exception as exc:
        error = str(exc)[:300]
        logger.warning("Research queue: import failed for article %s: %s", article_id, exc)

    queue_status = "done" if status == "scraped" else "failed"
    if queue_id is not None:
        try:
            get_client().table(QUEUE_TABLE).update({
                "status":       queue_status,
                "completed_at": now,
                "error":        error,
            }).eq("id", queue_id).execute()
        except Exception as exc:
            logger.warning("Research queue: could not close queue %s: %s", queue_id, exc)

    return {"status": status, "summarised": summarised, "queue_status": queue_status}
