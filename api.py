"""
FastAPI entry point for Railway.

Exposes HTTP endpoints so the admin app can trigger pipeline stages.
The APScheduler daily job starts in a background thread on startup.
"""

import logging
import os
import sys
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timezone

# Configure root logging for the uvicorn/Railway entrypoint. main.py does this for the
# CLI, but api.py never did — so under uvicorn the app loggers had no handler and Python
# emitted only WARNING+, silently dropping every INFO line (scrape statuses, "Research
# complete", "site_auth captured"). Mirror main.py's setup so those are visible in
# Railway logs. LOG_LEVEL env overrides (default INFO).
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)

from fastapi import BackgroundTasks, FastAPI, Header, HTTPException
from pydantic import BaseModel

from ingestion.scheduler import run_ingestion, start_scheduler, run_daily_pipeline
from filtering.filter import run_filtering
from clustering.cluster import run_clustering
from scoring.score import run_scoring
from briefing.brief import run_briefing
from tagging.tag import run_tagging
from daily_brief.daily_brief import run_daily_brief
from research.research import (
    claim_pending,
    complete_from_html,
    enqueue_article,
    resolve_article_by_url,
    run_research,
    run_research_article,
    run_research_cluster,
)
from research.site_auth import (
    SiteAuthUnavailable,
    force_capture,
    import_storage_state,
    run_capture_session,
    start_login,
)
from ingestion.competitors import run_competitors
from ingestion.storage import get_client
from research.domains import registrable_domain

logger = logging.getLogger(__name__)

API_KEY = os.environ.get("PIPELINE_API_KEY", "")


def _check_key(x_api_key: str) -> None:
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # The site-auth capture flow keeps an in-process session registry
    # (research/site_auth.py), so the service MUST run as a single replica — a login
    # started on one request must be found by its capture task in the same process.
    # Keep the Railway service at 1 replica; this logs the assumption at startup.
    replicas = os.environ.get("RAILWAY_REPLICA_COUNT")
    if replicas and replicas.strip() not in ("", "1"):
        logger.error(
            "Multiple replicas (%s) detected — the in-process site_auth session "
            "registry requires a SINGLE replica; login captures will break.",
            replicas,
        )
    else:
        logger.info("Single-replica assumption OK for in-process site_auth registry.")

    thread = threading.Thread(target=start_scheduler, daemon=True)
    thread.start()
    yield


app = FastAPI(lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/run/ingest")
def run_ingest(background_tasks: BackgroundTasks, x_api_key: str = Header(default="")):
    _check_key(x_api_key)
    background_tasks.add_task(run_ingestion)
    return {"status": "started"}


@app.post("/run/filter")
def run_filter(background_tasks: BackgroundTasks, date: str | None = None, x_api_key: str = Header(default="")):
    _check_key(x_api_key)
    background_tasks.add_task(run_filtering, run_date=date)
    return {"status": "started"}


@app.post("/run/scan")
def run_scan(background_tasks: BackgroundTasks, date: str | None = None, x_api_key: str = Header(default="")):
    """Ingest then filter in sequence — ensures filter only runs after ingest completes."""
    _check_key(x_api_key)
    def _scan(run_date: str | None) -> None:
        run_ingestion()
        run_filtering(run_date=run_date)
    background_tasks.add_task(_scan, date)
    return {"status": "started"}


@app.post("/run/cluster")
def run_cluster(background_tasks: BackgroundTasks, date: str | None = None, x_api_key: str = Header(default="")):
    _check_key(x_api_key)
    background_tasks.add_task(run_clustering, run_date=date)
    return {"status": "started"}


@app.post("/run/score")
def run_score(background_tasks: BackgroundTasks, date: str | None = None, x_api_key: str = Header(default="")):
    """Score clusters then tag them in sequence."""
    _check_key(x_api_key)
    def _score_and_tag(run_date: str | None) -> None:
        run_scoring(run_date=run_date)
        run_tagging(run_date=run_date)
    background_tasks.add_task(_score_and_tag, date)
    return {"status": "started"}


@app.post("/run/tag")
def run_tag(background_tasks: BackgroundTasks, date: str | None = None, x_api_key: str = Header(default="")):
    _check_key(x_api_key)
    background_tasks.add_task(run_tagging, run_date=date)
    return {"status": "started"}


@app.post("/run/brief")
def run_brief(background_tasks: BackgroundTasks, date: str | None = None, x_api_key: str = Header(default="")):
    _check_key(x_api_key)
    background_tasks.add_task(run_briefing, run_date=date)
    return {"status": "started"}


@app.post("/run/research")
def run_research_endpoint(background_tasks: BackgroundTasks, date: str | None = None, id: str | None = None, cluster_id: str | None = None, x_api_key: str = Header(default="")):
    """
    Run the research stage. Pass ?id=<article_id> to research a single article
    on demand (ignores cluster score + prior scrape_status), or
    ?cluster_id=<cluster_id> to research a whole story on demand (skips articles
    that already have a deep summary, routes manual domains to the extension
    queue, then regenerates the cluster brief); omit both to run the batch over
    scored clusters for ?date (the daily job).
    """
    _check_key(x_api_key)
    if id:
        background_tasks.add_task(run_research_article, id)
    elif cluster_id:
        background_tasks.add_task(run_research_cluster, cluster_id)
    else:
        background_tasks.add_task(run_research, run_date=date)
    return {"status": "started"}


@app.post("/run/daily-brief")
def run_daily_brief_endpoint(background_tasks: BackgroundTasks, date: str | None = None, x_api_key: str = Header(default="")):
    _check_key(x_api_key)
    background_tasks.add_task(run_daily_brief, run_date=date)
    return {"status": "started"}


@app.post("/run/competitors")
def run_competitors_endpoint(background_tasks: BackgroundTasks, id: str | None = None, x_api_key: str = Header(default="")):
    """
    Run the competitor scrape (follower counts + recent post engagement).
    Pass ?id=<competitor_id> to refresh a single competitor (admin card
    Refresh); omit it to refresh all (the daily job).
    """
    _check_key(x_api_key)
    background_tasks.add_task(run_competitors, id)
    return {"status": "started"}


@app.post("/run/pipeline")
def run_pipeline(background_tasks: BackgroundTasks, x_api_key: str = Header(default="")):
    """Run the full daily pipeline immediately — same as the scheduled 05:00 UTC job."""
    _check_key(x_api_key)
    background_tasks.add_task(run_daily_pipeline)
    return {"status": "started"}


@app.post("/site-auth/login/start")
def site_auth_login_start(
    background_tasks: BackgroundTasks,
    domain: str,
    label: str | None = None,
    x_api_key: str = Header(default=""),
):
    """
    Launch a headful remote browser at <domain> for a human-driven publisher login and
    return {session_id, live_url}. The Admin "Log in" button calls this, opens live_url
    in a new tab, then polls site_auth for a fresh captured_at. We schedule a background
    task that navigates the remote browser, watches for the publisher's auth cookie and
    upserts site_auth on a genuine login (or at the hard timeout).

    Returns 404 when Browserbase is not yet provisioned — Admin maps that to its
    existing "remote login not yet available" message.
    """
    _check_key(x_api_key)
    try:
        result = start_login(domain, label)
    except SiteAuthUnavailable as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    background_tasks.add_task(run_capture_session, result["session_id"])
    return result


class SiteAuthImport(BaseModel):
    domain: str
    cookies: list[dict]
    origins: list[dict] | None = None
    label: str | None = None


class ResearchImport(BaseModel):
    html: str
    article_id: int | None = None   # queue lane: known from the claimed item
    url: str | None = None          # popup lane: only the page URL is known
    queue_id: int | None = None


@app.post("/site-auth/import")
def site_auth_import(payload: SiteAuthImport, x_api_key: str = Header(default="")):
    """
    Import a login session captured in a real, human browser — the Curve Auth Chrome
    extension or a Cookie-Editor JSON export — instead of a remote Browserbase login.
    Converts the browser cookie list (+ optional localStorage origins) into a Playwright
    storage_state and upserts site_auth, keyed by the registrable base of `domain` (the
    exact row the research scraper reads). No automation touches the publisher, so there
    is nothing to detect — use it for sites hostile to remote browsers (WSJ, AFR).

    Returns {status, domain, cookies, origins} counts on success. 400 on bad input,
    500 if the DB write fails (unlike the fire-and-forget capture task, the interactive
    caller must see a real result).
    """
    _check_key(x_api_key)
    try:
        return import_storage_state(
            payload.domain, payload.cookies, payload.origins, payload.label
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Import failed: {exc}")


@app.post("/site-auth/login/finish")
def site_auth_login_finish(session_id: str, x_api_key: str = Header(default="")):
    """
    Manual backstop: force an immediate storage_state capture + upsert for an in-flight
    session. Admin never calls this (its modal only polls site_auth); it ships per the
    resolved decision for operator use. 404 if the session is unknown/already done.
    """
    _check_key(x_api_key)
    if not force_capture(session_id):
        raise HTTPException(status_code=404, detail="Unknown or already-finished session")
    return {"status": "capturing"}


@app.post("/sources/scrape-mode")
def set_scrape_mode(domain: str, mode: str, x_api_key: str = Header(default="")):
    """
    Set the research scrape policy for a publisher domain: 'auto' (the daily batch may
    attempt an automated logged-in scrape) or 'manual' (the batch skips it; scraped only
    on manual initiation). Keyed by registrable domain, so every feed of one publisher
    shares the setting. Backs the Auto/Manual toggle on the Admin Sources page; the batch
    also auto-demotes a domain to 'manual' on a bot_wall/paywalled result.
    """
    _check_key(x_api_key)
    if mode not in ("auto", "manual"):
        raise HTTPException(status_code=400, detail="mode must be 'auto' or 'manual'")
    base = registrable_domain(domain)
    if not base:
        raise HTTPException(status_code=400, detail="invalid domain")
    try:
        get_client().table("domain_scrape_settings").upsert(
            {
                "domain": base,
                "scrape_mode": mode,
                "last_reason": "manual toggle",
                "updated_at": datetime.now(timezone.utc).isoformat(),
            },
            on_conflict="domain",
        ).execute()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Update failed: {exc}")
    return {"status": "ok", "domain": base, "scrape_mode": mode}


# --- Extension content-grab lane (manual initiation) ------------------------
# Admin's "Research" button enqueues an article; the Curve Auth Chrome extension polls
# /research/queue/claim, opens the page in the operator's real logged-in browser, and
# posts the rendered HTML to /research/import. This is how 'manual' (bot-walled) domains
# get researched without any server-side fetch for a bot detector to flag.

@app.post("/research/enqueue")
def research_enqueue(id: str, x_api_key: str = Header(default="")):
    """Queue an article for the extension to grab. Idempotent per outstanding request."""
    _check_key(x_api_key)
    try:
        return enqueue_article(id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Enqueue failed: {exc}")


@app.post("/research/queue/claim")
def research_queue_claim(limit: int = 5, x_api_key: str = Header(default="")):
    """Extension poll: return pending items and mark them claimed. {items:[{queue_id,article_id,url}]}."""
    _check_key(x_api_key)
    return {"items": claim_pending(limit)}


@app.post("/research/import")
def research_import(payload: ResearchImport, x_api_key: str = Header(default="")):
    """
    Ingest article HTML captured by the extension in a logged-in browser: extract text,
    write full_text + deep summary, and close the queue row. Returns the resulting
    scrape status so the extension can surface success/failure.

    Identify the article by `article_id` (queue lane) or by `url` (popup "Send article
    content" — resolved to the matching news_articles row, 404 if none matches).
    """
    _check_key(x_api_key)
    if not payload.html:
        raise HTTPException(status_code=400, detail="html is required")
    article_id, queue_id = payload.article_id, payload.queue_id
    if article_id is None:
        if not payload.url:
            raise HTTPException(status_code=400, detail="article_id or url is required")
        try:
            article_id, queue_id = resolve_article_by_url(payload.url)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc))
    return complete_from_html(queue_id, article_id, payload.html)
