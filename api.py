"""
FastAPI entry point for Railway.

Exposes HTTP endpoints so the admin app can trigger pipeline stages.
The APScheduler daily job starts in a background thread on startup.
"""

import os
import threading
from contextlib import asynccontextmanager

from fastapi import BackgroundTasks, FastAPI, Header, HTTPException

from ingestion.scheduler import run_ingestion, start_scheduler, run_daily_pipeline
from filtering.filter import run_filtering
from clustering.cluster import run_clustering
from scoring.score import run_scoring
from briefing.brief import run_briefing
from tagging.tag import run_tagging
from daily_brief.daily_brief import run_daily_brief
from research.research import run_research, run_research_article
from research.site_auth import (
    SiteAuthUnavailable,
    force_capture,
    run_capture_session,
    start_login,
)
from ingestion.competitors import run_competitors

import logging

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
def run_research_endpoint(background_tasks: BackgroundTasks, date: str | None = None, id: str | None = None, x_api_key: str = Header(default="")):
    """
    Run the research stage. Pass ?id=<article_id> to research a single article
    on demand (ignores cluster score + prior scrape_status); omit it to run the
    batch over scored clusters for ?date (the daily job).
    """
    _check_key(x_api_key)
    if id:
        background_tasks.add_task(run_research_article, id)
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
