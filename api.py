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
from research.research import run_research
from ingestion.competitors import run_competitors

API_KEY = os.environ.get("PIPELINE_API_KEY", "")


def _check_key(x_api_key: str) -> None:
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")


@asynccontextmanager
async def lifespan(app: FastAPI):
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
def run_research_endpoint(background_tasks: BackgroundTasks, date: str | None = None, x_api_key: str = Header(default="")):
    _check_key(x_api_key)
    background_tasks.add_task(run_research, run_date=date)
    return {"status": "started"}


@app.post("/run/daily-brief")
def run_daily_brief_endpoint(background_tasks: BackgroundTasks, date: str | None = None, x_api_key: str = Header(default="")):
    _check_key(x_api_key)
    background_tasks.add_task(run_daily_brief, run_date=date)
    return {"status": "started"}


@app.post("/run/competitors")
def run_competitors_endpoint(background_tasks: BackgroundTasks, x_api_key: str = Header(default="")):
    """Run the competitor scrape (follower counts + recent post engagement)."""
    _check_key(x_api_key)
    background_tasks.add_task(run_competitors)
    return {"status": "started"}


@app.post("/run/pipeline")
def run_pipeline(background_tasks: BackgroundTasks, x_api_key: str = Header(default="")):
    """Run the full daily pipeline immediately — same as the scheduled 05:00 UTC job."""
    _check_key(x_api_key)
    background_tasks.add_task(run_daily_pipeline)
    return {"status": "started"}
