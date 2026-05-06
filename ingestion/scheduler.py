"""
Scheduler — runs the full daily pipeline at 5am UTC.

Single job:
  05:00 UTC daily → ingest → filter → cluster → score → tag → daily-brief
"""

import logging

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from ingestion.fetcher import fetch_all_sources
from ingestion.storage import upsert_articles
from filtering.filter import run_filtering
from clustering.cluster import run_clustering
from scoring.score import run_scoring
from tagging.tag import run_tagging
from daily_brief.daily_brief import run_daily_brief

logger = logging.getLogger(__name__)


def run_ingestion() -> None:
    """Fetch all sources and store raw articles."""
    logger.info("Ingestion started")
    articles = fetch_all_sources()
    if articles:
        upsert_articles(articles)
    logger.info("Ingestion complete — %d articles fetched", len(articles))


def run_daily_pipeline() -> None:
    """
    Full daily pipeline:
      1. Ingest from all sources
      2. Filter new articles
      3. Cluster — week continuity pass then new story grouping
      4. Score pending clusters
      5. Tag scored clusters
      6. Synthesise daily brief from scored stories above score threshold
    """
    from datetime import date
    today = date.today().isoformat()

    def _run(name: str, fn, *args, **kwargs) -> None:
        try:
            fn(*args, **kwargs)
        except Exception as exc:
            logger.error("Pipeline stage '%s' failed: %s", name, exc, exc_info=True)

    logger.info("=== Daily pipeline started (processing %s) ===", today)
    _run("ingest",       run_ingestion)
    _run("filter",       run_filtering,    run_date=today)
    _run("cluster",      run_clustering,   run_date=today)
    _run("score",        run_scoring,      run_date=today)
    _run("tag",          run_tagging,      run_date=today)
    _run("daily-brief",  run_daily_brief,  run_date=today)
    logger.info("=== Daily pipeline complete ===")


def start_scheduler() -> None:
    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(
        run_daily_pipeline,
        CronTrigger(hour=5, minute=0, timezone="UTC"),
        id="daily_pipeline",
        replace_existing=True,
    )
    logger.info("Scheduler started — daily pipeline runs at 05:00 UTC")
    scheduler.start()
