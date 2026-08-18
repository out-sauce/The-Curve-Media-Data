"""
Scheduler — runs the full daily pipeline at 5am UTC, plus a separate hourly job.

Jobs:
  05:00 UTC daily → ingest → filter → cluster → score → tag → research → brief
  (+ competitors, Mondays only)
  hourly (every :05) → Zernio self-Instagram Insights refresh (ingestion/zernio.py)
  — kept off the daily job because it needs its own cadence: Zernio caches post
  analytics for 60 minutes server-side so hourly is exactly the useful rate, and
  Instagram stories only live for 24 hours, so anything slower loses them outright.
  The daily pipeline calls run_zernio_daily instead, which is the same work plus the
  once-a-day extras (audience demographics, which Meta delays 48h anyway).
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
from research.research import run_research
from briefing.brief import run_briefing
from ingestion.competitors import run_competitors
from ingestion.guest_posts import run_guest_post_stats
from ingestion.zernio import run_zernio_hourly, run_zernio_daily
from drafting.draft import run_inbox_drafts
from ingestion.inbox import run_inbox_sweep

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
      6. Research — scrape full text + deep summaries for clusters above the
         research score threshold, transitioning them to 'researched'
      7. Brief — editorial name + brief for researched clusters that produced a
         deep summary (cluster_status stays 'researched')
      8. Social scan — The Curve's own channels (is_self) every day, so follower
         snapshots and content_stats stay daily; the full competitor sweep runs
         Mondays only (manual /run/competitors is unaffected).
      9. Guest posts — per-post-URL Apify scrape for calendar items posted from
         a guest's own account (social_kind='guest'), within the lookback window.
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
    _run("research",     run_research,     run_date=today)
    _run("brief",        run_briefing,     run_date=today)
    if date.today().weekday() == 0:  # Monday
        _run("competitors", run_competitors)
    else:
        logger.info("Full competitor sweep skipped — runs weekly on Mondays; scanning own channels only")
        _run("competitors", run_competitors, self_only=True)
    _run("guest_posts",  run_guest_post_stats)
    _run("zernio",       run_zernio_daily)
    logger.info("=== Daily pipeline complete ===")


def start_scheduler() -> None:
    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(
        run_daily_pipeline,
        CronTrigger(hour=5, minute=0, timezone="UTC"),
        id="daily_pipeline",
        replace_existing=True,
    )
    scheduler.add_job(
        run_zernio_hourly,
        # :05 rather than :00 — the daily pipeline fires at 05:00 and the two would
        # otherwise start together, doubling up on the same account for no gain.
        CronTrigger(minute=5, timezone="UTC"),
        id="zernio_hourly",
        replace_existing=True,
    )
    scheduler.add_job(
        run_inbox_sweep,
        CronTrigger(minute="*/15", timezone="UTC"),
        id="inbox_sweep",
        replace_existing=True,
    )
    scheduler.add_job(
        run_inbox_sweep,
        # The exhaustive pass. It is the ONLY thing that can find a conversation from
        # Meta's pre-connect replay: those arrive in the background, emit no webhooks,
        # and keep their original timestamps, so they sort into date order rather than
        # to the top where an incremental pass would see them.
        CronTrigger(hour=4, minute=30, timezone="UTC"),
        id="inbox_sweep_full",
        kwargs={"full": True},
        replace_existing=True,
    )
    scheduler.add_job(
        run_inbox_drafts,
        # Hourly at :35, where this used to be once a day at 05:30. It no longer waits for
        # a thread to go stale (DRAFT_MIN_AGE_HOURS=0) — it judges whatever is waiting on
        # us — and the Admin's To-reply list is only as good as the last run, so a daily
        # cadence would leave a day of DMs unjudged. Cheap to repeat: skip-if-current means
        # an unchanged inbox costs two listing queries and no model calls.
        # :35 sits after the :15/:30 inbox sweeps, so it judges a settled mirror.
        CronTrigger(minute=35, timezone="UTC"),
        id="inbox_drafts",
        replace_existing=True,
    )
    logger.info(
        "Scheduler started — daily pipeline at 05:00 UTC, Zernio refresh hourly at :05, "
        "inbox sweep every 15m (full at 04:30), inbox triage + drafts hourly at :35"
    )
    scheduler.start()
