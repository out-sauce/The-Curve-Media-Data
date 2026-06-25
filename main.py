"""
Entry point.

    python -m pipeline.main                   # continuous scheduler (5am UTC daily)
    python -m pipeline.main --once            # full pipeline for yesterday, run once and exit
    python -m pipeline.main --stage ingest    # ingestion only
    python -m pipeline.main --stage filter    # filter yesterday's articles
    python -m pipeline.main --stage cluster   # cluster yesterday's articles
    python -m pipeline.main --stage score     # score yesterday's clusters
    python -m pipeline.main --stage filter --date 2026-04-04   # override date
"""

import argparse
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)

from ingestion.scheduler import (
    run_daily_pipeline,
    run_ingestion,
    start_scheduler,
)
from filtering.filter import run_filtering
from clustering.cluster import run_clustering
from scoring.score import run_scoring
from ingestion.competitors import run_competitors

DATE_STAGES = {"filter", "cluster", "score"}

STAGES = {
    "ingest":       run_ingestion,
    "filter":       run_filtering,
    "cluster":      run_clustering,
    "score":        run_scoring,
    "competitors":  run_competitors,
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Curve Financial News Pipeline")
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--once",
        action="store_true",
        help="Run the full pipeline once and exit",
    )
    group.add_argument(
        "--stage",
        choices=STAGES.keys(),
        metavar="STAGE",
        help=f"Run a single stage and exit. One of: {', '.join(STAGES)}",
    )
    parser.add_argument(
        "--date",
        metavar="YYYY-MM-DD",
        help="Override the target date (default: yesterday)",
    )
    args = parser.parse_args()

    if args.stage:
        stage_fn = STAGES[args.stage]
        if args.stage in DATE_STAGES:
            stage_fn(run_date=args.date)
        else:
            stage_fn()
    elif args.once:
        run_daily_pipeline()
    else:
        start_scheduler()


if __name__ == "__main__":
    main()
