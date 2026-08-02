"""
Utility: reset all clustering/scoring/briefing data for a given date
so the cluster → score → brief stages can be re-run cleanly.

  python reset_date.py --date 2026-04-04 [--dry-run]

What it does — note the split, which exists because a cluster's `date` is no longer
immutable (migration 031: the clustering stage bumps a running story's date forward
whenever it gains articles):

  - Clusters BORN on the date (created_at::date == date): every article pointing at
    them is detached (status → 'included', cluster_id → NULL) and the cluster rows are
    deleted. The article-side filter is cluster_id, not fetched_at, because an extended
    story holds articles fetched on earlier days.
  - Clusters that merely GREW into the date (created_at is earlier): deleting these
    would destroy a running story and permanently orphan the earlier days' articles —
    they would keep a cluster_id pointing at a deleted row, so no future clustering run
    would ever see them again. Instead only the articles fetched on this date are
    detached, the count is recomputed, and the story is rolled back to the day it
    started. It is deleted only if that leaves it empty.
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

logger = logging.getLogger(__name__)

from clustering.cluster import _count_cluster_articles
from ingestion.storage import get_client, TABLE

CLUSTERS_TABLE = "story_clusters"

CHUNK = 50


def reset_date(date: str, dry_run: bool = False) -> None:
    supabase = get_client()

    rows = (
        supabase.table(CLUSTERS_TABLE)
        .select("cluster_id, created_at, name")
        .eq("date", date)
        .execute()
        .data
    ) or []

    born, extended = [], []
    for r in rows:
        (born if str(r.get("created_at") or "")[:10] == date else extended).append(r)

    logger.info("%s: %d cluster(s) — %d born on this date, %d extended into it",
                date, len(rows), len(born), len(extended))

    if dry_run:
        for r in born:
            logger.info("  [delete]   %s  %s", r["cluster_id"], r.get("name"))
        for r in extended:
            logger.info("  [rollback] %s  %s  (started %s)",
                        r["cluster_id"], r.get("name"), str(r.get("created_at"))[:10])
        logger.info("Dry run — nothing written")
        return

    # ── Clusters born on this date: detach every article, then delete ────────
    # The filter is cluster_id, not status. Clustering leaves articles at 'included',
    # so the old .in_("status", ["accepted","briefed","published"]) filter matched zero
    # rows and cluster_id was never actually cleared.
    for i in range(0, len(born), CHUNK):
        chunk = [r["cluster_id"] for r in born[i:i + CHUNK]]
        supabase.table(TABLE).update({
            "status": "included",
            "cluster_id": None,
        }).in_("cluster_id", chunk).execute()
        supabase.table(CLUSTERS_TABLE).delete().in_("cluster_id", chunk).execute()
    logger.info("Deleted %d cluster(s) created on %s", len(born), date)

    # ── Clusters that grew into this date: detach only this date's articles ──
    for r in extended:
        cluster_id = r["cluster_id"]
        origin = str(r.get("created_at"))[:10]
        supabase.table(TABLE).update({
            "status": "included",
            "cluster_id": None,
        }).eq("cluster_id", cluster_id) \
          .gte("fetched_at", f"{date}T00:00:00.000Z") \
          .lte("fetched_at", f"{date}T23:59:59.999Z").execute()

        count = _count_cluster_articles(supabase, cluster_id)
        if count == 0:
            supabase.table(CLUSTERS_TABLE).delete().eq("cluster_id", cluster_id).execute()
            logger.warning("Cluster %s emptied by reset — deleted", cluster_id)
            continue

        supabase.table(CLUSTERS_TABLE).update({
            "article_count": count,
            "date": origin,
        }).eq("cluster_id", cluster_id).execute()
        logger.info("Rolled back extended story '%s' (%s) to %s — %d article(s) left",
                    r.get("name"), cluster_id, origin, count)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, metavar="YYYY-MM-DD")
    parser.add_argument("--dry-run", action="store_true",
                        help="print the born/extended split without writing anything")
    args = parser.parse_args()
    reset_date(args.date, dry_run=args.dry_run)
