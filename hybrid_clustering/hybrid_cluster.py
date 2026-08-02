"""
Hybrid Clustering — runs after Voyage clustering, before scoring.

Three passes, all in one stage:

  1. NAME    — Claude gives every multi-article cluster a short headline name.
  2. ASSIGN  — Claude checks each singleton article and decides if it thematically
               belongs to one of the named clusters. Matching singletons are merged
               in; their original cluster row is deleted.
  3. ROUNDUP — Remaining singletons are sent through the custom_cluster_prompt to
               form roundup clusters (same idea as before, but output is auto type).

After this stage all clusters are status='pending', cluster_type='auto', and
ready for scoring.

DORMANT — not imported by main.py, api.py or ingestion/scheduler.py, and it predates
migration 031 (story continuation). Do not revive it as-is: it still writes weekly_story
as a story-grouping key (deprecated — a running story is one row now), never stamps
last_article_at (so briefing would treat anything it touches as already current), and
maintains article_count with a non-atomic read-then-increment. Use
clustering.cluster._count_cluster_articles instead.
"""

import json
import logging
import uuid
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any

import anthropic

from config import ANTHROPIC_API_KEY
from ingestion.storage import get_client, get_pipeline_settings, TABLE

logger = logging.getLogger(__name__)

CLUSTERS_TABLE = "story_clusters"
MODEL = "claude-sonnet-4-6"


def _get_monday(d: str) -> str:
    """Return the ISO date string for Monday of the week containing d."""
    dt = date.fromisoformat(d)
    return (dt - timedelta(days=dt.weekday())).isoformat()


# ── DB helpers ────────────────────────────────────────────────────────────────

def _fetch_pending_clusters(run_date: str) -> tuple[list[dict], list[dict]]:
    """Return (multi_article_clusters, singleton_clusters) for run_date."""
    client = get_client()
    resp = (
        client.table(CLUSTERS_TABLE)
        .select("id, cluster_id, name, anchor_article_id, article_count")
        .eq("date", run_date)
        .eq("cluster_status", "pending")
        .execute()
    )
    clusters = resp.data or []
    multi = [c for c in clusters if (c.get("article_count") or 0) > 1]
    singles = [c for c in clusters if (c.get("article_count") or 0) == 1]
    return multi, singles


def _fetch_articles_for_clusters(cluster_ids: list[str]) -> dict[str, list[dict]]:
    """Return {cluster_id: [articles]} for the given cluster IDs."""
    client = get_client()
    grouped: dict[str, list] = defaultdict(list)
    for i in range(0, len(cluster_ids), 50):
        chunk = cluster_ids[i: i + 50]
        resp = (
            client.table(TABLE)
            .select("id, cluster_id, title, summary, source_name")
            .in_("cluster_id", chunk)
            .execute()
        )
        for a in (resp.data or []):
            grouped[a["cluster_id"]].append(a)
    return grouped


# ── Step 1: Naming ────────────────────────────────────────────────────────────

def _name_clusters(
    clusters: list[dict],
    articles_by_cluster: dict[str, list],
    week_names: list[str],
) -> dict[str, str]:
    """
    Ask Claude to give each multi-article cluster a short headline name.
    If week_names is provided, Claude is asked to reuse a matching name
    from earlier in the week when the cluster is the same ongoing story.
    Returns {cluster_id: name}.
    """
    lines = []
    for c in clusters:
        cid = c["cluster_id"]
        articles = articles_by_cluster.get(cid, [])
        lines.append(f"cluster_id: {cid}")
        for a in articles:
            lines.append(f"  - {a.get('title', '')}")
        lines.append("")

    if week_names:
        hint_lines = [
            "Story names already used earlier this week — reuse one EXACTLY if the cluster is clearly the same ongoing story:",
            *[f"  - {n}" for n in week_names],
            "",
        ]
    else:
        hint_lines = []

    prompt = "\n".join([
        "Name each of the following news story clusters with a short headline (3–7 words, punchy, no filler).",
        "",
        'Return JSON only — an array: [{"cluster_id": "...", "name": "..."}]',
        "",
        *hint_lines,
        *lines,
    ])

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    try:
        msg = client.messages.create(
            model=MODEL,
            max_tokens=2048,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = msg.content[0].text.strip()
        raw = raw.removeprefix("```json").removeprefix("```").removesuffix("```").strip()
        data = json.loads(raw)
        return {item["cluster_id"]: item["name"] for item in data if item.get("cluster_id") and item.get("name")}
    except Exception as exc:
        logger.warning("Cluster naming failed: %s", exc)
        return {}


def _apply_names(names: dict[str, str], week_name_map: dict[str, str]) -> None:
    """Write cluster names and weekly_story back to the DB."""
    supabase = get_client()
    for cluster_id, name in names.items():
        canonical = name.strip().lower()
        weekly_story = canonical if canonical in week_name_map else None
        supabase.table(CLUSTERS_TABLE).update({
            "name": name,
            "weekly_story": weekly_story,
        }).eq("cluster_id", cluster_id).execute()

        # Backfill weekly_story on earlier clusters that share this name
        # but don't have it set yet (i.e. the first day the story appeared)
        if weekly_story:
            supabase.table(CLUSTERS_TABLE)\
                .update({"weekly_story": weekly_story})\
                .eq("name", week_name_map[canonical])\
                .is_("weekly_story", "null")\
                .execute()

    logger.info("Named %d clusters", len(names))


# ── Step 2: Singleton assignment ──────────────────────────────────────────────

SINGLETON_BATCH_SIZE = 60   # singletons per Claude call for assignment


def _assign_singletons_batch(
    batch: list[dict],
    named_clusters: list[dict],
    articles_by_cluster: dict[str, list],
    client: anthropic.Anthropic,
) -> dict[str, str | None]:
    """Process one batch of singletons against all named clusters."""
    cluster_lines = [
        f'- cluster_id: {c["cluster_id"]} | name: {c.get("name") or c.get("anchor_title") or ""}'
        for c in named_clusters
    ]
    singleton_lines = []
    for c in batch:
        cid = c["cluster_id"]
        articles = articles_by_cluster.get(cid, [])
        article = articles[0] if articles else {}
        title = article.get("title", "")
        summary = (article.get("summary") or "").strip()
        singleton_lines.append(f'- singleton_cluster_id: {cid} | "{title}": {summary}')

    prompt = "\n".join([
        "You have these named story clusters from today's financial news:",
        "",
        *cluster_lines,
        "",
        "And these individual articles that did not automatically cluster:",
        "",
        *singleton_lines,
        "",
        "For each individual article, decide if it thematically belongs to one of the named clusters.",
        "Think editorially — does this article add meaningfully to that story from a reader's perspective?",
        "Base this on theme and editorial angle, not just keyword overlap.",
        "If it does not clearly fit any cluster, return null.",
        "",
        'Return JSON only — an array: [{"singleton_cluster_id": "...", "target_cluster_id": "... or null"}]',
    ])

    try:
        msg = client.messages.create(
            model=MODEL,
            max_tokens=4096,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = msg.content[0].text.strip()
        raw = raw.removeprefix("```json").removeprefix("```").removesuffix("```").strip()
        data = json.loads(raw)
        return {
            item["singleton_cluster_id"]: item.get("target_cluster_id") or None
            for item in data
            if item.get("singleton_cluster_id")
        }
    except Exception as exc:
        logger.warning("Singleton assignment batch failed: %s", exc)
        return {}


def _assign_singletons(
    singletons: list[dict],
    named_clusters: list[dict],
    articles_by_cluster: dict[str, list],
) -> dict[str, str | None]:
    """
    Ask Claude whether each singleton article thematically belongs to one of
    the named clusters. Processes in batches to stay within token limits.
    Returns {singleton_cluster_id: target_cluster_id | None}.
    """
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    results: dict[str, str | None] = {}

    batches = [singletons[i: i + SINGLETON_BATCH_SIZE] for i in range(0, len(singletons), SINGLETON_BATCH_SIZE)]
    logger.info("Singleton assignment: %d singletons in %d batch(es)", len(singletons), len(batches))

    for idx, batch in enumerate(batches):
        logger.info("Singleton assignment batch %d/%d (%d items)", idx + 1, len(batches), len(batch))
        batch_results = _assign_singletons_batch(batch, named_clusters, articles_by_cluster, client)
        results.update(batch_results)

    return results


def _apply_singleton_assignments(
    assignments: dict[str, str | None],
    singletons: list[dict],
) -> list[dict]:
    """
    Merge assigned singletons into their target clusters in the DB.
    Returns the list of singletons that were NOT assigned (still need roundup pass).
    """
    supabase = get_client()
    assigned_ids = set()

    for singleton_cluster_id, target_cluster_id in assignments.items():
        if not target_cluster_id:
            continue

        # Re-link the article to the target cluster
        supabase.table(TABLE).update(
            {"cluster_id": target_cluster_id}
        ).eq("cluster_id", singleton_cluster_id).execute()

        # Increment target cluster article count
        # (fetch current count first, then update)
        resp = (
            supabase.table(CLUSTERS_TABLE)
            .select("article_count")
            .eq("cluster_id", target_cluster_id)
            .single()
            .execute()
        )
        current_count = (resp.data or {}).get("article_count") or 0
        supabase.table(CLUSTERS_TABLE).update(
            {"article_count": current_count + 1}
        ).eq("cluster_id", target_cluster_id).execute()

        # Remove the now-absorbed singleton cluster row
        supabase.table(CLUSTERS_TABLE).delete().eq("cluster_id", singleton_cluster_id).execute()

        assigned_ids.add(singleton_cluster_id)
        logger.info("Assigned singleton %s → cluster %s", singleton_cluster_id, target_cluster_id)

    remaining = [s for s in singletons if s["cluster_id"] not in assigned_ids]
    logger.info(
        "Singleton assignment: %d assigned, %d remaining",
        len(assigned_ids), len(remaining),
    )
    return remaining


# ── Step 3: Roundup grouping ──────────────────────────────────────────────────

ROUNDUP_BATCH_SIZE = 80   # singletons per Claude call for roundup grouping


ROUNDUP_BATCH_SIZE = 80   # singletons per Claude call for roundup grouping


def _group_singletons_into_roundups(
    singletons: list[dict],
    articles_by_cluster: dict[str, list],
    roundup_prompt: str,
    run_date: str,
) -> int:
    """
    Send remaining singletons to Claude using the custom_cluster_prompt.
    Groups them into roundup clusters (auto type). Merges articles and removes
    source singleton cluster rows. Processes in batches to stay within token limits.
    Returns the number of roundup clusters created.
    """
    if len(singletons) < 2:
        logger.info("Roundup: fewer than 2 singletons — skipping")
        return 0

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    supabase = get_client()
    clusters_by_id = {c["cluster_id"]: c for c in singletons}
    total_created = 0

    batches = [singletons[i: i + ROUNDUP_BATCH_SIZE] for i in range(0, len(singletons), ROUNDUP_BATCH_SIZE)]
    logger.info("Roundup grouping: %d singletons in %d batch(es)", len(singletons), len(batches))

    for batch_idx, batch in enumerate(batches):
        lines = []
        for c in batch:
            cid = c["cluster_id"]
            articles = articles_by_cluster.get(cid, [])
            article = articles[0] if articles else {}
            lines.append(f"cluster_id: {cid}")
            lines.append(f"title: {article.get('title', '')}")
            lines.append(f"summary: {(article.get('summary') or '').strip()}")
            lines.append("")

        try:
            msg = client.messages.create(
                model=MODEL,
                max_tokens=4096,
                system=roundup_prompt,
                messages=[{"role": "user", "content": "\n".join(lines)}],
            )
            raw = msg.content[0].text.strip()
            raw = raw.removeprefix("```json").removeprefix("```").removesuffix("```").strip()
            groupings = json.loads(raw)
            if not isinstance(groupings, list):
                raise ValueError(f"Expected list, got {type(groupings)}")
        except Exception as exc:
            logger.warning("Roundup batch %d/%d failed: %s", batch_idx + 1, len(batches), exc)
            continue

        for group in groupings:
            name = (group.get("name") or "").strip()
            source_ids = group.get("cluster_ids", [])
            valid = [cid for cid in source_ids if cid in clusters_by_id]

            if not name or len(valid) < 2:
                logger.warning("Skipping malformed roundup group: %s", group)
                continue

            new_cluster_id = str(uuid.uuid4())
            anchor_article_id = clusters_by_id[valid[0]].get("anchor_article_id")

            supabase.table(CLUSTERS_TABLE).insert({
                "cluster_id":        new_cluster_id,
                "date":              run_date,
                "name":              name,
                "anchor_article_id": anchor_article_id,
                "article_count":     len(valid),
                "cluster_status":    "pending",
                "source_cluster_ids": valid,
            }).execute()

            # Re-link articles and remove absorbed singleton rows
            supabase.table(TABLE).update({"cluster_id": new_cluster_id}).in_("cluster_id", valid).execute()
            supabase.table(CLUSTERS_TABLE).delete().in_("cluster_id", valid).execute()

            total_created += 1
            logger.info("Created roundup cluster '%s' from %d singletons", name, len(valid))

    return total_created


# ── Entry point ───────────────────────────────────────────────────────────────

def run_hybrid_clustering(run_date: str | None = None) -> None:
    """
    Hybrid clustering pass. Runs after Voyage clustering, before scoring.

      1. Names all multi-article clusters (Claude)
      2. Assigns singletons to named clusters by theme (Claude)
      3. Groups remaining singletons into roundup clusters (custom_cluster_prompt)
    """
    target_date = run_date or (date.today() - timedelta(days=1)).isoformat()
    logger.info("Hybrid clustering started for %s", target_date)

    settings = get_pipeline_settings()
    roundup_prompt = (settings.get("custom_cluster_prompt") or "").strip()

    # Fetch names used earlier this week so Claude can reuse them
    supabase = get_client()
    monday = _get_monday(target_date)
    resp = supabase.table(CLUSTERS_TABLE)\
        .select("name")\
        .gte("date", monday)\
        .lt("date", target_date)\
        .not_.is_("name", "null")\
        .execute()
    week_name_map: dict[str, str] = {}
    for r in (resp.data or []):
        if r.get("name"):
            key = r["name"].strip().lower()
            week_name_map[key] = r["name"]
    logger.info("Found %d existing story names from earlier this week", len(week_name_map))

    multi_clusters, singleton_clusters = _fetch_pending_clusters(target_date)
    logger.info(
        "Found %d multi-article clusters, %d singletons",
        len(multi_clusters), len(singleton_clusters),
    )

    if not multi_clusters and not singleton_clusters:
        logger.info("Hybrid clustering: nothing to process")
        return

    # Fetch all articles up front
    all_cluster_ids = [c["cluster_id"] for c in multi_clusters + singleton_clusters]
    articles_by_cluster = _fetch_articles_for_clusters(all_cluster_ids)

    # ── Step 1: Name multi-article clusters ───────────────────────────────────
    if multi_clusters:
        names = _name_clusters(multi_clusters, articles_by_cluster, list(week_name_map.values()))
        _apply_names(names, week_name_map)
        # Patch in-memory so step 2 has the names
        for c in multi_clusters:
            if c["cluster_id"] in names:
                c["name"] = names[c["cluster_id"]]

    # ── Step 2: Assign singletons to named clusters ───────────────────────────
    remaining_singletons = singleton_clusters
    if singleton_clusters and multi_clusters:
        assignments = _assign_singletons(singleton_clusters, multi_clusters, articles_by_cluster)
        remaining_singletons = _apply_singleton_assignments(assignments, singleton_clusters)
    else:
        logger.info("Skipping singleton assignment — no multi-article clusters to assign to")

    # ── Step 3: Group remaining singletons into roundups ──────────────────────
    if roundup_prompt and remaining_singletons:
        created = _group_singletons_into_roundups(
            remaining_singletons, articles_by_cluster, roundup_prompt, target_date
        )
        logger.info("Roundup pass created %d clusters", created)
    elif not roundup_prompt:
        logger.info("Roundup pass skipped — no custom_cluster_prompt configured")

    logger.info("Hybrid clustering complete")
