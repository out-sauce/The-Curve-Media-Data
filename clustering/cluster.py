"""
Clustering — two Claude calls, one DB write.

Call 1 (continuation): append today's articles to stories that already exist. Unlike the
  old "week continuity" pass, which minted a brand-new same-named row per day, this one
  extends the existing story_clusters row in place: the articles are repointed at it, its
  article_count is recounted, its `date` is bumped forward to the run date (so it
  resurfaces in the Admin day view and every downstream stage picks it up again) and its
  status is reset to 'pending' so score → tag → research → brief all re-run over it.
Call 2 (new stories): group the remaining articles into new named clusters.
Unplaced articles become singletons.

A cluster's `date` is therefore NOT immutable and its articles are NOT all from that date
— see migration 031.
"""

import json
import logging
import uuid
from datetime import date, datetime, timedelta, timezone

import anthropic

from config import ANTHROPIC_API_KEY
from ingestion.storage import get_client, get_pipeline_settings, TABLE

logger = logging.getLogger(__name__)

CLUSTERS_TABLE = "story_clusters"
MODEL = "claude-sonnet-4-6"

# The old free-text JSON responses truncated at max_tokens once the day grew past
# ~500 articles ("Unterminated string" parse errors in the 2026-07-22/23 runs), and
# every article silently fell through to a singleton. Both calls now use structured
# outputs (guaranteed-parseable JSON), a much larger max_tokens, and an explicit
# stop_reason check so truncation is a loud failure instead of a silent one.
CONTINUITY_MAX_TOKENS = 8192
NEW_CLUSTER_MAX_TOKENS = 16384

# Candidate window for the continuation pass. Rolling, not Monday-to-today: a story
# running since last Thursday is just as live on Tuesday as one that started this week.
LOOKBACK_DAYS = 7            # multi-article running stories
SINGLETON_LOOKBACK_DAYS = 3  # 1-article stories: only recent ones. Singletons are named
                             # after their article's headline, and a whole week of those
                             # snowballed the old prompt to ~1000 "week stories" in three
                             # days (2026-07-21→23).

# Candidates are gated and ranked by relevance_score, not by recency. The window holds
# ~480 multi-article clusters and ~700 singletons, so a date-ordered cap would spend the
# whole budget on the most recent day and the 7-day window would be fiction. Scoring runs
# before this ever sees a prior day's cluster, so every candidate has a score (a failed
# score writes 0.0, which this correctly excludes).
CANDIDATE_MIN_SCORE = 0.5

# Prompt caps. Multi-article stories get their own budget so a genuine running story can
# never be crowded out by singletons; within each budget the lowest-scoring candidates are
# dropped first, so trimming costs the least relevant stories rather than the oldest ones.
MAX_CANDIDATE_MULTI = 150
MAX_CANDIDATE_SINGLE = 50
MAX_CANDIDATE_STORIES = 200

# A cluster in one of these states is finished editorial output — new coverage forms a
# fresh story rather than reopening it. 'briefed' is the manual content-studio state and
# its brief column holds hand-written output; 'ready_for_content' has been promoted into
# production. These are the live cluster_status enum labels — the full set is
# (pending, scored, researched, ready_for_content, archived, briefed). Passing a label
# that is not in the enum makes PostgREST reject the whole query, so do not add
# 'published'/'accepted'/'rejected' here: migration 021's header lists them but the type
# was since recreated without them.
CLOSED_STATUSES = ("briefed", "archived", "ready_for_content")
OPEN_STATUSES = ["pending", "scored", "researched"]

# Articles per continuation call. The article list is cheap on input, but the response
# scales with how many of them match, so chunking keeps it clear of max_tokens.
CONTINUITY_ARTICLE_BATCH = 400

# PostgREST caps a response at 1000 rows by default.
PAGE_SIZE = 1000

# Structured-outputs schema for the new-clustering pass: a list of named article groups.
_GROUPS_SCHEMA = {
    "type": "object",
    "properties": {
        "clusters": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "description": {"type": "string"},
                    "article_ids": {"type": "array", "items": {"type": "integer"}},
                },
                "required": ["name", "description", "article_ids"],
                "additionalProperties": False,
            },
        },
    },
    "required": ["clusters"],
    "additionalProperties": False,
}

# The continuation pass has a different contract from the new-clustering pass: it returns
# a pointer to an EXISTING story, not a new name+description. Reusing _GROUPS_SCHEMA would
# force Claude to emit a name and description we then throw away — wasted output tokens,
# and an open invitation to silently rename a running story.
_CONTINUATION_SCHEMA = {
    "type": "object",
    "properties": {
        "continuations": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "story_index": {"type": "integer"},
                    "article_ids": {"type": "array", "items": {"type": "integer"}},
                },
                "required": ["story_index", "article_ids"],
                "additionalProperties": False,
            },
        },
    },
    "required": ["continuations"],
    "additionalProperties": False,
}


def _call_claude_json(system_prompt: str | None, user_prompt: str, max_tokens: int,
                      label: str, schema: dict, key: str) -> list[dict]:
    """
    Shared Claude call for both clustering passes. Returns the parsed list under `key`,
    or raises on refusal/truncation (caller catches).

    The stop_reason checks are the whole point of this being one function: the old
    free-text responses truncated silently at max_tokens and every article fell through
    to a singleton (2026-07-22/23). Both passes must keep them.
    """
    ai = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    kwargs = {"system": system_prompt} if system_prompt else {}
    msg = ai.messages.create(
        model=MODEL,
        max_tokens=max_tokens,
        messages=[{"role": "user", "content": user_prompt}],
        # Structured outputs: the API constrains the response to this schema, so
        # the JSON is guaranteed parseable — unless it truncates, which we check.
        output_config={
            "format": {
                "type": "json_schema",
                "schema": schema,
            }
        },
        **kwargs,
    )
    if msg.stop_reason == "refusal":
        raise ValueError(f"{label} request was refused")
    if msg.stop_reason == "max_tokens":
        raise ValueError(f"{label} response truncated at max_tokens={max_tokens}")
    raw = next(b.text for b in msg.content if b.type == "text")
    return json.loads(raw)[key]


def _call_claude_groups(system_prompt: str | None, user_prompt: str,
                        max_tokens: int, label: str) -> list[dict]:
    """Name-groups call (new clustering pass)."""
    return _call_claude_json(system_prompt, user_prompt, max_tokens, label,
                             _GROUPS_SCHEMA, "clusters")


DEFAULT_CLUSTER_PROMPT = """You are an editorial assistant for Curve Media, clustering today's financial news articles into distinct stories.

Group articles that are reporting on the same underlying story or feeding into the same broader narrative. Think editorially — two articles belong together if a reader would expect to read them as part of the same story, even if the headlines look different.

Rules:
- Group articles covering the same story or closely related developments
- Cluster along broad themes where multiple articles clearly feed the same narrative
- Do NOT force groupings — if an article genuinely stands alone, leave it out
- Do NOT group articles just because they are in the same industry
- A cluster needs at least 2 articles
- Name each cluster with a short punchy headline (3–7 words, no filler)
- Description: one sentence (~10 words) summarising what the story is about

Return a JSON array of clusters only — empty array if nothing should be grouped.
Articles not included will be kept as individual stories.
Return a JSON array only: [{"name": "...", "description": "...", "article_ids": [...]}]"""


_CONTINUATION_PROMPT = """You are an editorial assistant for Curve Media.

Below are stories Curve is already tracking, each with a numeric index, followed by today's newly-filtered articles.

Your job: for each of today's articles that is a genuine CONTINUATION of a tracked story, assign it to that story's index.

A continuation means the article reports a new development in the SAME ongoing narrative — a follow-up, a reaction, a next step, a consequence, or a new data point in the same running event. It is NOT a continuation if the article merely shares a topic, a sector, a company or a country with the tracked story.

Be conservative. An article you leave out is grouped into a new story a moment later, which is a far cheaper mistake than folding a genuinely distinct story into an existing one — that one silently corrupts a story readers are already following. Expect most of today's articles NOT to be continuations.

A tracked story listed with 1 article is a thin story, not a weak one; grow it if today's coverage genuinely follows it.

Rules:
- Use only the story indexes listed below. Never invent an index.
- Use only the article ids listed below. Never invent an id.
- Assign each article to at most one story.
- Return one entry per story that gained articles. Omit stories that gained none.
- Return an empty list if nothing continues a tracked story.

TRACKED STORIES
{story_block}

TODAY'S ARTICLES
{article_lines}
"""


# ---------------------------------------------------------------------------
# DB reads
# ---------------------------------------------------------------------------

def _fetch_open_clusters(target_date: str) -> list[dict]:
    """
    Candidate stories the continuation pass may extend, best-first.

      - article_count >= 2   over the full LOOKBACK_DAYS window
      - article_count == 1   only from the last SINGLETON_LOOKBACK_DAYS
      - relevance_score >= CANDIDATE_MIN_SCORE — a story Curve is not tracking is not a
        story to continue, and this is what keeps the list small enough to rank by
        editorial weight instead of truncating it by date
      - cluster_status not in CLOSED_STATUSES, published_at unset, ready_for_content false
      - date <= target_date  so a backfill run can never see (or bump) a later story

    Two queries rather than one plus a Python filter: a few days of singletons can exceed
    PostgREST's 1000-row default cap, and a server-side order + limit on each keeps the
    cap from silently deciding which candidates we see.
    """
    target = date.fromisoformat(target_date)
    window_start = (target - timedelta(days=LOOKBACK_DAYS)).isoformat()
    singleton_start = (target - timedelta(days=SINGLETON_LOOKBACK_DAYS)).isoformat()
    supabase = get_client()
    cols = "cluster_id, name, description, article_count, date, cluster_status"

    def _query(start: str, exact_one: bool, limit: int) -> list[dict]:
        q = (
            supabase.table(CLUSTERS_TABLE)
            .select(cols)
            .gte("date", start)
            .lte("date", target_date)
            .not_.in_("cluster_status", list(CLOSED_STATUSES))
            # Belt and braces beyond the status filter: a cluster that has shipped or
            # been promoted into production must not be silently grown and re-briefed.
            # ready_for_content exists both as a status and as this boolean flag.
            .is_("published_at", "null")
            .eq("ready_for_content", False)
            .not_.is_("name", "null")
            .gte("relevance_score", CANDIDATE_MIN_SCORE)
        )
        q = q.eq("article_count", 1) if exact_one else q.gte("article_count", 2)
        resp = (
            q.order("relevance_score", desc=True)
            .order("article_count", desc=True)
            .order("date", desc=True)
            .order("cluster_id")  # deterministic tiebreak
            .limit(limit)
            .execute()
        )
        return resp.data or []

    multi = _query(window_start, False, MAX_CANDIDATE_MULTI)
    singles = _query(singleton_start, True, MAX_CANDIDATE_SINGLE)

    candidates = [
        c for c in (multi + singles)[:MAX_CANDIDATE_STORIES]
        if (c.get("name") or "").strip()
    ]
    if len(multi) + len(singles) > len(candidates):
        logger.info(
            "Continuation: %d candidate stories trimmed to %d",
            len(multi) + len(singles), len(candidates),
        )
    return candidates


def _fetch_included_articles(run_date: str) -> list[dict]:
    """
    Today's filtered-in articles that are not yet in a cluster.

    cluster_id IS NULL is what makes the stage re-runnable. Clustering never changes
    news_articles.status, so without it a second run for the same date re-clusters
    everything — and with continuation that means today's own clusters (date == run_date)
    appear as candidates and get re-extended, while the new-clustering pass mints
    duplicate rows whose articles are then stolen by the newest row, leaving zero-article
    ghost clusters with a stale article_count. With it, a second run finds nothing.

    Paginated: PostgREST caps a response at 1000 rows by default, so a big day was
    silently clustering only its first 1000 articles.
    """
    client = get_client()
    out: list[dict] = []
    page = 0
    while True:
        resp = (
            client.table(TABLE)
            .select("id, title, summary")
            .eq("status", "included")
            .is_("cluster_id", "null")
            .gte("fetched_at", f"{run_date}T00:00:00.000Z")
            .lte("fetched_at", f"{run_date}T23:59:59.999Z")
            .order("id")
            .range(page * PAGE_SIZE, page * PAGE_SIZE + PAGE_SIZE - 1)
            .execute()
        )
        rows = resp.data or []
        out.extend(rows)
        if len(rows) < PAGE_SIZE:
            return out
        page += 1


def _count_cluster_articles(supabase, cluster_id: str) -> int:
    """
    Authoritative article count for a cluster, straight from the DB.

    Not a read-then-increment (hybrid_clustering/hybrid_cluster.py:262-272 does that):
    that is non-atomic and permanently self-corrupting — if a re-link partly failed the
    stored count drifts and never recovers. A COUNT is idempotent, so a re-run converges
    on the truth instead of compounding. count="exact" reads the Content-Range total, so
    the 1000-row cap cannot under-count a large story.
    """
    resp = (
        supabase.table(TABLE)
        .select("id", count="exact")
        .eq("cluster_id", cluster_id)
        .limit(1)
        .execute()
    )
    return resp.count or 0


# ---------------------------------------------------------------------------
# Claude calls
# ---------------------------------------------------------------------------

def _build_candidate_block(candidates: list[dict]) -> str:
    lines = []
    for i, c in enumerate(candidates, 1):
        n = c.get("article_count") or 1
        line = (
            f'[{i}] ({n} article{"s" if n != 1 else ""}, last updated {c.get("date")}) '
            f'{(c.get("name") or "").strip()}'
        )
        description = (c.get("description") or "").strip()
        if description:
            line += f" — {description}"
        lines.append(line)
    return "\n".join(lines)


def _call_continuation(articles: list[dict], candidates: list[dict]) -> dict[int, list]:
    """
    Call 1: match today's articles against existing stories.
    Returns {candidate_index: [article_ids]} with indexes already range-checked.
    Only matches are returned — anything not mentioned is not continuing a tracked story.
    """
    story_block = _build_candidate_block(candidates)
    merged: dict[int, list] = {}

    for start in range(0, len(articles), CONTINUITY_ARTICLE_BATCH):
        batch = articles[start:start + CONTINUITY_ARTICLE_BATCH]
        article_lines = "\n".join(
            f'id: {a["id"]} | {a["title"]} — {(a.get("summary") or "").strip()}'
            for a in batch
        )
        prompt = _CONTINUATION_PROMPT.format(
            story_block=story_block, article_lines=article_lines
        )
        try:
            data = _call_claude_json(
                None, prompt, CONTINUITY_MAX_TOKENS, "Story continuation",
                _CONTINUATION_SCHEMA, "continuations",
            )
        except Exception as exc:
            # Degrade to "nothing continues" for this batch — those articles simply flow
            # into the new-clustering pass. Same failure posture as the old week pass.
            logger.warning("Continuation call failed (batch at %d): %s", start, exc)
            continue

        for item in data:
            try:
                idx = int(item.get("story_index"))
            except (TypeError, ValueError):
                logger.warning("Continuation: dropping non-integer story_index %r",
                               item.get("story_index"))
                continue
            ids = item.get("article_ids") or []
            if not ids:
                continue
            if 1 <= idx <= len(candidates):
                merged.setdefault(idx, []).extend(ids)  # same story twice → merge
            else:
                logger.warning("Continuation: dropping out-of-range story_index %d", idx)
    return merged


def _call_new_clustering(articles: list[dict], system_prompt: str) -> list[dict]:
    """
    Call 2: group remaining articles into new named clusters.
    Returns [{name, description, article_ids}] — only multi-article groups.
    Articles not mentioned become singletons.
    """
    article_lines = "\n".join(
        f'id: {a["id"]} | {a["title"]} — {(a.get("summary") or "").strip()}'
        for a in articles
    )

    try:
        return _call_claude_groups(
            system_prompt, article_lines, NEW_CLUSTER_MAX_TOKENS, "New clustering"
        )
    except Exception as exc:
        logger.warning("New clustering call failed: %s", exc)
        return []


# ---------------------------------------------------------------------------
# DB write
# ---------------------------------------------------------------------------

def _extend_cluster(supabase, candidate: dict, article_ids: list, target_date: str) -> int:
    """
    Append article_ids to an existing story: repoint the articles, recount, bump the
    cluster's date to the run date and reset it to 'pending' so score → tag → research →
    brief all re-process it.

    Returns the new article_count, or 0 if the cluster was not claimed (no longer open) —
    in which case NOTHING was written and the caller must let those articles fall through
    to the new-clustering pass.
    """
    cluster_id = candidate["cluster_id"]
    now_iso = datetime.now(timezone.utc).isoformat()

    # Never bump a story's date backwards. run_clustering defaults to *yesterday* while
    # the scheduler passes *today*, so a manual backfill (--date 2026-07-20) must not
    # yank a live story out of today's Admin view.
    new_date = max(candidate.get("date") or target_date, target_date)

    # Claim first, articles second. The status filter re-checks eligibility at write time
    # (the Claude call sits between the SELECT and here, so an operator could have briefed
    # or published the story in between), and claiming before we touch news_articles means
    # a refused claim leaves no half-moved articles behind.
    #
    # This is a deliberate DEMOTION (researched → pending) — the only one in the codebase.
    # research._advance_clusters_to_researched explicitly never demotes and must not start.
    claimed = (
        supabase.table(CLUSTERS_TABLE)
        .update({
            "date":            new_date,
            "cluster_status":  "pending",
            "last_article_at": now_iso,
            "weekly_story":    (candidate.get("name") or "").strip().lower() or None,
        })
        .eq("cluster_id", cluster_id)
        .in_("cluster_status", OPEN_STATUSES)
        .is_("published_at", "null")
        .eq("ready_for_content", False)
        .execute()
    )
    if not (claimed.data or []):
        logger.info("Continuation: cluster %s no longer open — skipping extension",
                    cluster_id)
        return 0

    supabase.table(TABLE).update({"cluster_id": cluster_id}).in_("id", article_ids).execute()
    count = _count_cluster_articles(supabase, cluster_id)
    supabase.table(CLUSTERS_TABLE).update({"article_count": count}) \
        .eq("cluster_id", cluster_id).execute()

    logger.info(
        "Extended story '%s' (%s): +%d article(s) → %d total, date %s → %s",
        candidate.get("name"), cluster_id, len(article_ids), count,
        candidate.get("date"), new_date,
    )
    return count


def run_clustering(run_date: str | None = None) -> None:
    target_date = run_date or (date.today() - timedelta(days=1)).isoformat()
    logger.info("Clustering started for %s", target_date)

    settings = get_pipeline_settings()
    cluster_prompt = (settings.get("custom_cluster_prompt") or "").strip() or DEFAULT_CLUSTER_PROMPT

    articles = _fetch_included_articles(target_date)
    if not articles:
        logger.info("Clustering: no unclustered included articles to process")
        return

    logger.info("Clustering %d articles", len(articles))
    # Claude may echo ids back as ints or strings — match on the string form and
    # map back to the DB-typed id.
    id_lookup = {str(a["id"]): a["id"] for a in articles}
    assigned_ids: set = set()
    supabase = get_client()

    # ── Call 1: extend existing stories ──────────────────────────────────────
    # Extensions are written before Call 2 runs, so articles whose target cluster failed
    # to claim can still be picked up as a new story below.
    candidates = _fetch_open_clusters(target_date)
    extended_articles = 0
    extended_stories = 0
    if candidates:
        logger.info("Continuation: %d articles against %d open stories",
                    len(articles), len(candidates))
        matches = _call_continuation(articles, candidates)
        # Sorted by candidate index, and candidates are relevance-ranked — so when Claude
        # assigns one article to two stories (it does, despite the prompt saying at most
        # one) the higher-scoring story wins, deterministically. Dropping the id_lookup
        # filter is not an option either: Claude also invents article ids that were never
        # in the prompt, and repointing an unrelated article would be silent corruption.
        for idx in sorted(matches):
            valid = list(dict.fromkeys(
                id_lookup[str(i)] for i in matches[idx]
                if str(i) in id_lookup and id_lookup[str(i)] not in assigned_ids
            ))
            dropped = len(matches[idx]) - len(valid)
            if dropped:
                logger.info("Continuation: story '%s' — dropped %d unknown/already-assigned id(s)",
                            candidates[idx - 1].get("name"), dropped)
            if not valid:
                continue
            if _extend_cluster(supabase, candidates[idx - 1], valid, target_date):
                assigned_ids.update(valid)
                extended_articles += len(valid)
                extended_stories += 1
        logger.info("Continuation: %d article(s) appended to %d existing stories",
                    extended_articles, extended_stories)
    else:
        logger.info("No open stories in the last %d days — skipping continuation pass",
                    LOOKBACK_DAYS)

    # ── Call 2: new clustering ───────────────────────────────────────────────
    remaining = [a for a in articles if a["id"] not in assigned_ids]
    new_clusters: list[tuple[str, list, str]] = []
    if remaining:
        logger.info("New clustering: grouping %d remaining articles", len(remaining))
        for group in _call_new_clustering(remaining, cluster_prompt):
            name = (group.get("name") or "").strip()
            description = (group.get("description") or "").strip()
            ids = list(dict.fromkeys(
                id_lookup[str(i)] for i in (group.get("article_ids") or [])
                if str(i) in id_lookup and id_lookup[str(i)] not in assigned_ids
            ))
            if name and len(ids) >= 2:
                new_clusters.append((name, ids, description))
                assigned_ids.update(ids)

    singletons = [a for a in articles if a["id"] not in assigned_ids]

    # ── DB write: brand-new clusters ─────────────────────────────────────────
    now_iso = datetime.now(timezone.utc).isoformat()

    for name, ids, description in new_clusters:
        cluster_id = str(uuid.uuid4())
        supabase.table(CLUSTERS_TABLE).insert({
            "cluster_id":      cluster_id,
            "date":            target_date,
            "name":            name,
            "description":     description or None,
            # Deprecated by migration 031 — written only so any Admin query that still
            # selects it keeps returning data. A running story is one row now.
            "weekly_story":    name.strip().lower(),
            "article_count":   len(ids),
            "cluster_status":  "pending",
            "last_article_at": now_iso,
        }).execute()
        supabase.table(TABLE).update({"cluster_id": cluster_id}).in_("id", ids).execute()

    for a in singletons:
        cluster_id = str(uuid.uuid4())
        supabase.table(CLUSTERS_TABLE).insert({
            "cluster_id":      cluster_id,
            "date":            target_date,
            "name":            a.get("title", ""),
            "description":     a.get("summary") or None,
            "article_count":   1,
            "cluster_status":  "pending",
            "last_article_at": now_iso,
        }).execute()
        supabase.table(TABLE).update({"cluster_id": cluster_id}).eq("id", a["id"]).execute()

    logger.info(
        "Clustering complete — %d articles: %d appended to %d existing stories, "
        "%d new clusters, %d singletons",
        len(articles), extended_articles, extended_stories,
        len(new_clusters), len(singletons),
    )
