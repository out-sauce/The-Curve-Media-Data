"""
Stage 5 — The pitch.

Drafts the pitch for each researched story cluster that actually produced a deep
summary: the story's name, the pitch itself (what the story is and why The Curve's
audience would care, as prose) and The Curve angles beside it. Until 28 Oct 2026 this
stage wrote a summary into story_clusters.brief; the pitch replaced it (the Admin app's
story page shows and edits the pitch, and sends it to an Admin).

The instructions are the Pitch Agent's prompt (agents, slug "pitch"), edited in the
Admin app's Settings → Agents and also used by its "Draft pitch" button
(lib/story-pitch.ts); the Audience and Tone-of-Voice docs follow it. The model, the
name rule and the answer's shape stay here. Keep the two in step.

A pitch a person has saved (story_clusters.pitch_edited_at set) is never redrafted.
One that is still the pipeline's draft is redrafted when the story gains articles.

Input:
  - The story's description, key facts and team notes, where it has them
  - Its articles, deep summaries first (the research stage's)

Output:
  - name + pitch + pitch_angles (HTML) written to story_clusters
  - briefed_at timestamp set
  - cluster_status is left at 'researched' (advancing the cluster is a
    downstream / manual decision)
"""

import html
import json
import logging
import re
from datetime import datetime, timezone
from typing import Any

import anthropic

from config import ANTHROPIC_API_KEY
from ingestion.storage import get_client, get_pipeline_settings, TABLE

logger = logging.getLogger(__name__)

CLUSTERS_TABLE = "story_clusters"
PITCH_MODEL = "claude-opus-5-5"
PITCH_AGENT_SLUG = "pitch"
MAX_ARTICLES = 10

# The prompt the Pitch Agent was seeded with (the Admin app's lib/story-pitch.ts
# FALLBACK_PROMPT) — used only if its agents row is missing or emptied.
FALLBACK_PROMPT = """You write the pitch for a news story, for the editorial team of The Curve, a personal-finance media company (a podcast, social channels and a newsletter). A researcher sends the pitch to the editor, who decides which stories the team develops.

Write two things.

The pitch: two short paragraphs of plain prose, no bullet points, headings or labels.
1. What the story is: the news in two or three sentences, with the figures that matter.
2. Why The Curve should cover it: why its audience would find it interesting, in their terms.

The angles: three to five ways The Curve could tell it, one sentence each, each a distinct take (a reveal, a how-to, a relationship angle, a contrarian view…).

British spelling. Plain and specific: no hype, no rhetorical questions, no exclamation marks. Use only what the sources say; never invent a figure."""

# The story's name comes from this stage too (the Admin app's button keeps the name).
NAME_RULE = "Also name the story for the team: 4–7 words, direct, no source names, no clickbait."

PITCH_SCHEMA = {
    "type": "object",
    "properties": {
        "name": {"type": "string"},
        "pitch": {"type": "array", "items": {"type": "string"}},
        "angles": {"type": "array", "items": {"type": "string"}},
    },
    "required": ["name", "pitch", "angles"],
    "additionalProperties": False,
}

# Past research, the pitch belongs to the people working the story.
REDRAFTABLE_STATUSES = ("pending", "scored", "researched")


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

STORY_COLUMNS = "cluster_id, cluster_status, pitch, pitch_edited_at, briefed_at, last_article_at, description, key_facts, research"


def _fetch_briefable_clusters(run_date: str) -> list[dict[str, Any]]:
    client = get_client()
    response = (
        client.table(CLUSTERS_TABLE)
        .select(STORY_COLUMNS)
        .eq("cluster_status", "researched")
        .eq("date", run_date)
        .execute()
    )
    return response.data or []


def _brief_is_current(cluster: dict[str, Any]) -> bool:
    """
    True when the cluster's pitch already covers everything in it.

    The clustering stage stamps last_article_at every time a cluster is created or
    EXTENDED with a later day's coverage (migration 031), so comparing it against
    briefed_at is what lets an extended story be re-pitched while an unchanged one keeps
    the idempotent re-run skip. Legacy rows (last_article_at NULL, pre-031) and any
    unparseable timestamp are treated as current — i.e. the pre-031 skip behaviour.
    """
    last = cluster.get("last_article_at")
    briefed = cluster.get("briefed_at")
    if not last:
        return True
    if not briefed:
        return False
    try:
        return (
            datetime.fromisoformat(str(briefed).replace("Z", "+00:00"))
            >= datetime.fromisoformat(str(last).replace("Z", "+00:00"))
        )
    except Exception:
        return True


def _fetch_cluster_articles(cluster_id: str) -> list[dict[str, Any]]:
    client = get_client()
    response = (
        client.table(TABLE)
        .select("id, guid, title, summary, source_name, deep_summary, published_at")
        .eq("cluster_id", cluster_id)
        .execute()
    )
    return response.data or []


def _pitch_prompt() -> str:
    """The Pitch Agent's prompt, or the seed if its row is missing or emptied."""
    try:
        rows = (
            get_client().table("agents").select("prompt").eq("slug", PITCH_AGENT_SLUG).limit(1).execute().data
        ) or []
        prompt = (rows[0].get("prompt") or "").strip() if rows else ""
        return prompt or FALLBACK_PROMPT
    except Exception as exc:
        logger.warning("Pitch: could not read the Pitch Agent's prompt (%s) — using the seed", exc)
        return FALLBACK_PROMPT


def _system_prompt(settings: dict[str, Any]) -> str:
    parts = [_pitch_prompt()]
    audience = (settings.get("audience_doc") or "").strip()
    tov = (settings.get("tov_doc") or "").strip()
    if audience:
        parts.append(f"# The Curve's audience\n{audience}")
    if tov:
        parts.append(f"# The Curve's tone of voice\n{tov}")
    return "\n\n".join(parts)


# ---------------------------------------------------------------------------
# The pitch
# ---------------------------------------------------------------------------

def _plain(s: Any) -> str:
    text = re.sub(r"<[^>]*>", " ", str(s or "")).replace("&nbsp;", " ")
    return re.sub(r"\s+", " ", html.unescape(text)).strip()


def _facts(raw: Any) -> list[str]:
    """A story's key facts as plain strings (the Admin app's factsOf)."""
    if isinstance(raw, list):
        out = []
        for f in raw:
            if isinstance(f, dict):
                f = f.get("fact") or f.get("label") or f.get("text") or ""
            text = str(f).strip()
            if text:
                out.append(text)
        return out
    if isinstance(raw, str) and raw.strip():
        return [raw.strip()]
    return []


def _build_prompt(cluster: dict[str, Any], articles: list[dict[str, Any]]) -> str:
    parts: list[str] = []
    if _plain(cluster.get("description")):
        parts.append(_plain(cluster.get("description")))
    facts = _facts(cluster.get("key_facts"))
    if facts:
        parts.append("## Key facts\n" + "\n".join(f"- {f}" for f in facts))
    notes = [_plain(r.get("text")) for r in (cluster.get("research") or []) if isinstance(r, dict)]
    notes = [n for n in notes if n]
    if notes:
        parts.append("## Notes from the team\n" + "\n\n".join(notes)[:6000])

    # Researched articles first, then the newest.
    ordered = sorted(articles, key=lambda a: str(a.get("published_at") or ""), reverse=True)
    ordered = sorted(ordered, key=lambda a: not (a.get("deep_summary") or "").strip())
    for a in ordered[:MAX_ARTICLES]:
        body = _plain(a.get("deep_summary")) or _plain(a.get("summary"))
        parts.append(f"## {a.get('source_name') or 'Source'}: {a.get('title') or ''}\n{body[:2500]}")

    parts.append(NAME_RULE)
    return "\n\n".join(parts)


def _to_html(paragraphs: list[str], angles: list[str]) -> tuple[str, str]:
    pitch = "".join(f"<p>{html.escape(p, quote=False)}</p>" for p in paragraphs)
    bullets = "".join(f"<li><p>{html.escape(a, quote=False)}</p></li>" for a in angles)
    return pitch, (f"<ul>{bullets}</ul>" if bullets else "")


def _generate_pitch(cluster: dict[str, Any], articles: list[dict[str, Any]], system: str) -> tuple[str, str, str] | None:
    """
    Call Claude for the story's name, pitch and angles.
    Returns (name, pitch_html, angles_html) or None on failure.
    """
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    try:
        message = client.messages.create(
            model=PITCH_MODEL,
            max_tokens=4000,
            system=system,
            messages=[{"role": "user", "content": _build_prompt(cluster, articles)}],
            output_config={"effort": "medium", "format": {"type": "json_schema", "schema": PITCH_SCHEMA}},
        )
        if message.stop_reason in ("refusal", "max_tokens"):
            raise ValueError(f"stop: {message.stop_reason}")
        raw = "".join(b.text for b in message.content if b.type == "text")
        data = json.loads(raw)
        name = str(data.get("name") or "").strip()
        paragraphs = [str(p).strip() for p in data.get("pitch") or [] if str(p).strip()]
        angles = [str(a).strip() for a in data.get("angles") or [] if str(a).strip()]
        if not name or not paragraphs:
            raise ValueError("Missing name or pitch in response")
        pitch_html, angles_html = _to_html(paragraphs, angles)
        return name, pitch_html, angles_html
    except Exception as exc:
        logger.warning("Pitch: cluster %s failed: %s", cluster.get("cluster_id"), exc)
        return None


def _write_pitch(cluster_id: str, name: str, pitch: str, angles: str) -> bool:
    """Write the draft — only while no person has saved a pitch (pitch_edited_at null)."""
    res = (
        get_client().table(CLUSTERS_TABLE)
        .update({
            "name": name,
            "pitch": pitch,
            "pitch_angles": angles or None,
            "briefed_at": datetime.now(timezone.utc).isoformat(),
        })
        .eq("cluster_id", cluster_id)
        .is_("pitch_edited_at", "null")
        .execute()
    )
    return bool(res.data)


def rebrief_cluster(cluster_id: str) -> bool:
    """
    Redraft the name + pitch for a single cluster, overwriting the pipeline's own
    draft. This is the explicit "redo" used after on-demand research (the Admin
    story-level Research button, or a late-arriving extension import) — unlike
    run_briefing it ignores date and does not skip clusters that already have a
    pitch. Never for a pitch a person has saved, nor a story past research (in
    development, pitched, passed…): those belong to the team. Still requires at
    least one article with a deep summary. Returns True when a pitch was written.
    Never raises.
    """
    try:
        rows = (
            get_client().table(CLUSTERS_TABLE)
            .select(STORY_COLUMNS)
            .eq("cluster_id", cluster_id)
            .limit(1)
            .execute()
            .data
        ) or []
        if not rows:
            return False
        cluster = rows[0]
        if cluster.get("pitch_edited_at") or cluster.get("cluster_status") not in REDRAFTABLE_STATUSES:
            logger.info("Re-pitch: cluster %s is the team's now — skipping", cluster_id)
            return False

        articles = _fetch_cluster_articles(cluster_id)
        if not any((a.get("deep_summary") or "").strip() for a in articles):
            logger.info("Re-pitch: cluster %s has no deep summaries — skipping", cluster_id)
            return False

        result = _generate_pitch(cluster, articles, _system_prompt(get_pipeline_settings()))
        if result is None:
            return False

        name, pitch, angles = result
        written = _write_pitch(cluster_id, name, pitch, angles)
        logger.info("Re-pitch: cluster %s %s", cluster_id, "redrafted" if written else "edited meanwhile — left alone")
        return written
    except Exception as exc:
        logger.warning("Re-pitch: cluster %s failed: %s", cluster_id, exc)
        return False


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_briefing(run_date: str | None = None) -> None:
    """
    Stage 5, the pitch. Run after research.

    For each researched cluster that has at least one article with a deep
    summary:
      1. Skip clusters whose pitch a person has saved
      2. Skip clusters whose draft is already current — i.e. they have a pitch and
         have not gained articles since it was written (idempotent re-runs; an
         extended story is redrafted)
      3. Skip the cluster if no article has a non-empty deep_summary
      4. Draft the pitch via Claude (the Pitch Agent's prompt as the system prompt)
      5. Write name + pitch + pitch_angles + briefed_at to story_clusters (status unchanged)
    """
    from datetime import date, timedelta
    target_date = run_date or (date.today() - timedelta(days=1)).isoformat()
    logger.info("Pitch drafting started for %s", target_date)

    clusters = _fetch_briefable_clusters(target_date)
    if not clusters:
        logger.info("Pitch: no researched clusters to process")
        return

    system = _system_prompt(get_pipeline_settings())
    logger.info("Drafting pitches for up to %d clusters", len(clusters))

    drafted = 0
    failed = 0
    skipped = 0

    for cluster in clusters:
        cluster_id = cluster["cluster_id"]

        if cluster.get("pitch_edited_at"):
            skipped += 1
            continue

        # Status stays 'researched' after drafting, so a re-run would otherwise
        # redraft clusters that already have one. Skip those — unless the story has
        # gained articles since the pitch was written (continuation, migration 031),
        # in which case the draft no longer covers the whole story.
        if (cluster.get("pitch") or "").strip() and _brief_is_current(cluster):
            skipped += 1
            continue

        articles = _fetch_cluster_articles(cluster_id)
        if not articles:
            logger.warning("Cluster %s has no articles — skipping", cluster_id)
            continue

        # Only pitch stories the research stage actually summarised: require at
        # least one article with a non-empty deep_summary. Clusters marked
        # 'researched' but with no deep summary (all scrapes failed/paywalled,
        # or Claude failed) are left untouched for a later re-run.
        if not any((a.get("deep_summary") or "").strip() for a in articles):
            skipped += 1
            logger.info("Cluster %s has no deep summary — skipping pitch", cluster_id)
            continue

        result = _generate_pitch(cluster, articles, system)
        if result is None:
            failed += 1
            continue

        name, pitch, angles = result
        # Leave cluster_status at 'researched' — advancing a cluster past research
        # is the team's decision (the pitch is sent, then approved), not this stage's.
        try:
            if _write_pitch(cluster_id, name, pitch, angles):
                drafted += 1
            else:
                skipped += 1
        except Exception as exc:
            failed += 1
            logger.warning("Cluster %s — pitch write failed: %s", cluster_id, exc)

    logger.info(
        "Pitch drafting complete — %d drafted, %d skipped, %d failed",
        drafted, skipped, failed,
    )
