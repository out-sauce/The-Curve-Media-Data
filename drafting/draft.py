"""
DM reply drafting — suggestions for the operator, never sends.

WHAT THIS IS FOR. Reply latency here is bimodal, not slow: the median reply to an
incoming DM is 0.3 hours, but p90 is 247.8 hours — ten days. Most messages are answered
almost immediately and a long tail rots, which is why so many real replies in the corpus
open with "sorry it's taken us a while to come back to you". So this stage deliberately
does NOT try to draft everything. It targets threads that have already gone unanswered
past DRAFT_MIN_AGE_HOURS, which is the only part that is actually broken.

NO FINE-TUNING, AND NO RETRIEVAL. The house voice is taught by example, in the prompt.
The whole usable corpus is ~111 (their message -> our reply) pairs, roughly 10k tokens —
it fits in a cached prefix whole, so there is no vector index, no embedding pipeline and
no nearest-neighbour step to maintain. Revisit that only if the corpus outgrows the
prefix; at current volume that is years away.

THE DRAFT IS DISPOSABLE. The operator copies it into the composer, edits, sends — which
creates a normal source='admin' message row through the Admin, exactly as today. Nothing
here is ever edited in place, so drafts are overwritten freely and regenerating is always
safe. This module never sends anything: outbound belongs to the Admin app, which calls
Zernio directly (see ingestion/inbox.py's note on the same split).

THE GATE IS NOT ENFORCED YET, ON PURPOSE. `category` is classified and written but
nothing acts on it. About 24% of substantive incoming DMs touch the fund or investing,
and The Curve Investments is a licensed fund with a PDS — replies in the corpus make
concrete claims like "there is a new PDS coming into play on 1st August". That is
regulated territory, and an operator reading a confident AI draft about it is a real
risk even with a human pressing send. Writing the classification now means switching
enforcement on later is a config change, not a re-run over the whole inbox.

Never raises — failures log and skip, matching the rest of the pipeline.
"""

import json
import logging
import re

import anthropic

from config import (
    ANTHROPIC_API_KEY,
    DRAFT_MODEL,
    DRAFT_MIN_AGE_HOURS,
    DRAFT_MAX_THREADS,
    DRAFT_EXEMPLAR_LIMIT,
    DRAFT_EXEMPLAR_MIN_REPLY_LEN,
    DRAFT_EXEMPLAR_MIN_INCOMING_LEN,
    DRAFT_THREAD_CONTEXT_MESSAGES,
    DRAFT_MAX_TOKENS,
)
from ingestion.storage import (
    get_draft_exemplars,
    get_thread_messages,
    get_threads_needing_drafts,
    log_source_run,
    update_conversation_draft,
)

logger = logging.getLogger(__name__)

_RUN_CATEGORY = "inbox_draft"

# Categories are the gate's vocabulary, chosen from what the corpus actually contains.
# `fund_or_investing` and `complaint` are the two that will be refused once the gate is
# switched on; they are classified now so that switch costs nothing.
CATEGORIES = (
    "praise_or_thanks",
    "podcast_or_content_question",
    "money_question_general",
    "logistics_or_partnership",
    "fund_or_investing",
    "complaint",
    "other",
)

DRAFT_SCHEMA = {
    "type": "object",
    "properties": {
        "category": {"type": "string", "enum": list(CATEGORIES)},
        "draft": {"type": "string"},
    },
    "required": ["category", "draft"],
    "additionalProperties": False,
}

_VOICE = """You draft Instagram DM replies for The Curve, a New Zealand money and \
investing platform for women, founded by Sophie and Vic. You are writing a SUGGESTION \
for a human operator, who will read it, edit it and send it themselves.

VOICE IS THE POINT. A draft that sounds like customer service is useless to the \
operator even when every word is defensible — they will bin it and start again. Sound \
like Sophie and Vic texting a friend who happens to have asked about money.

Write like the examples:
- Genuinely, unguardedly warm. "Omg", "ahh", "yesss", strings of exclamation marks and \
a few emoji are ON-BRAND here, not excess. Match their energy — if they are excited, be \
more excited.
- Greet them by first name. Thank them for writing, and mean it.
- MATCH THEIR LENGTH. The team's real replies are usually short — two or three \
sentences. If your draft is twice as long as the examples, cut it. Length reads as \
corporate; brevity reads as a friend.
- Ask something back when there is a natural question to ask. Their replies often end \
by keeping the conversation going, and a draft that closes it down loses the thing that \
makes their DMs work.
- Answer the question directly when you can. Deferring is for when you genuinely do not \
know — it is not a default, and a hedge in front of every sentence is worse than a \
plain answer.
- No disclaimers, no "just to be clear", no corporate softening. If you would not text \
it to a friend, do not write it.
- New Zealand English. Sign off the way the examples do.
- If the thread has gone quiet a while, own it briefly and warmly — then get on with it.

Four hard limits. Everything else is style, and style should win:
- NEVER give financial advice, recommend an investment, or predict returns. This one is \
absolute — it is a licensed fund and this is not a place to be casual.
- NEVER state a fact that is not visible in this conversation — no dates, prices, fund \
terms or product details.
- NEVER write an email address, URL or phone number. Say it in words ("send that \
through to our team email?") and the operator will fill it in — that is why the \
examples show <email>. Better still, keep it in the DM where you can.
- NEVER invent a specific commitment — no "we'll cover this on an upcoming episode", no \
"we'll email you tomorrow" — unless that promise is already in this conversation. \
Offering warmly to pass something on is fine; inventing something the team then has to \
honour is not.

Write ONLY the message body. No preamble, no surrounding quotation marks, no notes to \
the operator. If you genuinely cannot write anything useful, return an empty draft."""


_EMAIL_RE = re.compile(r"[\w.+-]+@[\w-]+\.[\w.]+")


def _redact(text: str) -> str:
    """
    Strip email addresses out of an exemplar before it reaches the prompt.

    TWO separate reasons, both found by backtesting rather than by reasoning:

    1. PRIVACY. 79 distinct member email addresses appear in incoming DMs — the comment
       funnel exists to collect them. Feeding those to the model puts one member's
       personal address one plausible-completion away from being drafted into a
       different member's thread. Nothing downstream would catch it, because the address
       is real.
    2. ROUTING. The team's own replies use several addresses across two domains
       (hello@, sophie@, dan@, …). A model picking one out of the corpus is making a
       routing decision it has no basis for, and it looks authoritative when it is a
       guess.

    Redacting costs nothing: an exemplar teaches VOICE, and the voice of "drop us a line
    at <email>" survives the substitution intact. The operator fills in the real address
    when they copy the draft — which is exactly the division of labour this whole stage
    is built on.
    """
    return _EMAIL_RE.sub("<email>", text or "")


def _exemplar_block(pairs: list[dict]) -> str:
    """Render the corpus as labelled examples. Stable ordering — this is cached."""
    out = []
    for i, pair in enumerate(pairs, 1):
        out.append(
            f"<example {i}>\n"
            f"THEM: {_redact(pair['incoming']).strip()}\n"
            f"US:   {_redact(pair['ours']).strip()}\n"
            f"</example {i}>"
        )
    return "\n\n".join(out)


def _thread_block(messages: list[dict], name: str | None) -> str:
    who = name or "them"
    lines = []
    for message in messages:
        body = (message.get("body") or "").strip()
        speaker = "US" if message.get("direction") == "outgoing" else who.upper()
        # An attachment-only message still matters as context — it is usually a story
        # reply, and dropping it silently would make a reply read like a non-sequitur.
        lines.append(f"{speaker}: {body}" if body else f"{speaker}: [sent an attachment]")
    return "\n".join(lines)


def _draft_one(client: anthropic.Anthropic, system: list[dict],
               thread: dict) -> tuple[str, str] | None:
    """
    Draft a reply for one thread. Returns (category, draft) or None.

    One call per thread rather than the batched shape the scoring/tagging stages use.
    Batching would put several people's private threads in one request and one long
    draft could truncate the rest; with the exemplar corpus cached, the per-thread cost
    is small enough that the isolation is worth more than the saving.
    """
    messages = get_thread_messages(thread["id"], DRAFT_THREAD_CONTEXT_MESSAGES)
    if not messages:
        return None
    name = (thread.get("participant_name") or "").split()[:1]
    first_name = name[0] if name else None

    prompt = (
        f"Conversation with {thread.get('participant_name') or 'someone'}"
        f"{' (a follower)' if thread.get('ig_is_follower') else ''}, "
        f"oldest message first:\n\n"
        f"{_thread_block(messages, first_name)}\n\n"
        "Their last message is the one to answer. Classify it, then write the reply."
    )

    try:
        message = client.messages.create(
            model=DRAFT_MODEL,
            max_tokens=DRAFT_MAX_TOKENS,
            system=system,
            messages=[{"role": "user", "content": prompt}],
            output_config={
                "format": {"type": "json_schema", "schema": DRAFT_SCHEMA},
                "effort": "medium",
            },
        )
    except Exception as exc:
        logger.warning("Draft API error for %s: %s", thread["id"], str(exc)[:200])
        return None

    # The standing gotcha for every per-item Claude stage in this repo: a truncated or
    # refused response must fail loudly, never be half-parsed.
    if message.stop_reason == "refusal":
        logger.warning("Draft refused for thread %s", thread["id"])
        return None
    if message.stop_reason == "max_tokens":
        logger.warning(
            "Draft truncated at max_tokens=%d for thread %s", DRAFT_MAX_TOKENS, thread["id"],
        )
        return None

    try:
        raw = next(b.text for b in message.content if b.type == "text")
        data = json.loads(raw)
    except (StopIteration, json.JSONDecodeError, TypeError) as exc:
        logger.warning("Could not parse draft for %s: %s", thread["id"], str(exc)[:200])
        return None

    category = str(data.get("category") or "other")
    draft = str(data.get("draft") or "").strip()
    if category not in CATEGORIES:
        category = "other"
    return category, draft


def run_inbox_drafts(limit: int | None = None, min_age_hours: int | None = None) -> None:
    """
    Draft replies for neglected DM threads. Never raises.

    Skips a thread whose draft already answers its newest message, so a scheduled run
    over an unchanged inbox costs one listing query and nothing else.
    """
    name = "The Curve (inbox drafts)"
    drafted = 0
    try:
        if not ANTHROPIC_API_KEY:
            logger.warning("Inbox drafting skipped — ANTHROPIC_API_KEY not set")
            log_source_run(name, _RUN_CATEGORY, "error", 0, "ANTHROPIC_API_KEY not set")
            return

        threads = get_threads_needing_drafts(
            min_age_hours if min_age_hours is not None else DRAFT_MIN_AGE_HOURS,
            limit if limit is not None else DRAFT_MAX_THREADS,
        )
        if not threads:
            logger.info("Inbox drafting: nothing to draft")
            log_source_run(name, _RUN_CATEGORY, "ok", 0)
            return

        exemplars = get_draft_exemplars(
            DRAFT_EXEMPLAR_LIMIT,
            DRAFT_EXEMPLAR_MIN_REPLY_LEN,
            DRAFT_EXEMPLAR_MIN_INCOMING_LEN,
        )
        if not exemplars:
            logger.warning("Inbox drafting skipped — no exemplar pairs available")
            log_source_run(name, _RUN_CATEGORY, "error", 0, "no exemplars")
            return

        # Voice rules then examples, with the cache breakpoint at the end of the block.
        # Everything that varies per thread is in the user message, AFTER this prefix,
        # so the prefix stays byte-identical across every call in the run and each one
        # after the first is a cache read.
        system = [
            {"type": "text", "text": _VOICE},
            {
                "type": "text",
                "text": (
                    "Here is how the team has replied to real messages. Match this "
                    "voice.\n\n" + _exemplar_block(exemplars)
                ),
                "cache_control": {"type": "ephemeral"},
            },
        ]

        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        logger.info(
            "Inbox drafting: %d threads, %d exemplars", len(threads), len(exemplars),
        )
        for thread in threads:
            result = _draft_one(client, system, thread)
            if not result:
                continue
            category, draft = result
            if not draft:
                # An empty draft is a real answer — the model declined to write one.
                # Stamp it anyway so the thread is not retried every run.
                logger.info("No usable draft for thread %s (%s)", thread["id"], category)
            if update_conversation_draft(
                thread["id"], draft or None, thread["last_message_id"], category,
            ):
                drafted += 1

        logger.info("Inbox drafting: wrote %d drafts", drafted)
        log_source_run(name, _RUN_CATEGORY, "ok", drafted)
    except Exception as exc:
        logger.warning("Inbox drafting failed: %s", str(exc)[:300])
        log_source_run(name, _RUN_CATEGORY, "error", drafted, str(exc)[:500])
