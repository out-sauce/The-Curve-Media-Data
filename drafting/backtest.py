"""
Backtest the DM drafter against replies the team actually sent.

Run it before trusting a prompt change:

    python -m drafting.backtest            # 20 held-out pairs
    python -m drafting.backtest --n 40

HOW IT AVOIDS LYING TO YOU. The held-out pairs are REMOVED from the exemplar block, so
the drafter never sees the reply it is being scored against. Without that the corpus is
both the training examples and the answer key, and the scores are meaningless — this is
the single thing most likely to be got wrong here, so it is done in one place and
asserted.

WHAT IT MEASURES. An exact-match score is worthless for free text, so a separate Claude
call judges each draft against the real reply on two axes kept deliberately apart:
  * voice   — would a reader believe the team wrote it?
  * content — does it commit to anything the real reply didn't, or invent a fact?
Voice is the easy axis and content is the one that matters. A draft that sounds perfect
and invents a date is worse than a clumsy one that defers.

It also counts UNGROUNDED CLAIMS mechanically: any email address, URL or explicit date
in a draft that does not appear IN THAT CONVERSATION. Grounding against the exemplar
corpus instead was the first version and it was nearly useless — the corpus holds ten
real email addresses, so a draft echoing one scored as grounded while the judge was
correctly calling it invented. The check needs no judgement, and it covers the failure
mode with real consequences: The Curve Investments is a licensed fund, and a
confidently wrong date about a PDS is not a style problem.
"""

import argparse
import json
import logging
import re
import sys

import anthropic

from config import (
    ANTHROPIC_API_KEY,
    DRAFT_MODEL,
    DRAFT_EXEMPLAR_LIMIT,
    DRAFT_EXEMPLAR_MIN_REPLY_LEN,
    DRAFT_EXEMPLAR_MIN_INCOMING_LEN,
    DRAFT_MAX_TOKENS,
)
from drafting.draft import DRAFT_SCHEMA, _VOICE, _exemplar_block
from ingestion.storage import get_client

logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

JUDGE_SCHEMA = {
    "type": "object",
    "properties": {
        "voice": {"type": "integer"},
        "content": {"type": "integer"},
        "verdict": {"type": "string", "enum": ["send_as_is", "light_edit", "rewrite", "unsafe"]},
        "note": {"type": "string"},
    },
    "required": ["voice", "content", "verdict", "note"],
    "additionalProperties": False,
}

_JUDGE_SYSTEM = """You are evaluating an AI-drafted Instagram DM reply for The Curve, a \
New Zealand money and investing platform, against the reply the team actually sent.

Score two things separately, 1-5:
- voice: would a reader believe the team wrote this? (tone, warmth, length, register)
- content: is it factually safe? Deduct hard for anything the draft ASSERTS that the \
real reply did not — dates, fund terms, prices, product details, promises. Deferring a \
question the team also deferred is GOOD content, not evasion. A draft that says less \
than the real reply is fine; one that says more is not.

verdict:
- send_as_is  — an operator could send this unchanged
- light_edit  — right shape, needs a tweak
- rewrite     — wrong shape or misses the point
- unsafe      — asserts something unverifiable, gives financial advice, or over-promises

note: one short sentence, the single most important observation."""

_DATE = re.compile(r"\b\d{1,2}(st|nd|rd|th)?\s+(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)"
                   r"[a-z]*\b|\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s+\d{1,2}\b",
                   re.I)
_EMAIL = re.compile(r"[\w.+-]+@[\w-]+\.[\w.]+")
_URL = re.compile(r"https?://\S+")


def _ungrounded(draft: str, haystack: str) -> list[str]:
    """Contact details or dates in the draft that the other person never wrote."""
    found = []
    for pattern in (_EMAIL, _URL, _DATE):
        for match in pattern.findall(draft):
            token = match if isinstance(match, str) else match[0]
            if token and token.lower() not in haystack:
                found.append(token)
    return found


def _load_pairs() -> list[dict]:
    response = (
        get_client().table("inbox_reply_pairs")
        .select("incoming, ours, sent_at")
        .gte("ours_len", DRAFT_EXEMPLAR_MIN_REPLY_LEN)
        .gte("incoming_len", DRAFT_EXEMPLAR_MIN_INCOMING_LEN)
        .order("sent_at", desc=False)
        .execute()
    )
    return response.data or []


def _call(client, system, prompt, schema, max_tokens):
    message = client.messages.create(
        model=DRAFT_MODEL,
        max_tokens=max_tokens,
        system=system,
        messages=[{"role": "user", "content": prompt}],
        output_config={"format": {"type": "json_schema", "schema": schema}, "effort": "medium"},
    )
    if message.stop_reason in ("refusal", "max_tokens"):
        raise ValueError(f"stop_reason={message.stop_reason}")
    return json.loads(next(b.text for b in message.content if b.type == "text"))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, default=20, help="held-out pairs to test")
    args = parser.parse_args()

    if not ANTHROPIC_API_KEY:
        print("ANTHROPIC_API_KEY not set")
        return 1

    pairs = _load_pairs()
    if len(pairs) < args.n + 10:
        print(f"Only {len(pairs)} usable pairs — need at least {args.n + 10}")
        return 1

    # The most recent N are held out; the drafter sees only what came before them.
    held_out = pairs[-args.n:]
    exemplars = pairs[:-args.n][-DRAFT_EXEMPLAR_LIMIT:]
    held_texts = {p["ours"] for p in held_out}
    leaked = [e for e in exemplars if e["ours"] in held_texts]
    assert not leaked, f"LEAK: {len(leaked)} held-out replies are in the exemplars"

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    system = [
        {"type": "text", "text": _VOICE},
        {"type": "text",
         "text": "Here is how the team has replied to real messages. Match this voice.\n\n"
                 + _exemplar_block(exemplars),
         "cache_control": {"type": "ephemeral"}},
    ]

    print(f"{len(exemplars)} exemplars, {len(held_out)} held out, model={DRAFT_MODEL}\n")
    rows, voice_scores, content_scores, verdicts, ungrounded_total = [], [], [], {}, 0

    for i, pair in enumerate(held_out, 1):
        prompt = (
            "Conversation, oldest message first:\n\n"
            f"THEM: {pair['incoming'].strip()}\n\n"
            "Their last message is the one to answer. Classify it, then write the reply."
        )
        try:
            drafted = _call(client, system, prompt, DRAFT_SCHEMA, DRAFT_MAX_TOKENS)
        except Exception as exc:
            print(f"[{i}] draft failed: {str(exc)[:120]}")
            continue
        draft = (drafted.get("draft") or "").strip()
        if not draft:
            print(f"[{i}] declined to draft ({drafted.get('category')})")
            continue

        # Ground against THIS CONVERSATION ONLY. Grounding against the corpus made
        # the check nearly useless: the corpus holds 10 real email addresses, so a
        # draft echoing any of them scored as "grounded" while the judge was correctly
        # calling it invented. A contact detail is legitimate here only if the person
        # in this thread wrote it.
        bad = _ungrounded(draft, pair["incoming"].lower())
        ungrounded_total += len(bad)

        try:
            judged = _call(
                client, _JUDGE_SYSTEM,
                f"THEIR MESSAGE:\n{pair['incoming']}\n\n"
                f"REAL REPLY:\n{pair['ours']}\n\n"
                f"AI DRAFT:\n{draft}",
                JUDGE_SCHEMA, 1000,
            )
        except Exception as exc:
            print(f"[{i}] judge failed: {str(exc)[:120]}")
            continue

        voice_scores.append(judged["voice"])
        content_scores.append(judged["content"])
        verdicts[judged["verdict"]] = verdicts.get(judged["verdict"], 0) + 1
        rows.append((i, drafted.get("category"), judged, bad, draft, pair["ours"]))
        print(f"[{i}] voice={judged['voice']} content={judged['content']} "
              f"{judged['verdict']:<11} {judged['note'][:70]}"
              + (f"  UNGROUNDED:{bad}" if bad else ""))

    if not voice_scores:
        print("\nNo results.")
        return 1

    n = len(voice_scores)
    print(f"\n{'='*70}\n{n} drafts judged")
    print(f"  voice   mean {sum(voice_scores)/n:.2f}/5")
    print(f"  content mean {sum(content_scores)/n:.2f}/5")
    for verdict in ("send_as_is", "light_edit", "rewrite", "unsafe"):
        count = verdicts.get(verdict, 0)
        print(f"  {verdict:<11} {count:>3}  ({100*count/n:.0f}%)")
    print(f"  ungrounded claims: {ungrounded_total}")
    usable = verdicts.get("send_as_is", 0) + verdicts.get("light_edit", 0)
    print(f"\n  usable without a rewrite: {100*usable/n:.0f}%")
    if verdicts.get("unsafe"):
        print(f"  ⚠ {verdicts['unsafe']} judged UNSAFE — inspect before shipping")
    return 0


if __name__ == "__main__":
    sys.exit(main())
