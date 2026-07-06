"""
Scraper — fetches full article HTML and extracts clean text.

Uses httpx for HTTP and trafilatura for main-content extraction.
Attaches per-source cookie string when available.
"""

import logging
from dataclasses import dataclass
from typing import Optional

import httpx
import trafilatura

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.5",
}

SCRAPE_TIMEOUT = 20
MIN_WORD_COUNT = 150  # below this = paywalled stub or login wall

# Word-count alone doesn't catch a paywall: publishers routinely render a teaser
# plus nav/subscribe/related-articles chrome that clears MIN_WORD_COUNT, so the stub
# gets stored as "scraped" and the deep summary ends up describing the wall itself.
# These phrases are high-precision paywall/login-wall markers — strings that appear
# in the *wall*, not in genuine article prose. Matched case-insensitively against the
# extracted text; any hit within a short body (see PAYWALL_MAX_WORDS) => paywalled.
PAYWALL_PHRASES = (
    "subscribe to continue",
    "subscribe to read",
    "to continue reading",
    "continue reading this article",
    "to read the full article",
    "read the full story",
    "already a subscriber",
    "already have an account",
    "sign in to read",
    "sign in to continue",
    "register to continue",
    "create a free account",
    "start your free trial",
    "become a subscriber",
    "this article is for subscribers",
    "this content is for subscribers",
    "for subscribers only",
    "subscribers only",
    "you have reached your",
    "you've reached your",
    "unlock this article",
    "unlock full access",
    "behind a paywall",
    "support our journalism",
)

# A genuine long article can mention "subscribe" in passing; a paywall stub is short.
# Only treat a phrase hit as a wall when the whole extraction is below this length.
PAYWALL_MAX_WORDS = 500


@dataclass
class ScrapeResult:
    status: str              # "scraped" | "paywalled" | "failed"
    full_text: Optional[str]
    word_count: int
    error: Optional[str]


def _looks_paywalled(text: str, word_count: int) -> bool:
    """
    True when the extracted text is a paywall/login wall rather than an article.

    A short body (< PAYWALL_MAX_WORDS) that contains any high-precision paywall
    marker phrase is treated as a wall even though it cleared MIN_WORD_COUNT.
    """
    if word_count >= PAYWALL_MAX_WORDS:
        return False
    lowered = text.lower()
    return any(phrase in lowered for phrase in PAYWALL_PHRASES)


def classify_text(text: Optional[str]) -> ScrapeResult:
    """
    Turn extracted article text into a ScrapeResult, applying both the word-count
    gate and the paywall-phrase check. Shared by the static and browser scrapers so
    the paywall contract cannot drift between the two engines.
    """
    if not text or len(text.split()) < MIN_WORD_COUNT:
        return ScrapeResult(
            status="paywalled",
            full_text=None,
            word_count=0,
            error="Content below minimum threshold — likely paywalled or login wall",
        )

    word_count = len(text.split())
    if _looks_paywalled(text, word_count):
        return ScrapeResult(
            status="paywalled",
            full_text=None,
            word_count=0,
            error="Extracted text matches a paywall/login-wall marker",
        )

    return ScrapeResult(
        status="scraped",
        full_text=text,
        word_count=word_count,
        error=None,
    )


def scrape_article(url: str, cookie_string: str | None = None) -> ScrapeResult:
    """
    Fetch url and extract main article text.
    cookie_string: raw Cookie header value from sources.cookies.
    Never raises — all errors returned as ScrapeResult(status="failed").
    """
    try:
        headers = {**HEADERS}
        if cookie_string:
            headers["Cookie"] = cookie_string

        with httpx.Client(
            headers=headers,
            timeout=SCRAPE_TIMEOUT,
            follow_redirects=True,
        ) as client:
            resp = client.get(url)
            resp.raise_for_status()
            html = resp.text

        text = trafilatura.extract(
            html,
            include_comments=False,
            include_tables=False,
            no_fallback=False,
        )

        return classify_text(text)

    except httpx.HTTPStatusError as exc:
        return ScrapeResult(
            status="failed",
            full_text=None,
            word_count=0,
            error=f"HTTP {exc.response.status_code}",
        )
    except Exception as exc:
        return ScrapeResult(
            status="failed",
            full_text=None,
            word_count=0,
            error=str(exc)[:200],
        )
