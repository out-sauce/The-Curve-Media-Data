"""
Browser scraper — renders article pages in a real Chromium tab (Playwright) and
extracts clean text from the *rendered* HTML.

Unlike the static httpx scraper (scraper.py), this navigates the page in a real
browser seeded with a per-domain logged-in Playwright `storage_state`, so paywalled,
JS-rendered, consent-walled and anti-bot publishers render their full body exactly as
a subscriber sees it. Once rendered, extraction is deterministic — the same
trafilatura pass the static scraper uses — so there is no LLM in this path. The
deep-summary LLM call lives downstream in research.py, unchanged.

Shares the ScrapeResult dataclass and MIN_WORD_COUNT contract with scraper.py so both
engines are interchangeable at the call site. Never raises — all errors are returned
as ScrapeResult(status="failed"), mirroring scrape_article.

Runs Chromium locally inside the container (Dockerfile installs it), or connects to a
hosted Chromium over CDP when BROWSER_CDP_URL is set (no other code change).
"""

import asyncio
import logging

import trafilatura

from config import (
    BROWSER_CDP_URL,
    BROWSER_PAGE_TIMEOUT,
    BROWSERBASE_API_KEY,
    BROWSERBASE_PROJECT_ID,
    RESEARCH_USE_BROWSERBASE,
)
from .scraper import MIN_WORD_COUNT, ScrapeResult

logger = logging.getLogger(__name__)

# Container-suitable Chromium flags — Railway/Docker have no sandbox namespace and
# limited /dev/shm; these keep a single tab stable and memory-gentle.
_LAUNCH_ARGS = [
    "--no-sandbox",
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--disable-blink-features=AutomationControlled",
]

_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


def _extract(html: str) -> ScrapeResult:
    """Run rendered HTML through trafilatura and apply the paywall/word-count check."""
    text = trafilatura.extract(
        html,
        include_comments=False,
        include_tables=False,
        no_fallback=False,
    )
    if not text or len(text.split()) < MIN_WORD_COUNT:
        return ScrapeResult(
            status="paywalled",
            full_text=None,
            word_count=0,
            error="Content below minimum threshold — likely paywalled or login wall",
        )
    return ScrapeResult(
        status="scraped",
        full_text=text,
        word_count=len(text.split()),
        error=None,
    )


def _browserbase_connect_url() -> str | None:
    """
    Create a Browserbase session (UK region/proxy) and return its CDP connect URL, for
    the env-toggled Browserbase read path. Returns None (caller falls back) if the SDK
    or credentials are missing — keeps the never-raise contract.
    """
    if not (BROWSERBASE_API_KEY and BROWSERBASE_PROJECT_ID):
        return None
    try:
        from browserbase import Browserbase
        bb = Browserbase(api_key=BROWSERBASE_API_KEY)
        session = bb.sessions.create(
            project_id=BROWSERBASE_PROJECT_ID,
            region="eu-west-2",
            proxies=[{"type": "browserbase", "geolocation": {"country": "GB"}}],
        )
        return session.connect_url
    except Exception as exc:
        logger.warning("Browserbase read-path session unavailable, using local Chromium: %s", exc)
        return None


async def _scrape(url: str, storage_state: dict | None) -> ScrapeResult:
    from playwright.async_api import async_playwright

    async with async_playwright() as pw:
        # Read-path engine selection (in priority order):
        #   RESEARCH_USE_BROWSERBASE → managed UK-IP browser over CDP (opt-in);
        #   BROWSER_CDP_URL          → generic hosted Chromium over CDP;
        #   else                     → local headless Chromium (default).
        bb_url = _browserbase_connect_url() if RESEARCH_USE_BROWSERBASE else None
        if bb_url:
            browser = await pw.chromium.connect_over_cdp(bb_url)
            owns_browser = True
        elif BROWSER_CDP_URL:
            browser = await pw.chromium.connect_over_cdp(BROWSER_CDP_URL)
            owns_browser = True
        else:
            browser = await pw.chromium.launch(headless=True, args=_LAUNCH_ARGS)
            owns_browser = True

        context = None
        try:
            context = await browser.new_context(
                storage_state=storage_state,
                user_agent=_USER_AGENT,
                locale="en-GB",
            )
            context.set_default_navigation_timeout(BROWSER_PAGE_TIMEOUT)
            context.set_default_timeout(BROWSER_PAGE_TIMEOUT)

            page = await context.new_page()
            await page.goto(url, wait_until="domcontentloaded")
            # Let late-loading paywall/article scripts settle; tolerate a missing idle.
            try:
                await page.wait_for_load_state(
                    "networkidle", timeout=BROWSER_PAGE_TIMEOUT
                )
            except Exception:
                pass

            html = await page.content()
            return _extract(html)
        finally:
            if context is not None:
                try:
                    await context.close()
                except Exception:
                    pass
            if owns_browser:
                try:
                    await browser.close()
                except Exception:
                    pass


def scrape_article_with_browser(
    url: str, storage_state: dict | None = None
) -> ScrapeResult:
    """
    Render `url` in a headless Chromium tab and extract its main article text.

    storage_state: Playwright storage_state() JSON for the article's registrable
    domain (cookies + per-origin localStorage), or None when no auth is stored.
    Never raises — all errors are returned as ScrapeResult(status="failed"), matching
    scrape_article's contract so the two engines are interchangeable.
    """
    try:
        return asyncio.run(_scrape(url, storage_state))
    except Exception as exc:
        return ScrapeResult(
            status="failed",
            full_text=None,
            word_count=0,
            error=str(exc)[:200],
        )
