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
    RESEARCH_PROXY_PASSWORD,
    RESEARCH_PROXY_SERVER,
    RESEARCH_PROXY_USERNAME,
    RESEARCH_USE_BROWSERBASE,
)
from .scraper import ScrapeResult, classify_text

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


def _proxy_config() -> dict | None:
    """Playwright proxy dict for the local-Chromium egress, or None for a direct
    connection. Lets the scraper render from a UK (or any) IP via any proxy you
    control — no managed-browser vendor required. Auth fields are only attached when
    set (Chromium ignores user/pass for SOCKS)."""
    if not RESEARCH_PROXY_SERVER:
        return None
    proxy: dict = {"server": RESEARCH_PROXY_SERVER}
    if RESEARCH_PROXY_USERNAME:
        proxy["username"] = RESEARCH_PROXY_USERNAME
    if RESEARCH_PROXY_PASSWORD:
        proxy["password"] = RESEARCH_PROXY_PASSWORD
    return proxy


# Substrings that distinguish *why* a page yielded no article, matched case-insensitively
# against the rendered HTML. Bot walls vs. paywalls vs. JS shells need different fixes.
_DIAGNOSTIC_MARKERS = (
    "unusual activity",
    "are you a robot",
    "press & hold",
    "press and hold",
    "verify you are human",
    "captcha",
    "px-captcha",
    "access to this page has been denied",
    "subscribe to continue",
    "sign in to continue",
    "become a subscriber",
    "enable javascript",
)


async def _log_non_article(page, url: str, html: str, status: str) -> None:
    """Log the page title, final URL, size and any known challenge/paywall markers when a
    render produced no article. Diagnostic only — wrapped so it never perturbs the scrape."""
    try:
        title = await page.title()
    except Exception:
        title = ""
    lowered = html.lower()
    markers = [m for m in _DIAGNOSTIC_MARKERS if m in lowered]
    logger.info(
        "Non-article render: status=%s url=%s final_url=%s title=%r html_len=%d markers=%s",
        status, url, getattr(page, "url", url), title, len(html), markers,
    )


def _extract(html: str) -> ScrapeResult:
    """Run rendered HTML through trafilatura and apply the paywall/word-count check."""
    text = trafilatura.extract(
        html,
        include_comments=False,
        include_tables=False,
        no_fallback=False,
    )
    return classify_text(text)


def _browserbase_connect_url() -> str | None:
    """
    Create a Browserbase session (EU region, UK proxy) and return its CDP connect URL, for
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
            # Browserbase only runs in us-west-2 / us-east-1 / eu-central-1 /
            # ap-southeast-1. eu-central-1 (Frankfurt) is the nearest to the UK;
            # the UK *IP* is delivered by the GB proxy below, not the region.
            region="eu-central-1",
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
            # Local Chromium. A proxy set here (RESEARCH_PROXY_SERVER) gives the render a
            # UK/other IP; it must be applied at launch for Chromium to honour it.
            browser = await pw.chromium.launch(
                headless=True, args=_LAUNCH_ARGS, proxy=_proxy_config()
            )
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
            result = _extract(html)
            if result.status != "scraped":
                # No article extracted — log what the page actually was so we can tell a
                # bot wall from a real paywall from a blank JS shell (the raw HTML is not
                # persisted anywhere). Best-effort; never affects the returned result.
                await _log_non_article(page, url, html, result.status)
            return result
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
