"""
Site-auth capture — the WRITE half of the per-publisher login feature.

The research scraper (research/research.py + research/browser_scraper.py) is a
site_auth *reader*: it seeds Playwright with a stored storage_state to beat paywalls.
This module is the missing writer. It launches a headful, human-driven remote browser
(Browserbase) so an operator can complete a real publisher login, then captures that
logged-in session via Playwright `context.storage_state()` and upserts it into
site_auth — keyed by the lowercased registrable base domain (e.g. 'ft.com'), the exact
key the scraper reads back.

Contract consumed by the already-shipped Admin app (out-sauce__The-Curve-Media-Admin):
  POST {PIPELINE_URL}/site-auth/login/start?domain=<base>&label=<optional>
    (header x-api-key: PIPELINE_API_KEY)
  -> 200 { session_id, live_url }
The Admin modal opens live_url, then POLLS GET /api/site-auth-status?domain=<base>
every 3s and flips to "Auth captured" (auto-closing ~1.2s later) the moment
site_auth.captured_at advances. Its ONLY completion signal is our upsert — so the
capture MUST gate on a genuine logged-in state, never a stray first cookie, or the
modal closes mid-login.

Completion trigger (per the resolved decisions): a per-session asyncio task polls the
live context's cookies; when the publisher's named auth cookie(s) are present AND
stable across a debounce window it upserts ONCE. Publishers without an allowlist entry
(everything except FT today; BBC has no auth paywall) rely solely on the hard-timeout
final capture, so the modal stays open for the full session rather than closing early.

In-process session registry → assumes a SINGLE Railway replica (asserted at API
startup); the session created on one request must be found by its capture task in the
same process. Mirrors browser_scraper's never-raise discipline: a provider error is
logged and swallowed, never crashing the API.
"""

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Any

from config import (
    BROWSERBASE_API_KEY,
    BROWSERBASE_PROJECT_ID,
    SITE_AUTH_DEBOUNCE_SECONDS,
    SITE_AUTH_POLL_INTERVAL,
    SITE_AUTH_SESSION_TIMEOUT,
)
from ingestion.storage import get_client
from .domains import registrable_domain

logger = logging.getLogger(__name__)

SITE_AUTH_TABLE = "site_auth"

# Browserbase London region for a UK egress IP (paired with a UK proxy + geolocation),
# matching the scraper's en-GB locale and the UK/AU publishers in scope.
_BROWSERBASE_REGION = "eu-west-2"

# Per-publisher cookie allowlist: base domain -> the named auth cookie(s) whose
# presence (stable across the debounce window) means a genuine logged-in session.
# Gates the periodic-snapshot upsert so the Admin modal never closes mid-login.
#
# FT is the only entry at launch. 'FTSession_s' is the FT subscriber/session cookie;
# confirm the exact name empirically in the Browserbase debugger during build (FT also
# sets 'FTSession'). BBC is intentionally absent — it has no auth paywall, so it falls
# back to timeout final-capture only.
_AUTH_COOKIE_ALLOWLIST: dict[str, list[str]] = {
    "ft.com": ["FTSession_s", "FTSession"],
}

# In-process registry: session_id -> session metadata. Single-replica only.
_SESSIONS: dict[str, dict[str, Any]] = {}


class SiteAuthUnavailable(Exception):
    """Raised when Browserbase is not provisioned/configured — the endpoint maps this
    to a 404 so the Admin button shows its existing 'not yet available' message."""


# ---------------------------------------------------------------------------
# DB write
# ---------------------------------------------------------------------------

def upsert_site_auth(domain: str, storage_state: dict, label: str | None) -> None:
    """
    Upsert the captured session into site_auth keyed by the registrable base domain —
    the exact row write the Admin poll awaits. Complementary to research.py's
    _record_auth_usage (which only touches last_status/last_used_at). Best-effort:
    logs and swallows on failure so a provider/DB hiccup never crashes the API.
    """
    now = datetime.now(timezone.utc).isoformat()
    row = {
        "domain": domain,
        "storage_state": storage_state,
        "label": label,
        "captured_at": now,
        "last_status": "captured",
        "updated_at": now,
    }
    try:
        get_client().table(SITE_AUTH_TABLE).upsert(row, on_conflict="domain").execute()
        logger.info("site_auth captured for %s (label=%s)", domain, label)
    except Exception as exc:
        logger.warning("Could not upsert site_auth for %s: %s", domain, exc)


# ---------------------------------------------------------------------------
# Browserbase session lifecycle
# ---------------------------------------------------------------------------

def _browserbase_client():
    if not BROWSERBASE_API_KEY or not BROWSERBASE_PROJECT_ID:
        raise SiteAuthUnavailable("Browserbase is not configured (BROWSERBASE_API_KEY / BROWSERBASE_PROJECT_ID)")
    try:
        from browserbase import Browserbase
    except Exception as exc:  # SDK not installed
        raise SiteAuthUnavailable(f"browserbase SDK unavailable: {exc}")
    return Browserbase(api_key=BROWSERBASE_API_KEY)


def start_login(domain: str, label: str | None = None) -> dict[str, str]:
    """
    Create a headful Browserbase session for a human-driven login and return
    {session_id, live_url}. Normalises `domain` to the lowercased registrable base so
    the capture write keys on the same value the scraper reads. Raises
    SiteAuthUnavailable when Browserbase is not provisioned.

    Does NOT navigate or connect here — the Browserbase session ends when its CDP
    connection closes, so the long-lived connection (which also navigates to the
    domain) is owned by the capture task; start_login only provisions the session.
    """
    base = registrable_domain(domain)
    if not base:
        raise ValueError(f"Could not derive a registrable domain from {domain!r}")

    bb = _browserbase_client()

    # UK proxy + geolocation so the publisher sees a UK visitor, matching en-GB scrapes.
    session = bb.sessions.create(
        project_id=BROWSERBASE_PROJECT_ID,
        region=_BROWSERBASE_REGION,
        timeout=SITE_AUTH_SESSION_TIMEOUT,
        proxies=[{"type": "browserbase", "geolocation": {"country": "GB"}}],
    )

    # Fullscreen debugger URL is the human-drivable live view returned to Admin.
    debug = bb.sessions.debug(session.id)
    live_url = getattr(debug, "debugger_fullscreen_url", None) or getattr(debug, "debugger_url", "")

    _SESSIONS[session.id] = {
        "domain": base,
        "label": label,
        "connect_url": session.connect_url,
        "started_at": time.monotonic(),
        "last_captured_at": None,
        "captured": False,
        "force_capture": False,
        "done": False,
    }
    logger.info("Started site-auth login session %s for %s", session.id, base)
    return {"session_id": session.id, "live_url": live_url}


def _release_session(session_id: str) -> None:
    """Ask Browserbase to release the session. Best-effort."""
    try:
        bb = _browserbase_client()
        bb.sessions.update(session_id, project_id=BROWSERBASE_PROJECT_ID, status="REQUEST_RELEASE")
    except Exception as exc:
        logger.debug("Could not release Browserbase session %s: %s", session_id, exc)


def force_capture(session_id: str) -> bool:
    """
    Manual backstop (POST /site-auth/login/finish): flag the live capture task to take
    an immediate snapshot on its next poll. Returns False if the session is unknown
    (already finished/torn down). Admin never calls this; it ships per the decision.
    """
    meta = _SESSIONS.get(session_id)
    if not meta or meta.get("done"):
        return False
    meta["force_capture"] = True
    return True


# ---------------------------------------------------------------------------
# Capture task (long-lived CDP connection: navigate, poll, capture, teardown)
# ---------------------------------------------------------------------------

def _auth_cookies_present(cookies: list[dict], domain: str) -> bool:
    """True when at least one allowlisted auth cookie for `domain` is present with a
    non-empty value. Non-allowlisted domains return False (timeout-capture only)."""
    names = _AUTH_COOKIE_ALLOWLIST.get(domain)
    if not names:
        return False
    have = {c.get("name"): c.get("value") for c in cookies}
    return any(have.get(n) for n in names)


async def _capture_and_upsert(context, meta: dict) -> None:
    """Snapshot the live context's storage_state and upsert it. Never raises."""
    try:
        storage_state = await context.storage_state()
        upsert_site_auth(meta["domain"], storage_state, meta.get("label"))
        meta["captured"] = True
        meta["last_captured_at"] = time.monotonic()
    except Exception as exc:
        logger.warning("storage_state capture failed for %s: %s", meta.get("domain"), exc)


async def run_capture_session(session_id: str) -> None:
    """
    Own the Browserbase session's single long-lived CDP connection for its lifetime:
    navigate to the publisher, poll cookies, upsert once the allowlisted auth cookie is
    stable across the debounce window (or immediately on a manual finish), and on the
    hard timeout take a FINAL snapshot before teardown — so a late or non-allowlisted
    login is never lost. Scheduled via FastAPI BackgroundTasks. Never raises.
    """
    meta = _SESSIONS.get(session_id)
    if not meta:
        return

    domain = meta["domain"]
    connect_url = meta["connect_url"]
    cookie_seen_since: float | None = None

    try:
        from playwright.async_api import async_playwright

        async with async_playwright() as pw:
            browser = await pw.chromium.connect_over_cdp(connect_url)
            try:
                context = browser.contexts[0] if browser.contexts else await browser.new_context()
                page = context.pages[0] if context.pages else await context.new_page()
                try:
                    await page.goto(f"https://{domain}/", wait_until="domcontentloaded")
                except Exception as exc:
                    logger.debug("Initial navigation to %s failed (human can navigate): %s", domain, exc)

                deadline = meta["started_at"] + SITE_AUTH_SESSION_TIMEOUT
                while time.monotonic() < deadline:
                    await asyncio.sleep(SITE_AUTH_POLL_INTERVAL)

                    # Manual backstop — capture now regardless of the allowlist.
                    if meta.get("force_capture"):
                        meta["force_capture"] = False
                        await _capture_and_upsert(context, meta)
                        continue

                    if meta.get("captured"):
                        # Already captured once; keep the session open for the human
                        # until timeout, but don't re-upsert and advance captured_at.
                        continue

                    try:
                        cookies = await context.cookies()
                    except Exception:
                        cookies = []

                    if _auth_cookies_present(cookies, domain):
                        if cookie_seen_since is None:
                            cookie_seen_since = time.monotonic()
                        elif time.monotonic() - cookie_seen_since >= SITE_AUTH_DEBOUNCE_SECONDS:
                            await _capture_and_upsert(context, meta)
                    else:
                        cookie_seen_since = None  # cookie gone — reset the debounce

                # Hard timeout: final snapshot so a late/non-allowlisted login survives.
                await _capture_and_upsert(context, meta)
            finally:
                try:
                    await browser.close()
                except Exception:
                    pass
    except Exception as exc:
        logger.warning("Capture session %s for %s ended on error: %s", session_id, domain, exc)
    finally:
        meta["done"] = True
        _release_session(session_id)
        _SESSIONS.pop(session_id, None)
        logger.info("Site-auth session %s for %s torn down", session_id, domain)
