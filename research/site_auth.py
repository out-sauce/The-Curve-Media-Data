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
    SITE_AUTH_KEEP_ALIVE,
    SITE_AUTH_POLL_INTERVAL,
    SITE_AUTH_RESIDENTIAL_DOMAINS,
    SITE_AUTH_SESSION_TIMEOUT,
)
from ingestion.storage import get_client
from .domains import registrable_domain

logger = logging.getLogger(__name__)

SITE_AUTH_TABLE = "site_auth"

# Browserbase only runs in us-west-2 / us-east-1 / eu-central-1 / ap-southeast-1.
# eu-central-1 (Frankfurt) is the nearest to the UK; the UK egress IP is delivered by
# the paired GB proxy + geolocation, not the region. Matches the scraper's en-GB
# locale and the UK/AU publishers in scope.
_BROWSERBASE_REGION = "eu-central-1"

# Per-publisher cookie allowlist: base domain -> the named auth cookie(s) whose
# presence (stable across the debounce window) means a genuine logged-in session.
# Gates the periodic-snapshot upsert so the Admin modal never closes mid-login.
#
# 'FTSession_s' is the FT subscriber/session cookie; confirm the exact name empirically
# in the Browserbase debugger during build (FT also sets 'FTSession'). BBC is
# intentionally absent — it has no auth paywall, so it falls back to timeout
# final-capture only.
#
# wsj.com: Dow Jones sets the persistent auto-login token 'djcs_auto' and the session
# cookie 'djcs_session' on a genuine WSJ login — best-guess names to VERIFY in the
# debugger (like FT was). Until confirmed, a wrong name simply means WSJ falls back to
# timeout final-capture (same as no entry), so this is safe to ship unverified.
_AUTH_COOKIE_ALLOWLIST: dict[str, list[str]] = {
    "ft.com": ["FTSession_s", "FTSession"],
    "wsj.com": ["djcs_auto", "djcs_session"],
}

# In-process registry: session_id -> session metadata. Single-replica only.
_SESSIONS: dict[str, dict[str, Any]] = {}


class SiteAuthUnavailable(Exception):
    """Raised when Browserbase is not provisioned/configured — the endpoint maps this
    to a 404 so the Admin button shows its existing 'not yet available' message."""


# ---------------------------------------------------------------------------
# DB write
# ---------------------------------------------------------------------------

def upsert_site_auth(
    domain: str, storage_state: dict, label: str | None, raise_on_error: bool = False
) -> None:
    """
    Upsert the captured session into site_auth keyed by the registrable base domain —
    the exact row write the Admin poll awaits. Complementary to research.py's
    _record_auth_usage (which only touches last_status/last_used_at). Best-effort by
    default: logs and swallows on failure so a provider/DB hiccup never crashes the
    background capture task. The interactive import path passes raise_on_error=True so
    the HTTP caller gets a real success/failure instead of a false OK.
    """
    now = datetime.now(timezone.utc).isoformat()
    row = {
        "domain": domain,
        "storage_state": storage_state,
        "captured_at": now,
        "last_status": "captured",
        "updated_at": now,
    }
    # Only write `label` when supplied. On an upsert conflict a column absent from the
    # payload is left out of the UPDATE SET clause, so an import (which sends no label)
    # preserves the Admin-set label rather than nulling it; a fresh row just defaults it.
    if label is not None:
        row["label"] = label
    try:
        get_client().table(SITE_AUTH_TABLE).upsert(row, on_conflict="domain").execute()
        logger.info("site_auth captured for %s (label=%s)", domain, label)
    except Exception as exc:
        logger.warning("Could not upsert site_auth for %s: %s", domain, exc)
        if raise_on_error:
            raise


# ---------------------------------------------------------------------------
# Manual import — cookies lifted from a real, human browser (no remote browser)
# ---------------------------------------------------------------------------

# Browser cookie stores (chrome.cookies / Cookie-Editor exports) spell sameSite
# differently from Playwright's storage_state, which only accepts Strict/Lax/None.
_SAME_SITE_MAP = {
    "no_restriction": "None",
    "none": "None",
    "lax": "Lax",
    "unspecified": "Lax",
    "": "Lax",
    "strict": "Strict",
}


def _normalise_cookie(c: dict) -> dict | None:
    """Map one browser/Cookie-Editor cookie dict to a Playwright storage_state cookie.
    Returns None for a cookie missing the fields Playwright requires."""
    name = c.get("name")
    domain = c.get("domain")
    if not name or not domain:
        return None

    # Session cookies have no expiry; Playwright encodes that as -1.
    raw_exp = c.get("expires", c.get("expirationDate"))
    if c.get("session"):
        expires = -1.0
    else:
        try:
            expires = float(raw_exp)
        except (TypeError, ValueError):
            expires = -1.0

    same_site = _SAME_SITE_MAP.get(str(c.get("sameSite") or "").lower(), "Lax")
    return {
        "name": name,
        "value": c.get("value") or "",
        "domain": domain,
        "path": c.get("path") or "/",
        "expires": expires,
        "httpOnly": bool(c.get("httpOnly")),
        "secure": bool(c.get("secure")),
        "sameSite": same_site,
    }


def _build_origins(origins_in: list | None) -> list[dict]:
    """Normalise localStorage payloads into Playwright storage_state `origins`. Accepts
    each origin's localStorage as either a {key: value} object or a [{name, value}] list."""
    out: list[dict] = []
    for o in origins_in or []:
        origin = o.get("origin") if isinstance(o, dict) else None
        if not origin:
            continue
        ls = o.get("localStorage")
        items: list[dict] = []
        if isinstance(ls, dict):
            items = [{"name": str(k), "value": str(v)} for k, v in ls.items()]
        elif isinstance(ls, list):
            items = [
                {"name": it["name"], "value": str(it.get("value", ""))}
                for it in ls
                if isinstance(it, dict) and it.get("name")
            ]
        out.append({"origin": origin, "localStorage": items})
    return out


def import_storage_state(
    domain: str,
    cookies: list[dict],
    origins: list[dict] | None = None,
    label: str | None = None,
) -> dict:
    """
    Build a Playwright storage_state from cookies (and optional localStorage) captured in
    a real human browser — the Chrome extension or a Cookie-Editor JSON export — and
    upsert it into site_auth, keyed by the registrable base of `domain` (the exact key
    the research scraper reads). No automation ever touches the publisher, so there is
    nothing for an anti-bot system to detect. Raises ValueError on bad input and
    propagates a DB failure so the HTTP caller sees a real result.
    """
    base = registrable_domain(domain)
    if not base:
        raise ValueError(f"Could not derive a registrable domain from {domain!r}")

    normalised = [nc for c in (cookies or []) if (nc := _normalise_cookie(c))]
    if not normalised:
        raise ValueError("No valid cookies supplied (need at least name + domain)")

    storage_state = {"cookies": normalised, "origins": _build_origins(origins)}
    upsert_site_auth(base, storage_state, label, raise_on_error=True)
    return {
        "status": "imported",
        "domain": base,
        "cookies": len(normalised),
        "origins": len(storage_state["origins"]),
    }


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
    # Hostile publishers (SITE_AUTH_RESIDENTIAL_DOMAINS, e.g. wsj.com) egress via
    # residential IPs — far better reputation against PerimeterX/DataDome IP scoring
    # than the default datacenter-leaning pool; residential is metered per-GB so it's
    # scoped per-domain rather than global.
    proxy = {"type": "browserbase", "geolocation": {"country": "GB"}}
    if base in SITE_AUTH_RESIDENTIAL_DOMAINS:
        proxy["residential"] = True

    # keepAlive lets the capture task detach during login for non-allowlisted domains
    # (see run_capture_session); harmless when the detach path is unused. Requires a
    # Browserbase plan that supports it — gated behind SITE_AUTH_KEEP_ALIVE (default off).
    create_kwargs: dict[str, Any] = {
        "project_id": BROWSERBASE_PROJECT_ID,
        "region": _BROWSERBASE_REGION,
        "timeout": SITE_AUTH_SESSION_TIMEOUT,
        "proxies": [proxy],
    }
    if SITE_AUTH_KEEP_ALIVE:
        create_kwargs["keep_alive"] = True
    session = bb.sessions.create(**create_kwargs)

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


async def _navigate(context, domain: str) -> None:
    """Land the human on the publisher page. Best-effort — they can navigate themselves."""
    page = context.pages[0] if context.pages else await context.new_page()
    try:
        await page.goto(f"https://{domain}/", wait_until="domcontentloaded")
    except Exception as exc:
        logger.debug("Initial navigation to %s failed (human can navigate): %s", domain, exc)


async def _run_attached(pw, connect_url: str, domain: str, meta: dict, deadline: float) -> None:
    """
    Hold the Browserbase session's single long-lived CDP connection for its lifetime:
    navigate, poll cookies, upsert once the allowlisted auth cookie is stable across the
    debounce window (or immediately on a manual finish), and on the hard timeout take a
    FINAL snapshot before teardown. This is the default path — required for allowlisted
    domains (gating on the auth cookie needs live polling) and whenever keepAlive is off
    (a disconnect would end the session). Automation stays attached the whole login, so
    `Runtime.enable` is visible to anti-bot scripts; mask it with Advanced Stealth.
    """
    cookie_seen_since: float | None = None
    browser = await pw.chromium.connect_over_cdp(connect_url)
    try:
        context = browser.contexts[0] if browser.contexts else await browser.new_context()
        await _navigate(context, domain)

        while time.monotonic() < deadline:
            await asyncio.sleep(SITE_AUTH_POLL_INTERVAL)

            # Manual backstop — capture now regardless of the allowlist.
            if meta.get("force_capture"):
                meta["force_capture"] = False
                await _capture_and_upsert(context, meta)
                continue

            if meta.get("captured"):
                # Already captured once; keep the session open for the human until
                # timeout, but don't re-upsert and advance captured_at.
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


async def _run_detached(pw, connect_url: str, domain: str, meta: dict, deadline: float) -> None:
    """
    keepAlive-only path: attach ONLY to navigate, then disconnect so the human logs in
    with zero automation attached — no `Runtime.enable` for PerimeterX/DataDome to flag
    during the sensitive login window. Reconnect briefly only to snapshot the session on
    a manual finish or the hard timeout. Because we can't poll cookies while detached
    there's no allowlist gating here: capture is finish- or timeout-driven (as it already
    was for non-allowlisted domains). Requires keepAlive so the session survives the
    disconnect; guarded so a keepAlive-unavailable plan just fails safely into teardown.

    NOTE: does NOT mask the operator's live Browserbase debugger view, which is itself
    DevTools attached to the page — that "developer tools detected" signal needs Advanced
    Stealth. Verify the keepAlive reconnect end-to-end in the debugger before relying on it.
    """
    # 1) Brief attach purely to land the human on the publisher page, then disconnect.
    browser = await pw.chromium.connect_over_cdp(connect_url)
    try:
        context = browser.contexts[0] if browser.contexts else await browser.new_context()
        await _navigate(context, domain)
    finally:
        try:
            await browser.close()
        except Exception:
            pass

    # 2) Wait with NO connection open for a manual finish or the hard timeout.
    while time.monotonic() < deadline:
        await asyncio.sleep(SITE_AUTH_POLL_INTERVAL)
        if meta.get("force_capture"):
            break

    # 3) Reconnect briefly only to snapshot the now-logged-in session.
    meta["force_capture"] = False
    browser = await pw.chromium.connect_over_cdp(connect_url)
    try:
        context = browser.contexts[0] if browser.contexts else await browser.new_context()
        await _capture_and_upsert(context, meta)
    finally:
        try:
            await browser.close()
        except Exception:
            pass


async def run_capture_session(session_id: str) -> None:
    """
    Own the Browserbase session for its lifetime and capture the logged-in storage_state.
    Dispatches to the attached path (default; gates allowlisted domains, keeps the session
    alive when keepAlive is off) or the detached path (keepAlive + non-allowlisted domain:
    no automation attached during login). Scheduled via FastAPI BackgroundTasks. Never raises.
    """
    meta = _SESSIONS.get(session_id)
    if not meta:
        return

    domain = meta["domain"]
    connect_url = meta["connect_url"]
    deadline = meta["started_at"] + SITE_AUTH_SESSION_TIMEOUT
    # Detach only when keepAlive can preserve the session across the disconnect AND we
    # aren't gating on an auth cookie (which needs live polling). Allowlisted domains
    # (FT, WSJ) therefore stay on the attached path — the gating-vs-clean-login tradeoff:
    # to detach WSJ instead, drop it from _AUTH_COOKIE_ALLOWLIST and it becomes eligible.
    detach = SITE_AUTH_KEEP_ALIVE and domain not in _AUTH_COOKIE_ALLOWLIST

    try:
        from playwright.async_api import async_playwright

        async with async_playwright() as pw:
            if detach:
                await _run_detached(pw, connect_url, domain, meta, deadline)
            else:
                await _run_attached(pw, connect_url, domain, meta, deadline)
    except Exception as exc:
        logger.warning("Capture session %s for %s ended on error: %s", session_id, domain, exc)
    finally:
        meta["done"] = True
        _release_session(session_id)
        _SESSIONS.pop(session_id, None)
        logger.info("Site-auth session %s for %s torn down", session_id, domain)
