"""
Registrable-domain helpers — the single source of truth for the base-domain key that
site_auth rows are keyed by.

Both halves of the site-auth feature MUST derive the key the same way, or a captured
session keys under one string and the scraper looks it up under another:
  * the capture-write path (research/site_auth.py) normalises the requested login
    domain to its registrable base before upserting site_auth;
  * the research read path (research/research.py) reduces each article URL to the same
    base before fetching the stored storage_state.

`host_matches` implements the contract's host-resolution rule verbatim:
    host == domain or host.endswith("." + domain)
"""

from urllib.parse import urlparse

# Multi-part public suffixes we may encounter; keep the last 3 labels so a
# 'www.bbc.co.uk/news' URL keys on 'bbc.co.uk', not 'co.uk'. Small, hand-maintained
# list — enough for the handful of UK/AU publishers in scope.
_TWO_LABEL_TLDS = {
    "co.uk", "org.uk", "gov.uk", "ac.uk", "co.nz", "com.au", "net.au",
    "org.au", "co.za", "co.jp", "com.br",
}


def registrable_domain(url_or_host: str) -> str:
    """
    Reduce a URL or bare host to its registrable domain, e.g.
    'https://www.ft.com/content/abc' -> 'ft.com',
    'https://www.bbc.co.uk/news/x'   -> 'bbc.co.uk',
    'www.ft.com'                     -> 'ft.com'.
    Returns '' if no host can be parsed.
    """
    s = (url_or_host or "").strip()
    if not s:
        return ""
    # urlparse only extracts hostname when a scheme/'//' is present; accept bare hosts.
    host = urlparse(s).hostname if "//" in s or "://" in s else None
    if not host:
        # Treat the input as a bare host (strip any path/port that snuck in).
        host = s.split("/")[0].split(":")[0]
    host = (host or "").lower().strip(".")
    if not host:
        return ""
    parts = host.split(".")
    if len(parts) <= 2:
        return host
    last_two = ".".join(parts[-2:])
    if last_two in _TWO_LABEL_TLDS:
        return ".".join(parts[-3:])
    return last_two


def host_matches(host: str, domain: str) -> bool:
    """
    True when `host` belongs to the registrable `domain`, per the contract:
        host == domain or host.endswith("." + domain)
    e.g. host_matches('www.ft.com', 'ft.com') -> True.
    """
    host = (host or "").lower().strip(".")
    domain = (domain or "").lower().strip(".")
    if not host or not domain:
        return False
    return host == domain or host.endswith("." + domain)
