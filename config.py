import os
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env.local"))

SUPABASE_URL = os.environ["NEXT_PUBLIC_SUPABASE_URL"]
SUPABASE_SERVICE_ROLE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

NEWSAPI_KEY = os.getenv("NEWSAPI_API_KEY", "")
FINNHUB_KEY = os.getenv("FINNHUB_API_KEY", "")
VOYAGE_API_KEY = os.getenv("VOYAGE_API_KEY", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")

# Apify — scrapes Instagram/TikTok social sources in the scan stage.
APIFY_TOKEN = os.getenv("APIFY_TOKEN", "")
# Actor ids use '~' (not '/') in the API path. Overridable in case we switch actors.
APIFY_INSTAGRAM_ACTOR = os.getenv("APIFY_INSTAGRAM_ACTOR", "apify~instagram-api-scraper")
APIFY_TIKTOK_ACTOR = os.getenv("APIFY_TIKTOK_ACTOR", "clockworks~tiktok-scraper")

# Maximum articles to keep per source per run (0 = no limit)
MAX_ARTICLES_PER_SOURCE = int(os.getenv("MAX_ARTICLES_PER_SOURCE", 50))

# Browser scraper — research stage renders article pages in a real Chromium tab
# (Playwright), seeded with a per-domain logged-in storage_state from site_auth,
# so paywalled/JS-rendered publishers extract their full body. Auth itself is what
# beats the paywall; extraction stays deterministic (trafilatura over rendered HTML).
# Engine toggle: false falls back to the static httpx scraper (safe degrade).
RESEARCH_USE_BROWSER = os.getenv("RESEARCH_USE_BROWSER", "true").lower() == "true"
# Per-page navigation/extraction timeout in milliseconds.
BROWSER_PAGE_TIMEOUT = int(os.getenv("BROWSER_PAGE_TIMEOUT", 45000))
# Hard cap on browser scrapes per run (bounds a runaway run; ~50/day expected).
MAX_BROWSER_SCRAPES_PER_RUN = int(os.getenv("MAX_BROWSER_SCRAPES_PER_RUN", 100))
# Optional hosted-Chromium endpoint (Browser Use Cloud / browserless / Steel).
# When set, connect over CDP instead of launching Chromium locally — no other code
# change. Safety valve if Railway memory proves tight.
BROWSER_CDP_URL = os.getenv("BROWSER_CDP_URL", "")

# Site-auth capture (research/site_auth.py) — launches a headful, human-driven remote
# browser via Browserbase so an operator can complete a real publisher login; on login
# completion the captured Playwright storage_state() is upserted into site_auth (the
# write half of the per-domain auth the research scraper reads). Needs a Browserbase
# account/project with UK-proxy entitlement provisioned.
BROWSERBASE_API_KEY = os.getenv("BROWSERBASE_API_KEY", "")
BROWSERBASE_PROJECT_ID = os.getenv("BROWSERBASE_PROJECT_ID", "")
# Hard timeout (seconds) bounding a login session before teardown + final capture.
SITE_AUTH_SESSION_TIMEOUT = int(os.getenv("SITE_AUTH_SESSION_TIMEOUT", 600))
# How often (seconds) the capture task polls the live context's cookies.
SITE_AUTH_POLL_INTERVAL = int(os.getenv("SITE_AUTH_POLL_INTERVAL", 5))
# Debounce: the publisher's auth cookie(s) must be present across this many seconds
# of consecutive polls before the upsert fires (guards against a premature capture
# that would close the admin modal mid-login).
SITE_AUTH_DEBOUNCE_SECONDS = int(os.getenv("SITE_AUTH_DEBOUNCE_SECONDS", 10))
# Read-path toggle: when true the research scraper routes through Browserbase (UK IP)
# instead of local headless Chromium. Default off — local headless stays the default,
# Browserbase is opt-in.
RESEARCH_USE_BROWSERBASE = os.getenv("RESEARCH_USE_BROWSERBASE", "false").lower() == "true"

# Competitor run — caps the most-recent posts captured per competitor and the
# lookback window (in days) they must fall within. Reuses the Apify config above.
COMPETITOR_POST_LIMIT = int(os.getenv("COMPETITOR_POST_LIMIT", 10))
COMPETITOR_LOOKBACK_DAYS = int(os.getenv("COMPETITOR_LOOKBACK_DAYS", 14))
