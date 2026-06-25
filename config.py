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
