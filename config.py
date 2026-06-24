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
