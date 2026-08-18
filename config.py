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
# LinkedIn + YouTube (regular & Shorts) competitor actors, plus the IG/TikTok
# transcript actors. Actor ids use '~' (not '/') in the API path; all overridable.
APIFY_LINKEDIN_ACTOR             = os.getenv("APIFY_LINKEDIN_ACTOR",             "harvestapi~linkedin-profile-posts")
APIFY_YOUTUBE_ACTOR              = os.getenv("APIFY_YOUTUBE_ACTOR",              "streamers~youtube-scraper")
APIFY_YOUTUBE_SHORTS_ACTOR       = os.getenv("APIFY_YOUTUBE_SHORTS_ACTOR",       "streamers~youtube-shorts-scraper")
APIFY_INSTAGRAM_TRANSCRIPT_ACTOR = os.getenv("APIFY_INSTAGRAM_TRANSCRIPT_ACTOR", "apple_yang~instagram-transcripts-scraper")
APIFY_TIKTOK_TRANSCRIPT_ACTOR    = os.getenv("APIFY_TIKTOK_TRANSCRIPT_ACTOR",    "scrape-creators~best-tiktok-transcripts-scraper")

# Zernio (zernio.com) — OAuth-connected self-account Insights (real shares/saves/
# reach/accounts_engaged, which Apify's public IG scrape can never provide), plus the
# comments/DM inbox (ingestion/inbox.py) that the move off Outstand was for. Account
# connection happens in the Admin app; this pipeline only reads social_accounts.account_id
# (populated there) and pulls analytics. Sole source for self Instagram (Apify's
# self-Instagram scrape is retired — see ingestion/competitors.py's _resolve_channels);
# competitor Instagram tracking is unaffected and stays on Apify. Content_stats/
# competitor_posts window sizing reuses the existing SELF_CONTENT_STATS_*/COMPETITOR_*
# constants below rather than introducing parallel ones.
#
# There is no import/watermark config any more: Outstand billed per imported post, so
# it needed a metered incremental window. Zernio syncs each connected account's external
# posts on its own background cycle (~90 min, ~12 months retained) and analytics are
# plain reads, so there is nothing to bound.
ZERNIO_API_KEY = os.getenv("ZERNIO_API_KEY", "")
# Note the shape: the host is zernio.com and the API lives under /api, so paths in code
# start with an explicit /v1. The docs' "https://api.zernio.com/v1" is a stale variant
# and does not match the OpenAPI spec's declared server.
ZERNIO_API_BASE = os.getenv("ZERNIO_API_BASE", "https://zernio.com/api")
# Shared secret for verifying X-Zernio-Signature on the inbox webhook. When unset the
# receiver fails closed — an unverified webhook is an open write endpoint.
ZERNIO_WEBHOOK_SECRET = os.getenv("ZERNIO_WEBHOOK_SECRET", "")
# Window for the daily follower-series gap-fill. 89 days is the documented ceiling on
# Zernio's Instagram follower-history endpoint; follower-stats (what we actually call)
# isn't documented as capped, but staying inside the same bound keeps the two
# interchangeable if we ever switch.
ZERNIO_FOLLOWER_HISTORY_DAYS = int(os.getenv("ZERNIO_FOLLOWER_HISTORY_DAYS", 89))

# Inbox (ingestion/inbox.py) — the comments + DM mirror. The webhook is the fast path
# but never the complete one: Meta replays up to 500 pre-connect conversations per
# account with NO webhooks at all, dead-lettered events are gone after ~51h, and a
# third party hiding/deleting/liking a comment is never evented. So a reconciliation
# sweep is the source of completeness and the webhook is only latency.
INBOX_SWEEP_LOOKBACK_DAYS = int(os.getenv("INBOX_SWEEP_LOOKBACK_DAYS", 30))
INBOX_PAGE_LIMIT = int(os.getenv("INBOX_PAGE_LIMIT", 50))
# Page cap per listing. Cursor pagination re-queries a live window on every page, so an
# unbounded walk can chase a moving target; the incremental sweep runs every 15 minutes
# and the nightly full pass is what guarantees coverage.
INBOX_MAX_PAGES = int(os.getenv("INBOX_MAX_PAGES", 10))
INBOX_FULL_MAX_PAGES = int(os.getenv("INBOX_FULL_MAX_PAGES", 60))
# Re-open a post's comment thread when it is younger than this even if the comment
# count hasn't moved (edits, hides and deletions don't change the count).
INBOX_THREAD_REFRESH_DAYS = int(os.getenv("INBOX_THREAD_REFRESH_DAYS", 14))
INBOX_EVENT_RETENTION_DAYS = int(os.getenv("INBOX_EVENT_RETENTION_DAYS", 30))
# This service's own public base URL (Railway), used to register the webhook endpoint
# with Zernio. Registration is an API call, not a dashboard setting, so the URL has to
# come from the deployed environment or the subscription points at the wrong host.
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "")

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

# Optional egress proxy for the LOCAL-Chromium read path — gives the scraper a UK
# (or any) IP without a managed-browser vendor. Point it at any proxy you control: an
# Oracle Cloud Free / cheap-VPS London box running gost/Dante, a Tailscale exit node's
# SOCKS proxy, or a residential-proxy endpoint. Format: "http://host:port" or
# "socks5://host:port". Only applied when launching Chromium locally (the CDP/Browserbase
# paths carry their own egress). Empty = direct (Railway's own IP). NB: Chromium does not
# support SOCKS5 with username/password — use an http(s) proxy if you need proxy auth.
RESEARCH_PROXY_SERVER = os.getenv("RESEARCH_PROXY_SERVER", "")
RESEARCH_PROXY_USERNAME = os.getenv("RESEARCH_PROXY_USERNAME", "")
RESEARCH_PROXY_PASSWORD = os.getenv("RESEARCH_PROXY_PASSWORD", "")

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
# keepAlive (Browserbase Startup plan+): keeps the session alive after the Playwright
# CDP connection disconnects. When true, the capture task DETACHES during the login for
# any non-allowlisted domain — navigate, disconnect, let the human log in with zero
# automation attached (no `Runtime.enable` for anti-bot scripts to detect), then
# reconnect briefly only to grab storage_state. Default off: without keepAlive a
# disconnect would end the session, so the connection is held open for the whole login
# as before. Does NOT mask the operator's live DevTools view — that needs Advanced Stealth.
SITE_AUTH_KEEP_ALIVE = os.getenv("SITE_AUTH_KEEP_ALIVE", "false").lower() == "true"
# Per-domain residential-proxy allowlist (comma-separated registrable base domains, e.g.
# "wsj.com,nytimes.com"). These sessions use Browserbase residential egress (better IP
# reputation vs. PerimeterX/DataDome) instead of the default datacenter-leaning pool.
# Residential is metered per-GB, so scope it to the hostile publishers only.
SITE_AUTH_RESIDENTIAL_DOMAINS = {
    d.strip().lower()
    for d in os.getenv("SITE_AUTH_RESIDENTIAL_DOMAINS", "wsj.com").split(",")
    if d.strip()
}
# Read-path toggle: when true the research scraper routes through Browserbase (UK IP)
# instead of local headless Chromium. Default off — local headless stays the default,
# Browserbase is opt-in.
RESEARCH_USE_BROWSERBASE = os.getenv("RESEARCH_USE_BROWSERBASE", "false").lower() == "true"

# Competitor run — caps the most-recent posts captured per competitor and the
# lookback window (in days) they must fall within. Reuses the Apify config above.
COMPETITOR_POST_LIMIT = int(os.getenv("COMPETITOR_POST_LIMIT", 10))
COMPETITOR_LOOKBACK_DAYS = int(os.getenv("COMPETITOR_LOOKBACK_DAYS", 14))

# Public Storage bucket where competitor avatars + post thumbnails are persisted.
# IG/TikTok CDN URLs expire within a day or two, so the pipeline downloads each
# image and re-uploads it here under a deterministic path, writing the stable
# public URL back into the *_avatar_url / thumbnail_url columns.
COMPETITOR_THUMBNAILS_BUCKET = os.getenv("COMPETITOR_THUMBNAILS_BUCKET", "competitor-thumbnails")

# Guest posts (ingestion/guest_posts.py) — calendar items posted from a GUEST's own
# account (content_calendar_items.social_kind = 'guest' in the admin app). The guest
# may or may not be a tracked competitor, so their stats come from one Apify run per
# post URL rather than any profile scrape. The daily sweep only re-scrapes posts whose
# publish_date falls within this window; the admin's on-demand refresh ignores it.
GUEST_POST_LOOKBACK_DAYS = int(os.getenv("GUEST_POST_LOOKBACK_DAYS", 90))
# LinkedIn single-post actor (the profile actor above can't take a post URL).
# Verified live (2026-08-12): takes {"post_urls": [url-or-id, ...]}, no cookies.
# Set empty to disable LinkedIn guest scrapes (items log a warning and skip).
APIFY_LINKEDIN_POST_ACTOR = os.getenv("APIFY_LINKEDIN_POST_ACTOR", "apimaestro~linkedin-post-detail")

# The is_self ("The Curve") competitor additionally feeds content_stats. That feed
# uses a wider window than the competitor card: a larger per-channel fetch and a
# separate (longer) lookback applied only to the content_stats post set. The
# competitor_posts card still uses COMPETITOR_POST_LIMIT / COMPETITOR_LOOKBACK_DAYS.
SELF_CONTENT_STATS_LOOKBACK_DAYS = int(os.getenv("SELF_CONTENT_STATS_LOOKBACK_DAYS", 90))
SELF_CONTENT_STATS_LIMIT = int(os.getenv("SELF_CONTENT_STATS_LIMIT", 100))

# ── Inbox reply triage + drafting (drafting/draft.py) ─────────────────────────
# The stage judges every thread waiting on us and drafts a reply for the ones that need
# one, so the work is bounded by "needs a human" rather than by age.
DRAFT_MODEL = os.getenv("DRAFT_MODEL", "claude-opus-5")
# 0 = no age gate: judge a thread as soon as it is waiting on us. This used to be 24,
# from when the stage targeted only the neglect tail (median reply is 0.3h, p90 247.8h) —
# with a judgement in front of the drafting that filter is doing the wrong job, but the
# knob survives so reinstating it is a config change.
DRAFT_MIN_AGE_HOURS = int(os.getenv("DRAFT_MIN_AGE_HOURS", 0))
# A runaway backstop, not a policy. Sized to clear the whole standing backlog in one run
# (116 waiting threads when the judgement was introduced) with headroom; a run that hits
# it logs a warning rather than truncating quietly.
DRAFT_MAX_THREADS = int(os.getenv("DRAFT_MAX_THREADS", 200))
# The exemplar corpus is the cached prompt prefix. ~111 pairs clear the length filters
# today (~10k tokens), so the cap is headroom rather than a real limit.
DRAFT_EXEMPLAR_LIMIT = int(os.getenv("DRAFT_EXEMPLAR_LIMIT", 150))
# Below these lengths the pairs are "😂" / "thank you!" — real replies, useless as style
# examples: 111 of the 473 raw pairs have replies under 25 characters.
DRAFT_EXEMPLAR_MIN_REPLY_LEN = int(os.getenv("DRAFT_EXEMPLAR_MIN_REPLY_LEN", 80))
DRAFT_EXEMPLAR_MIN_INCOMING_LEN = int(os.getenv("DRAFT_EXEMPLAR_MIN_INCOMING_LEN", 40))
DRAFT_THREAD_CONTEXT_MESSAGES = int(os.getenv("DRAFT_THREAD_CONTEXT_MESSAGES", 12))
DRAFT_MAX_TOKENS = int(os.getenv("DRAFT_MAX_TOKENS", 2000))
