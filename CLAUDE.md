# Curve Media — Data Pipeline (Curve_Data_Py)

The Python half of The Curve Media platform. A financial-news pipeline that ingests
articles/social posts, filters → clusters → scores → tags → researches → briefs them,
and writes results to Supabase. The Next.js Admin app
(`out-sauce__The-Curve-Media-Admin`) is the human-facing consumer of this data and
triggers stages over HTTP.

## Tech stack

- **Python 3.11+**, FastAPI + Uvicorn (`api.py`) for the HTTP control surface.
- **APScheduler** runs the daily pipeline (05:00 UTC) in a background thread.
- **Supabase** (service-role client, `ingestion/storage.py`) for all persistence.
- **Anthropic Claude** (`claude-sonnet-4-6`) for scoring/tagging/summaries.
- **Playwright + Chromium** for the research-stage browser scraper; **Browserbase**
  for headful, human-driven remote logins (site-auth capture).
- Apify / NewsAPI / Finnhub / feedparser for ingestion sources.
- Deployed on **Railway** via the `Dockerfile` (bundles Chromium). **Single replica**
  required — the site-auth flow keeps an in-process session registry.

## Run & test

- Install deps: `pip install -r requirements.txt` then `playwright install --with-deps chromium`.
- Config comes from env / `.env.local` (see `config.py`). Required: `NEXT_PUBLIC_SUPABASE_URL`,
  `SUPABASE_SERVICE_ROLE_KEY`. Optional: `PIPELINE_API_KEY` (guards every endpoint),
  `ANTHROPIC_API_KEY`, Apify/NewsAPI/Finnhub keys, `BROWSERBASE_API_KEY` +
  `BROWSERBASE_PROJECT_ID` (site-auth capture).
- API: `uvicorn api:app --reload`. CLI: `python main.py --once` (full run) or
  `python main.py --stage <ingest|filter|cluster|score|competitors> [--date YYYY-MM-DD]`.
- There is no formal test suite; validate with `python -m py_compile` on changed files
  and `fastapi.testclient.TestClient` smoke tests of the endpoints.

## Key conventions

- Every `/run/*` and `/site-auth/*` endpoint is sync `def`, checks `x-api-key` via
  `_check_key`, and schedules real work on FastAPI `BackgroundTasks` (coroutines are
  awaited). Return `{"status": ...}` immediately.
- Scrapers never raise — errors come back as `ScrapeResult(status="failed")`. The
  site-auth capture path follows the same never-crash discipline.
- `site_auth` rows are keyed by **registrable base domain** (e.g. `ft.com`). The single
  source of truth for that key is `research/domains.py` (`registrable_domain`,
  `host_matches`); both the capture writer and the scraper reader import it so the keys
  cannot drift.
- DB schema changes are manual, timestamped SQL files under `migrations/`; nothing is
  auto-applied at runtime.

## Recent changes

- **Competitor multi-channel reshape + The Curve → content_stats.** `ingestion/competitors.py`
  now treats each competitor as one brand row with up to two channels: it resolves the
  `instagram`/`tiktok` handle (from `*_handle`, falling back to parsing `*_url`), scrapes
  each channel, and writes the per-platform `{instagram,tiktok}_{avatar_url,follower_count,engagement_rate,post_count}`
  columns plus `competitor_posts.platform` (legacy single columns are left to the Admin's
  backfill). Avatars and post thumbnails are downloaded and re-uploaded to the public
  `competitor-thumbnails` Storage bucket (deterministic paths, overwrite on re-run) so the
  stored public URL never expires; a failed image fetch preserves the prior value (new
  posts fall through to `null`). The single `is_self` ("The Curve") row additionally upserts
  its posts into `content_stats`, deduped on `(platform, post_id)` via lookup-then-update-else-insert
  (only scraped fields touched, so `shares`/`saves`/`reach` etc. survive), over a wider
  90-day window (`SELF_CONTENT_STATS_LOOKBACK_DAYS` / `SELF_CONTENT_STATS_LIMIT`) decoupled
  from the 14-day/10-post `competitor_posts` cap. Per-channel `try/except` keeps one bad
  channel from blanking the other. New `config.py` keys: `COMPETITOR_THUMBNAILS_BUCKET`,
  `SELF_CONTENT_STATS_LOOKBACK_DAYS`, `SELF_CONTENT_STATS_LIMIT`. All schema already exists
  on the live DB (shipped by the Admin app); no migration is applied here.

- **Site-auth login capture (write half).** Added `research/site_auth.py`: a Browserbase
  headful remote-login flow. `POST /site-auth/login/start?domain=&label=` returns
  `{session_id, live_url}` (Browserbase fullscreen debugger URL) and schedules a
  background task that navigates a UK-proxied remote browser, watches for the
  publisher's auth cookie (per-publisher allowlist + debounce; FT only at launch, BBC
  has no paywall), and upserts `site_auth` once a genuine login is detected — or takes a
  final snapshot at a 10-minute hard timeout. `POST /site-auth/login/finish?session_id=`
  is a manual backstop forcing an immediate capture. The shared domain helper was
  promoted to `research/domains.py`. The research read-path scraper gained an env-toggled
  Browserbase route (`RESEARCH_USE_BROWSERBASE`, default off). A startup log asserts the
  single-replica assumption (in-process session registry).
