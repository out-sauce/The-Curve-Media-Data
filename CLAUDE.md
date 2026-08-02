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

- **Cluster status semantics tightened (scored / researched / briefed).** A cluster
  only advances to `researched` when **at least one of its articles has a non-NULL
  `scrape_status`** (any value — scraped/paywalled/bot_wall/failed all count; NULL means
  nothing was ever attempted, so it stays `scored`). The shared helper is
  `research.research._advance_clusters_to_researched` (never demotes; only
  `pending`/`scored` advance, `briefed`/`archived` untouched, never raises) and it is
  called from every lane that gives an article a scrape_status: the daily batch
  `run_research` (which previously flipped **all** selected clusters unconditionally,
  even all-manual-skipped ones), `run_research_cluster`, `run_research_article`, and the
  extension import (`_maybe_rebrief_cluster`). `briefed` is **reserved for the manual
  content-studio flow** (Admin `saveResearchData`) — no pipeline code sets it anymore
  (the dormant `custom_clustering` stage now inserts `scored` instead of `briefed`), and
  `rebrief_cluster` now refuses to overwrite a `briefed` cluster's brief, since that
  column then holds the studio's manual output. Auto-generated briefs (batch briefing,
  on-demand re-brief) keep the cluster at `researched`. Admin's toolbar "Run research
  (score 70+)" button (`runResearchForTodaysTopArticles`) was switched from per-article
  dispatch (which never wrote a brief) to one `POST /run/research?cluster_id=` per
  qualifying story — same path as the story-level Research button, so scrape → brief →
  status advance happen together; it skips `briefed`/`archived` clusters.

- **Competitor social scan is weekly; own channels stay daily.** The scheduled
  pipeline (`ingestion/scheduler.py`) runs the full competitor sweep only on Mondays;
  every other day it calls `run_competitors(self_only=True)`, which filters to the
  is_self ("The Curve") row so daily follower snapshots and content_stats upserts
  continue uninterrupted. The manual `POST /run/competitors` endpoint and
  `python main.py --stage competitors` still run the full sweep on demand any day.

- **Cluster-level on-demand research + brief redo.** Admin's story **Research** button now
  makes one call — `POST /run/research?cluster_id=` → `run_research_cluster` — instead of
  fanning out per article. It never re-scrapes articles that already have a `deep_summary`;
  the rest are routed by domain scrape mode (`manual` → extension queue via
  `enqueue_article`, `auto` → inline `_process_article`), and the cluster brief is then
  **regenerated unconditionally** via `briefing.brief.rebrief_cluster(cluster_id)`
  (overwrites `name`/`brief`/`briefed_at`; still requires ≥1 deep summary; ignores the
  run_briefing "already briefed" skip). A successful extension import
  (`complete_from_html`) also re-briefs the article's cluster, but only when the cluster
  already has a brief — so briefs refresh as late extension grabs land without preempting
  the briefing stage. `run_research_article` (`?id=`) keeps its always-rescrape override.
  Admin's `runResearchForTodaysTopArticles` bulk action now skips `scrape_status='scraped'`
  articles too.

- **Account-safe research: per-domain auto/manual scrape mode + `bot_wall` + extension
  content-grab lane.** The daily batch used to send subscriber logins to every domain from
  a datacenter IP; PerimeterX/DataDome publishers (Bloomberg confirmed) bot-wall those
  reads, and tying bot-flagged automated traffic to a paid account risks suspension. Now a
  per-**registrable-domain** policy lives in `domain_scrape_settings` (`scrape_mode` =
  `auto`|`manual`, keyed like `site_auth.domain`/`sources.site_auth_domain`, default
  `auto`). `run_research` scrapes `auto` domains and skips `manual` ones; the first time an
  automated scrape returns `bot_wall` or `paywalled` it **auto-demotes** the domain to
  `manual` (`_demote_domain`), so a hostile publisher is hit with the login **at most
  once**. `bloomberg.com` is seeded `manual`. Toggle via `POST /sources/scrape-mode?domain=&mode=`
  (Admin Sources page). A new `bot_wall` scrape status distinguishes an anti-bot/CAPTCHA
  wall from a subscription paywall — `research/browser_scraper.py` detects it (`_is_bot_wall`)
  and also strips PerimeterX `_px*`/`pxcts` cookies (`_strip_bot_cookies`) before rendering;
  migration 029 widened the `news_articles.scrape_status` CHECK (from migration 018) to
  allow `bot_wall`, or the write is rejected. `run_research_article` (manual initiation)
  ignores the policy and always tries. **Extension content-grab lane** for `manual`
  domains: Admin's Research button enqueues an article (`research_queue` table, `POST
  /research/enqueue?id=`); the Curve Auth Chrome extension polls (`POST /research/queue/claim`),
  opens the article in the operator's real logged-in browser (`background.js` service
  worker + `chrome.alarms`), reads the rendered HTML, and posts it to `POST /research/import`
  → `research_from_html` runs the same trafilatura extract + Claude summary via the shared
  `_persist_result` (`scrape_method="extension"`). No server-side fetch, so nothing for a
  bot detector to flag. Only runs while the operator's Chrome is open with auto-research
  enabled (options toggle). The popup also has a **Send article content** button: it posts
  the current tab's rendered HTML to `/research/import` with `url` instead of `article_id`
  (`resolve_article_by_url` matches exact → query/fragment-stripped → prefix, 404 if the
  page isn't a pipeline article, and closes any outstanding `research_queue` row) — no
  Admin round-trip needed when the operator is already on the article. Browserbase is no longer used for auth capture (the extension
  replaced it) and the read-path toggle `RESEARCH_USE_BROWSERBASE` should be off. Migrations
  029 (`domain_scrape_settings` + CHECK) and 030 (`research_queue`) applied manually; both
  tables have RLS enabled with no policies (service-role access only), matching
  `sources`/`site_auth`.

- **LinkedIn + YouTube competitor channels & post transcripts.** `ingestion/competitors.py`
  now resolves up to five channels per competitor: Instagram, TikTok, LinkedIn, YouTube
  and YouTube Shorts. LinkedIn scrapes via `harvestapi~linkedin-profile-posts` (handle is
  the full profile URL); YouTube + Shorts share one handle and one set of `youtube_*` stat
  columns on `competitors` (`stats_key="youtube"` folds Shorts in), while
  `competitor_posts.platform` keeps `"youtube"`/`"youtube_shorts"` distinct. Each post is
  written to `competitor_posts` and, for the `is_self` ("The Curve") row, to `content_stats`
  / `follower_snapshots` exactly as the existing IG/TikTok flow does. Instagram + TikTok
  posts additionally get a best-effort `transcript` (one batched Apify call per channel over
  the selected posts) written to `competitor_posts.transcript` and `content_stats.transcript`;
  LinkedIn/YouTube have no transcript for now. New `config.py` actor ids: `APIFY_LINKEDIN_ACTOR`,
  `APIFY_YOUTUBE_ACTOR`, `APIFY_YOUTUBE_SHORTS_ACTOR`, `APIFY_INSTAGRAM_TRANSCRIPT_ACTOR`,
  `APIFY_TIKTOK_TRANSCRIPT_ACTOR` (all env-overridable). Migration
  `026_add_linkedin_youtube_transcript.sql` adds the `linkedin_*`/`youtube_*` columns plus
  `transcript` (idempotent; applied manually). The LinkedIn/YouTube actor inputs+outputs were
  verified live (2026-07-02) and corrected — the earlier best-guesses were all wrong and
  fetched nothing/nulls: **LinkedIn** (`harvestapi~linkedin-profile-posts`) takes
  `{"targetUrls": [profile_or_company_url], "maxPosts": n}` (NOT `profileUrl`/`resultsLimit`);
  each item exposes `author{}` (follower count is a *string* in `author.info`, e.g.
  "1,811 followers"; avatar at `author.avatar.url`), `content` (text), `engagement.{likes,comments,shares}`,
  `postedAt.date`, `linkedinUrl`, `postImages[]`. It also returns **reposts** whose `author` is
  the original poster, so `_li_profile` reads the follower count from a native (non-repost) post.
  **YouTube** (`streamers~youtube-scraper`) takes a ready-made channel URL in
  `startUrls` — pass `youtube_url` verbatim; never rebuild as `@{handle}` (a `channel/UC…` id
  becomes `@channel/UC…` → CHANNEL_DOES_NOT_EXIST). Channel IDs are case-sensitive. There is no
  standalone `type:"channel"` item — every video item carries `numberOfSubscribers`,
  `channelTotalVideos`, `channelName`, `channelAvatarUrl`; posts use `id`/`title`/`url`/`date`/
  `viewCount`/`likes`/`commentsCount`/`thumbnailUrl`. LinkedIn/YouTube have no transcript for now.
  Actors return a bad target as a data item with an `error` key (not a non-2xx), so `_run_channel`
  raises on an `error` item or on a no-follower-count-and-no-posts result → logs an error
  `source_run` instead of silently writing nulls. The IG/TikTok transcript actors were verified
  live (2026-06-30): the IG actor takes a single `{"videoUrl": ...}` per run (one run per post,
  `transcript_batched=False`) and returns text in `text`; the TikTok actor takes
  `{"videos": [...]}` and returns it in `transcript`.

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
