# Article research

All claims verified — the repo matches the plan exactly, no `LIVE_DB_SCHEMA.sql` (migrations are authoritative), and the reviewer's Playwright-vs-Browser-Use question is now decisively answerable. The one refinement to fold in from the final clarifying answer: sessions expire at 7/30 days, so the plan should surface a **stale-auth tag** back to the portal.

## Context

The pipeline groups articles into **stories** (`story_clusters`, `migrations/002`) whose members are `news_articles` rows. The **research stage** (`research/research.py`, triggered by `POST /run/research`) is the "add research/article content to the article" step: for each article in a `scored` cluster whose `relevance_score >= research_score_threshold` (default 0.60, `migrations/020`), it scrapes the article URL, writes `full_text`/`word_count`/`scrape_status`/`scraped_at`, then calls Claude (`claude-sonnet-4-6`) for `deep_summary`/`key_facts`/`relevance_notes` (`migrations/018`), and finally flips every researched cluster to `researched`.

Today the scrape (`research/scraper.py`) is a single static `httpx` GET + `trafilatura.extract`, attaching a raw `Cookie:` header from `sources.cookies` (`migrations/019`), looked up per feed-source by `_fetch_cookies_by_source` (`research.py:67`, used at lines 145–146, 154). That fails on JS-rendered, consent-walled, anti-bot, and paywalled publishers — anything under `MIN_WORD_COUNT=150` is marked `paywalled` with no text. The live deploy is just `uvicorn api:app` (`railway.json`, no Dockerfile/Chromium), and the `sources.cookies` feature has never been used — so auth starts from scratch.

The goal: scrape inside a **real logged-in browser tab** that navigates to each article link and extracts the body, authed by **per-publisher-domain** session state stored in the DB and **written by the portal/admin app** (read-only here), running **on Railway**. The surrounding control flow — cluster selection, the "skip unless `None`/`failed`" idempotency (`research.py:139`), the Claude call, the counters, the cluster→`researched` transition — is correct and stays as-is. This is fundamentally swapping the *fetch+extract engine* and *re-keying auth from source to domain*.

## Approach

### Recommendation on the reviewer's question: scripted Playwright, not the `browser-use` agent

The reviewer asked whether we're suggesting Playwright over Browser Use, and you confirmed you're happy with Playwright if it's better. **It is, for this job — so this plan commits to scripted Playwright as the engine and drops the `browser-use` agent.** Concretely:

- **Auth is what beats paywalls, not the agent.** A real Chromium tab seeded with a logged-in `storage_state` renders the full article exactly as a subscriber sees it. Once the page is rendered, pulling the body is a trivial, deterministic extraction — there is no decision-making that needs an LLM steering the page.
- **`browser-use` makes one Claude call *per agent step, per article*.** At ~50 articles/day that is real recurring cost, latency, and nondeterminism (the agent can wander, mis-click consent dialogs, or summarise instead of returning raw text) for zero extraction benefit over a scripted `goto → wait → extract`.
- **`browser-use` is built *on* Playwright anyway** — it's the agentic layer over the same engine. Going straight to scripted Playwright removes a moving part while keeping the identical browser and identical auth format. Your Anthropic key stays in play for the part that genuinely needs an LLM: the existing `_call_claude` deep-summary step, unchanged.

So: **engine = headless Chromium via Playwright**, seeded with **Playwright `storage_state` JSON** per domain. If a specific domain later proves un-scrapeable by deterministic extraction, `browser-use` (same Chromium, same `storage_state`) remains an easy per-domain escalation — noted as a future lever, **not implemented now**.

### Build on existing patterns

Keep all of `run_research`'s control flow and replace only the *fetch+extract* call, mirroring how `ingestion/social.py` wraps an external engine behind one normalising function that logs and continues rather than aborting the batch.

- **New module `research/browser_scraper.py`** exposing `scrape_article_with_browser(url, storage_state=None) -> ScrapeResult` — the *same dataclass and contract* as `scrape_article` (`status` ∈ `scraped`/`paywalled`/`failed`, `full_text`/`word_count`/`error`, **never raises**, reuses `MIN_WORD_COUNT`). Internally: launch headless Chromium, create a context with the domain's `storage_state`, `goto(url)` with a load wait, grab rendered HTML (`page.content()`), run it through `trafilatura.extract` (reuse, not reinvent — `scraper.py` already depends on it), normalise to `ScrapeResult` (empty/short body → `paywalled`; any exception → `failed`). Playwright's API is async, so wrap the body in `asyncio.run(...)` inside the sync function, keeping the call site in `research.py` synchronous like `scrape_article`.
- **Per-domain structured auth.** Auth moves from `sources.cookies` (per-source raw header) to a new **per-domain** table. We store a full **Playwright `storage_state` JSON** (cookies *with* `domain`/`path`/`expires`/`httpOnly`/`secure`/`sameSite`, plus per-origin `localStorage`) rather than a replayed `Cookie:` string — this is the format that gives the best paywall survival because it carries the login session, CSRF/consent state, and metering tokens paywalls actually check, and it's exactly what a Playwright context loads natively. In `research.py`, derive each article's registrable domain from `article["url"]` and look up the matching auth row. (Legacy `sources.cookies` is **not** consulted — confirmed unused, starting from scratch.)
- **Stale-auth signal back to the portal.** Subscriber sessions expire at ~7 or 30 days, so the portal needs to know when a capture has gone stale. When an article on a domain that *has* a stored `storage_state` still comes back `paywalled`/`failed`, that's the stale-session symptom. We record a lightweight per-domain freshness signal on the auth table (`last_status` + `last_used_at`) that this app updates and the portal can read to flag "auth looks stale — re-capture". This is a write-back to the auth table; the portal owns the human-facing tag/reminder.
- **Config + deployment** follow `config.py` conventions (`os.getenv` with defaults, commented like the Apify block) and add a Dockerfile so Chromium is present on Railway. Since it's a daily batch with no real-time constraint, we run **sequentially and slowly** — fine for 50 pages across 5–10 domains, and gentle on Railway memory (one tab at a time).

## Files to change

- **`requirements.txt`** — add `playwright` (pinned in the existing `==`/`>=` style). No `browser-use`. Playwright needs Python 3.11+, satisfied by the Dockerfile base image. (`trafilatura` already present and reused.)
- **`config.py`** — add, in the commented Apify-block style:
  - `RESEARCH_USE_BROWSER` (bool, default true) — engine toggle; `false` falls back to the existing static `scrape_article` for safe degrade.
  - `BROWSER_PAGE_TIMEOUT` (ms, per-page nav/extract timeout).
  - `MAX_BROWSER_SCRAPES_PER_RUN` (default comfortably above the ~50 expected, e.g. 100) — bounds a runaway run.
  - `BROWSER_CDP_URL` (default `""`) — optional hosted-Chromium endpoint (Browser Use Cloud / browserless / Steel); when set, connect over CDP instead of launching locally, with **no other code change**. Safety valve if Railway memory proves tight.
  - No new LLM key — `ANTHROPIC_API_KEY` stays as the deep-summary key only.
- **`research/browser_scraper.py`** *(new)* — `scrape_article_with_browser(url, storage_state=None) -> ScrapeResult`. Launch local headless Chromium (container-suitable flags) or `connect_over_cdp(BROWSER_CDP_URL)` when set; create context with `storage_state`; navigate; extract via `trafilatura`; apply the `MIN_WORD_COUNT` paywall check; return `status="failed"` on any exception. **Import `ScrapeResult` and `MIN_WORD_COUNT` from `.scraper`** rather than redefining, so both engines share one contract. Always close the context/browser in a `finally`.
- **`research/research.py`** —
  - Replace `_fetch_cookies_by_source` (lines 67–79) with `_fetch_auth_by_domain(domains) -> dict[str, dict]` (queries the new table) plus a `_registrable_domain(url) -> str` helper.
  - In `run_research`, build the per-batch `{domain: storage_state}` map from the articles' URLs (replacing the `source_id`→cookie map at lines 145–146); drop `source_id`-based cookie selection.
  - In the per-article loop (around lines 151–157): compute the article's domain, look up its `storage_state`, and when `RESEARCH_USE_BROWSER` is on call `scrape_article_with_browser(url, storage_state=...)`, else the existing `scrape_article(url, cookie_string=None)`. Add a `MAX_BROWSER_SCRAPES_PER_RUN` counter that, once exceeded, falls back to the static path (or stops browser scraping) and log how many articles used the browser path.
  - **Stale-auth write-back:** after each scrape on a domain that had a stored `storage_state`, update that domain's `last_status`/`last_used_at` on the auth table; this lets the portal surface a "re-capture" tag. Everything else — DB updates, `_call_claude`, counters, the cluster→`researched` loop (lines 190–193) — is unchanged.
  - Update the module docstring to describe browser-based extraction.
- **`research/scraper.py`** — unchanged behaviour; remains the static/legacy path and the owner of the shared `ScrapeResult`/`MIN_WORD_COUNT`.
- **`migrations/023_add_site_auth.sql`** *(new)* — the per-domain auth table the portal writes and this app reads, following the `migrations/022` convention (mirror live schema, idempotent, documented):
  ```sql
  CREATE TABLE IF NOT EXISTS site_auth (
    domain        text PRIMARY KEY,    -- registrable domain, e.g. 'ft.com'
    storage_state jsonb NOT NULL,      -- Playwright storage_state:
                                       --   {cookies:[{name,value,domain,path,expires,httpOnly,secure,sameSite}],
                                       --    origins:[{origin, localStorage:[{name,value}]}]}
    label         text,                -- human note, e.g. 'FT subscriber session'
    captured_at   timestamptz,         -- when the portal exported this session (for the 7/30-day staleness reminder)
    last_status   text,                -- last scrape outcome on this domain: 'scraped'|'paywalled'|'failed' — written by this app
    last_used_at  timestamptz,         -- when this app last used this session — written by this app
    updated_at    timestamptz DEFAULT now()
  );
  COMMENT ON TABLE site_auth IS
    'Per-publisher-domain login session for the research-stage browser scraper.
     storage_state is a full Playwright storageState() JSON, written by the portal
     (capture from a logged-in browser → context.storage_state()). Keyed by registrable
     domain. The portal writes domain/storage_state/label/captured_at; the data app
     writes last_status/last_used_at so the portal can flag stale (7/30-day) sessions.';
  ```
  This is the exact contract for the portal to match. Optionally add `news_articles.scrape_method text` for auditability (the `scrape_status` CHECK `scraped`/`failed`/`paywalled` is unchanged — confirmed in `migrations/018`).
- **`Dockerfile`** *(new)* + **`railway.json`** — Docker build on a Python 3.11+ slim base that runs `playwright install --with-deps chromium`, keeping `startCommand: uvicorn api:app --host 0.0.0.0 --port $PORT`. Add `"build": { "builder": "DOCKERFILE" }` to `railway.json`. `.dockerignore` already excludes `migrations/`, `*.md`, and `.env*` (all fine — migrations are applied out-of-band, Railway injects env vars). `api.py`'s `POST /run/research` needs no change.

## Verification

- **Helpers:** unit-test `_registrable_domain` (`https://www.ft.com/content/abc` → `ft.com`) and that a sample `storage_state` JSON loads into a Playwright context without error.
- **Local scrape harness:** run `scrape_article_with_browser` against (a) a plain article → `status="scraped"`, sane `word_count`; (b) a known paywalled article *with* valid `storage_state` for its domain → `scraped` with full body; (c) the same with empty/bad auth → `paywalled`; (d) a dead URL → `failed`, no raise.
- **Stage run:** for a research-eligible date, `POST /run/research?date=YYYY-MM-DD` (with `x-api-key`) or `run_research(run_date=...)`; confirm in Supabase that target rows have `full_text`, `scrape_status='scraped'`, `scraped_at`, plus `deep_summary`/`key_facts`/`relevance_notes`, and the clusters moved to `researched`.
- **Stale-auth signal:** with a deliberately empty/expired `storage_state` for a paywalled domain, confirm the run writes `last_status='paywalled'`/`last_used_at` on that `site_auth` row, so the portal can flag it for re-capture.
- **Idempotency:** re-run the same date — already-`scraped` rows skipped, only `failed` rows retried (`research.py:139` preserved).
- **Toggle / cap:** `RESEARCH_USE_BROWSER=false` falls back to the static scraper; `MAX_BROWSER_SCRAPES_PER_RUN` is respected and the browser-path count is logged.
- **Deployment smoke test:** deploy the Docker image to Railway, hit `/health`, run a small research batch; confirm Chromium launches with no missing-shared-library errors and the service stays within its memory limit. If memory is tight, set `BROWSER_CDP_URL` to a hosted browser and re-run with no code change.
