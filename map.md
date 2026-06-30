# Repository map — Curve_Data_Py

- `api.py` — FastAPI app: key-checked `/run/*` stage triggers + `/site-auth/login/*` capture endpoints; starts the scheduler thread.
- `main.py` — CLI entry point (full pipeline `--once`, or a single `--stage`).
- `config.py` — env-driven configuration (Supabase, Anthropic, Apify, browser/Browserbase, site-auth timings).
- `requirements.txt` / `Dockerfile` / `railway.json` — deps and Railway/Docker deployment (Chromium bundled).
- `reset_date.py` — utility to reset/replay a run date.

- `ingestion/` — source fetchers (NewsAPI, Finnhub, RSS, Apify social), the Supabase storage layer, the APScheduler daily job, and competitor scraping.
- `filtering/` — relevance filter that marks ingested articles included/excluded.
- `clustering/` — base story-clustering stage (embeddings → clusters).
- `custom_clustering/` — alternative prompt-driven clustering variant.
- `hybrid_clustering/` — hybrid embedding + LLM clustering variant.
- `scoring/` — scores clusters for editorial relevance.
- `tagging/` — assigns topic/geo tags to scored clusters.
- `research/` — research stage: scrapes full article text and writes deep summaries.
  - `scraper.py` — static httpx + trafilatura scraper (fallback engine).
  - `browser_scraper.py` — Playwright/Chromium scraper (local, CDP, or Browserbase); seeds per-domain `storage_state`.
  - `domains.py` — registrable-domain + host-match helpers; single source of truth for the `site_auth` key.
  - `site_auth.py` — Browserbase headful login capture: launch session, poll auth cookie, upsert `site_auth`.
  - `research.py` — orchestrates the research stage and the stale-auth write-back.
- `briefing/` — generates per-cluster briefs.
- `daily_brief/` — composes the daily editorial brief.

- `migrations/` — timestamped SQL migrations (manually applied); `site_auth` lives in `023_add_site_auth.sql`.
- `plans/` — design notes for shipped features.
- `TASK_DOCS/` — requester-attached task context (not part of the app).
