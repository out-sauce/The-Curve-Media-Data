# Track competitor social posts

The repo confirms the plan's foundations. Below is the revised plan with details now verified against the actual code (latest migration is `022`; `log_source_run`'s second positional arg is `source_category`; the daily pipeline composes stages in `run_daily_pipeline()` via `_run()`; social fetch is reached through `fetcher.fetch_all_sources`, but the competitor stage is independent of it).

## Context

`The-Curve-Media-Data` is a Python pipeline (FastAPI on Railway + APScheduler) that ingests news/social content into Supabase. The "Curve App" is a separate admin UI that reads/writes the same Supabase tables (`sources`, `pipeline_settings`, `source_runs`, …); the pipeline reads its config from those tables at runtime, so a new feature means new tables + a pipeline module + a trigger, with no app-side code coupling beyond the shared DB. (No `LIVE_DB_SCHEMA.sql` is present, so I'm relying on `migrations/` — latest is `022_add_social_sources.sql`.)

An Apify integration already exists in `ingestion/social.py`: `_run_actor()` POSTs to Apify's `run-sync-get-dataset-items` endpoint with `APIFY_TOKEN`, and `_PLATFORMS` configures the Instagram actor (`apify~instagram-api-scraper`) and TikTok actor (`clockworks~tiktok-scraper`) from `config.py`. But that path is purpose-built for the **news** flow: its Instagram input requests `resultsType: "posts"`, each `_normalise_*` extracts only `(post_id, caption, permalink, published_at, image_url)`, the rows become `news_articles`, and **all engagement and follower data is discarded**. It's keyed off `sources` rows where `source_type in ('instagram','tiktok')`. A grep for `competitor`/`follower` returns nothing — there is no competitor concept and none of the metrics this request needs (follower count, likes, comments, views) are captured.

So this is a genuinely separate run: a different account list (competitors, not news sources), a different actor input (profile/follower + per-post engagement, not just captions), and a different storage target (engagement time series, not articles). Per the answered questions: Instagram + TikTok only (default actors `apify~instagram-api-scraper` and `clockworks~tiktok-scraper`); runs **daily after the news run** and also on demand; competitors live in a **new dedicated table**; follower counts kept as a **historical time series**; posts capped at the 10 most recent within a 14-day window; the Curve App reads the new tables directly from Supabase (no read API needed here).

## Approach

Build a parallel "competitor run" that **reuses the existing Apify plumbing** but writes to new tables:

- **Reuse** `_run_actor()` and `_parse_ts()` by extracting them verbatim into a shared `ingestion/apify.py` (with the `APIFY_BASE`/`APIFY_TIMEOUT` constants), and reuse the existing `APIFY_TOKEN`/actor-id config in `config.py`. `social.py` then imports them — no behavioural change to the news flow.
- **New `competitors` table** as the runtime registry the Curve App edits, mirroring the `sources` shape (`name`/`handle`/`url`/`source_type`/`enabled`) so the app's existing source-management UI patterns transfer directly.
- **New module `ingestion/competitors.py`** with `run_competitors()`. Per competitor it: (1) captures follower count, (2) captures the ≤10 most recent posts within 14 days with likes/comments/views/caption, (3) upserts both into new tables. It logs per-account success/error and never aborts the whole run on one failure — the same resilience contract as `_fetch_social_source`. For logging it reuses `log_source_run(name, "competitor", "ok"/"error", count)`, where `"competitor"` lands in the existing `source_runs.source_category` column — no new run-log table needed.
- **New storage functions** in `ingestion/storage.py`: `get_competitors()`, `insert_competitor_snapshot()`, `upsert_competitor_posts()`, following the existing `client.table(...).upsert(..., on_conflict=..., ignore_duplicates=...)` pattern.
- **New trigger**: `POST /run/competitors` in `api.py` (mirroring `/run/scan`, guarded by `_check_key`), plus appending a `competitors` stage to `run_daily_pipeline()` in `scheduler.py` so it runs daily after `daily-brief`.

**Apify input strategy.** For Instagram, switch the input to `resultsType: "details"` on `apify~instagram-api-scraper` — its profile result carries `followersCount` (plus `followsCount`, `postsCount`) and a `latestPosts` array with per-post `likesCount`/`commentsCount`/`videoViewCount`, giving follower + recent posts in one call. For TikTok, `clockworks~tiktok-scraper` returns `authorMeta.fans` plus per-video `diggCount`/`commentCount`/`playCount`. Exact field names will be confirmed against a live actor run during build; if Instagram's `latestPosts` proves to omit engagement, fall back to a second `resultsType:"posts"` call per account for the posts. Field mapping mirrors the existing `_normalise_*` functions. The 2-week/last-10 cap is applied **client-side** (`cutoff = now - 14d` filter, then sort by published_at desc and slice to 10), independent of any actor `resultsLimit`.

**Snapshotting model.** Insert one **follower snapshot row per competitor per run** (append-only time series → growth charts). Store **posts keyed by `guid = '{platform}:{post_id}'`** and re-upsert engagement on each run (`on_conflict='guid'`, `ignore_duplicates=False`) so like/comment/view counts refresh as posts mature.

## Files to change

- **`migrations/023_create_competitors.sql`** (new) — `competitors` table: `id bigserial pk`, `name text not null unique`, `handle text`, `url text` (display profile), `source_type text not null check (source_type in ('instagram','tiktok'))`, `enabled boolean not null default true`, `created_at timestamptz not null default now()`. Indexes on `source_type` and `enabled`. `create table if not exists` per `007_create_sources.sql`. The Curve App writes this list.
- **`migrations/024_create_competitor_data.sql`** (new) — two tables:
  - `competitor_snapshots`: `id`, `competitor_id bigint references competitors(id)`, `handle`, `platform`, `follower_count`, `following_count` (nullable), `post_count` (nullable), `captured_at timestamptz not null default now()`. Index on `(competitor_id, captured_at)`. Append-only (no unique-per-run constraint).
  - `competitor_posts`: `id`, `competitor_id bigint references competitors(id)`, `guid text unique` (`{platform}:{post_id}`), `platform`, `post_url`, `caption text`, `like_count`, `comment_count`, `view_count`, `published_at timestamptz`, `fetched_at timestamptz`. Indexes on `competitor_id` and `published_at`.
- **`ingestion/apify.py`** (new) — shared `run_actor(actor_id, run_input)` and `parse_ts(value)`, moved verbatim from `social.py` along with `APIFY_BASE`/`APIFY_TIMEOUT`.
- **`ingestion/social.py`** — replace the local `_run_actor`/`_parse_ts` definitions with imports from `ingestion/apify.py`. No other change to the news flow.
- **`ingestion/competitors.py`** (new) — `run_competitors()`: guards on `APIFY_TOKEN` (like `fetch_social`), reads `get_competitors()`, and per account builds the platform-specific actor input (IG `resultsType:"details"`; TikTok profile scrape), runs the actor via `run_actor`, normalises the profile follower count and per-post engagement metrics, applies the 14-day cutoff + 10-post cap, and calls the new storage upserts. Per-account `try/except` + `log_source_run(name, "competitor", "ok"/"error", count, err)`. A `_PLATFORMS`-style dict mirrors `social.py` with `normalise_profile`/`normalise_post`/`input` per platform.
- **`ingestion/storage.py`** — add `get_competitors()` (enabled rows from `competitors`, `select("id, name, handle, url, source_type, enabled")`), `insert_competitor_snapshot(row)` (plain `insert`), `upsert_competitor_posts(rows)` (`on_conflict='guid'`, `ignore_duplicates=False`).
- **`config.py`** — add `COMPETITOR_POST_LIMIT` (default 10) and `COMPETITOR_LOOKBACK_DAYS` (default 14); reuse existing `APIFY_TOKEN`/`APIFY_INSTAGRAM_ACTOR`/`APIFY_TIKTOK_ACTOR`.
- **`api.py`** — add `@app.post("/run/competitors")` calling `run_competitors` as a `background_tasks.add_task`, guarded by `_check_key`.
- **`ingestion/scheduler.py`** — import `run_competitors` and append `_run("competitors", run_competitors)` at the end of `run_daily_pipeline()` (after `daily-brief`), so it runs daily after the news run.
- **`main.py`** — add `from ingestion.competitors import run_competitors` and `"competitors": run_competitors` to `STAGES` (not in `DATE_STAGES`, since it takes no `run_date`), enabling `python -m main --stage competitors`.

## Verification

- **Migrations**: apply `023`/`024` to a Supabase branch; `\d competitor_posts` / `\d competitor_snapshots` show expected columns, FKs, and the unique `guid` constraint.
- **Config**: insert 1–2 known competitor handles (one IG, one TikTok) into `competitors`; confirm `get_competitors()` returns them.
- **End-to-end**: with `APIFY_TOKEN` set, trigger `POST /run/competitors` (with `x-api-key`) or `python -m main --stage competitors`. Logs show `follower_count=X / N posts` per account; the Apify dashboard shows the two actor runs.
- **Data checks**: `competitor_snapshots` has a fresh row per competitor with non-null `follower_count`; `competitor_posts` has ≤10 rows per competitor, all `published_at` within 14 days, with populated `like_count`/`comment_count`/`view_count`/`caption`. Re-run and confirm idempotency: a new snapshot row appended, posts updated in place (no dup `guid`).
- **Resilience**: set one handle to a bogus value; the run logs an `error` `source_runs` row (category `competitor`) for that account and still completes the others.
- **Isolation**: `news_articles` and the daily news pipeline are untouched; confirm the competitor stage runs *after* `daily-brief` in `run_daily_pipeline`.

All material product decisions are settled by the answered questions; the one remaining unknown (exact Apify field names / whether IG `details` returns per-post engagement inline) is an implementation detail to confirm against a live actor run, not a decision that needs your input.
