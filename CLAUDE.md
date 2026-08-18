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
- **Zernio** (`ingestion/zernio.py`, `ingestion/inbox.py`) for The Curve's OWN channels:
  owner-only Instagram insights, audience demographics, story insights, and the
  comments/DM mirror. Replaced Outstand. Competitor tracking stays on Apify — Zernio can
  only read accounts we've connected. The Admin app is a second Zernio client, for
  publishing.
- Deployed on **Railway** via the `Dockerfile` (bundles Chromium). **Single replica**
  required — the site-auth flow keeps an in-process session registry, and the Zernio
  inbox webhook hands work to in-process background tasks.

## Run & test

- Install deps: `pip install -r requirements.txt` then `playwright install --with-deps chromium`.
- Config comes from env / `.env.local` (see `config.py`). Required: `NEXT_PUBLIC_SUPABASE_URL`,
  `SUPABASE_SERVICE_ROLE_KEY`. Optional: `PIPELINE_API_KEY` (guards every endpoint),
  `ANTHROPIC_API_KEY`, Apify/NewsAPI/Finnhub keys, `BROWSERBASE_API_KEY` +
  `BROWSERBASE_PROJECT_ID` (site-auth capture), `ZERNIO_API_KEY` (own-channel analytics
  + the comments/DM inbox), `ZERNIO_WEBHOOK_SECRET` + `PUBLIC_BASE_URL` (the inbox
  webhook — the receiver fails closed without the secret, and registration needs the
  deployed URL).
- API: `uvicorn api:app --reload`. CLI: `python main.py --once` (full run) or
  `python main.py --stage <ingest|filter|cluster|score|competitors|zernio|zernio-daily>
  [--date YYYY-MM-DD]`.
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

- **Outstand → Zernio (migrations 037/038/039).** `ingestion/outstand.py` is GONE,
  replaced by `ingestion/zernio.py` (analytics) + `ingestion/inbox.py` (comments/DMs).
  The move was for the **comments and DM APIs Outstand simply did not have**; Zernio
  also covers everything Outstand did, plus audience demographics, story insights, a
  daily follower series and per-post Reels watch time. Every `OUTSTAND_*` config key,
  `POST /run/outstand`, the `outstand` stage and the `outstand_hourly` job are deleted.
  **Read every note below about Outstand as history** — the machinery is gone, but the
  column semantics it established are all still live.
  - **The whole import/watermark/billing subsystem is gone.** Outstand billed per
    imported post, so it needed `social_accounts.outstand_last_imported_at` and a
    bounded poll loop. Zernio background-syncs each connected account's external posts
    itself (~90 min, ~12 months retained) and analytics are plain GETs, so there is
    nothing to meter. The column is left in place (the Admin owns that table) but
    nothing reads or writes it. `get_outstand_connected_accounts` →
    `get_zernio_connected_accounts`, minus the watermark.
  - **Row identity is unchanged and that is the load-bearing assumption.**
    Zernio's `platforms[].platformPostId` is the platform's own media id — the same
    18-digit Instagram id Outstand wrote to `content_stats.post_id` — so existing rows
    continue rather than duplicating. A post with **no** `platformPostId` is SKIPPED,
    never keyed on Zernio's internal `_id`: a row keyed on a vendor id would never
    reconcile with the one already there. **Verify this live before cutover** — if a
    second row appears for a post that already had one, stop.
  - **`engagement_rate` is computed, not taken from the vendor.** Checked against the
    live table: all 100 Outstand-written values encode interactions/**reach**×100
    exactly. Zernio's `engagementRate` divides by the *first non-zero* of impressions,
    reach, views — so its basis can change from post to post with nothing in the row to
    say which was used — and arrives rounded to 2dp. We compute interactions/reach×100
    ourselves, which keeps the column meaning one thing either side of the swap and
    makes `engagement_rate == engagement_on_reach × 100` an invariant. The vendor value
    is kept in `platform_specific.vendor_engagement_rate` for reconciliation.
  - **Zero means ABSENT for half the metrics** (`_nz` in zernio.py). Zernio returns
    `0`, not null, for a metric it could not obtain — follows on a reel, watch time on
    a non-video, any insight inside Meta's ~24h delay, anything on an unsynced post —
    and `upsert_self_content_stats` only skips `None`. So a 0 would be *written*, read
    as "this post reached nobody", and overwrite the good value from the previous hour.
    `views/reach/impressions/clicks/follows/watch-times/duration` map 0 → None.
    `likes/comments/shares/saves` deliberately do NOT: a real post can honestly have
    zero, and mapping those to None would make a genuine zero unwritable forever.
    Related guard: a post whose `syncStatus` is `pending`/`unavailable` is skipped
    entirely (its analytics block is all zeros) — the analogue of Outstand's
    `token_expired` check.
  - **Instagram stories are `platform = 'instagram_story'`** (migration 038 widens the
    `content_stats_platform_check`, which would otherwise reject them with a 23514).
    Folding them into `'instagram'` would inject ~10 rows/day into every query that
    filters on that value and silently change what "our Instagram performance" means.
    The sweep runs hourly (Meta serves a story for 24h) and has a **settle pass**: a
    story's numbers climb until it expires, so the last live poll is systematically an
    undercount, and Zernio serves the final figures afterwards from Meta's story
    webhook (`source: "cached"`). `sticker_taps` is **never written** — Meta exposes no
    such metric for stories (`navigation` is taps/exits/swipes, a different thing) and
    that column is hand-entered by the operator; a 0 there would destroy typed-in data.
    No `engagement_*` rates either: a story's interactions-over-views is a different
    question from a feed post's.
  - **Demographics land in the Admin's existing `audience_demographics`** — its shape
    already matched exactly. Gender is normalised `M/F/U → male/female/unknown`: the
    table's natural key includes `bucket`, so writing "F" where the rest of the system
    writes "female" would split one audience across two buckets permanently. Country is
    ISO-2 verbatim (matches the existing rows); city keeps its region suffix. Written as
    a keyed upsert, never delete-then-insert, because the table also holds hand-entered
    and podcast rows. Only `follower_demographics` — there is no column distinguishing
    it from the engaged audience, so writing both would collide on the key.
  - **Follower backfill is gap-fill only.** `/v1/accounts/follower-stats` gives the
    daily series and the current count and the avatar and the lifetime post count in one
    call (its account entries extend the full SocialAccount schema), so it replaces both
    of Outstand's account calls. The Instagram series is ~108 rows since 2021 — largely
    monthly, hand-entered — and it is the denominator behind every `engagement_audience`
    value, so the backfill uses `insert_only` and never overwrites an existing day.
    **Filling gaps moves `engagement_audience` on posts published in them**, and only
    posts still inside the 90-day window get recomputed, so the column is briefly
    inconsistent across that boundary. `_followers_at`'s cache is now cleared at the
    start of every run — the Outstand version loaded it once and froze for the life of
    the process.
  - **New per-post columns (037):** `follows`, `avg_watch_time_ms`,
    `total_watch_time_ms`. `impressions`, `clicks` and `duration_sec` already existed
    and are populated for the first time. Watch times are **milliseconds** (Meta's unit).
    Per the standing gotcha, `_content_stats_column_set()` caches the column list per
    process, so 037 must land before the deploy restarts the service or all three are
    silently filtered out of every write.
  - Latent bug fixed on the way: the account-matching filter used `and` where `or` was
    meant (`outstand.py:244`), which accepted an entry belonging to a *different*
    account of the same network. Harmless with one connected account; a foot-gun the
    moment there are two. The replacement matches on `accountId` first and only falls
    back to platform when the entry carries no account id to contradict it.

- **The comments + DM inbox** (`ingestion/inbox.py`, migration 039 — five tables).
  New capability, not a port. **The webhook is the fast path; the sweep is the correct
  one**, and neither is redundant: Meta replays up to 500 pre-connect conversations per
  account *in the background, emitting no webhooks*, keeping each thread's original
  timestamp so replayed threads sort into date order rather than to the top — a single
  incremental pass provably cannot see them. Dead-lettered events are gone after ~51h.
  And a third party editing/hiding/deleting/liking a comment is never evented at all.
  So nothing depends on webhook completeness: `*/15` incremental sweep, exhaustive
  `full=True` pass nightly at 04:30, and `POST /run/inbox-sweep?full=true` after
  connecting an account.
  `POST /webhooks/zernio` deviates from this repo's endpoint conventions on purpose and
  says so in place: `async def` (the HMAC needs the RAW body), **no `x-api-key`** (the
  signature is the authentication, and it fails closed when the secret is unset), and it
  returns **200 for everything authenticated** including unknown events and unparseable
  bodies — a non-2xx costs 7 retries over ~51h and **10 consecutive failures make Zernio
  disable the subscription**, which would silently stop every comment and DM. The ack
  budget is 5 seconds, so the only synchronous work is the signature check and one
  ledger insert; `inbox_webhook_events.event_id` (the envelope uuid) is the PK and
  *is* the at-least-once dedupe. Note the signature format differs from Outstand's:
  a bare lowercase hex digest with **no `sha256=` prefix** — the old verifier *required*
  one, so copying it would reject every delivery.
  Ownership split: this repo writes every row; the Admin owns
  `inbox_conversations.{is_read,read_at,read_by}`, `inbox_comments.{handled_at,
  handled_by}` and its own optimistic `source='admin'` rows. The upserts here achieve
  that by never naming those columns. Comments link to our content via
  `inbox_posts.platform_post_id` → `content_stats.post_id` → `calendar_item_id`,
  resolved at write time, **fill-only-if-null and retried every sweep** (a post
  published minutes ago has no stats row yet). `parent_comment_id` is a platform id
  **string, not a uuid FK**, because a reply's webhook can arrive before its parent is
  mirrored. **TikTok has no comment API and no DMs in Zernio at all** — one of the four
  brand channels is simply not covered, and the UI says so rather than showing an empty
  list.
  **Three defects found on the first live Instagram sync (2026-08-18) and fixed.**
  All three were silent — the sweep logged `ok` throughout.
  - **`/v1/inbox/conversations/{id}/messages` REQUIRES `accountId`.** Without it every
    call is a 400 `missing_required_field`, caught by `_sweep_messages`' own
    `except` and logged as a warning, so NOT ONE message was mirrored while the
    conversation still rendered a summary line — `inbox_conversations.last_message`
    comes from the conversation LISTING, not from a message row. A thread with zero
    messages is therefore always a possible read.
  - **The REST endpoints use Meta's field names; the webhook envelope does not.**
    Threads return `message`/`from{id,name,username,isOwner}`/`parentId`; the webhook
    sends `text`/`author`/`parentCommentId`, and messages come back flat
    (`senderId`/`senderName`) rather than nesting a `sender`. The normalisers read only
    the webhook shape, so all 359 first-sync comments stored a NULL body, NULL author
    and `author_is_owner = false` — our own replies included. `_comment_row`/
    `_message_row` now accept **both** shapes; keep it that way. The thread payload's
    explicit `isOwner` is preferred over comparing usernames.
  - **The comment-thread endpoint paginates on `pagination.cursor`, NOT
    `nextCursor`** — every other listing here uses `nextCursor`, so reading that key
    returned None while `hasMore` stayed true and capped each thread at one page
    (50-of-184, 50-of-167, 50-of-94 on three posts). Both keys are read now. The page
    limit counts **top-level comments only** — replies arrive nested inside their parent
    and don't consume it — so a page count can never be compared with `commentCount`.
  Backfill: 359 → 654 comments, bodies and authors populated, DMs mirrored.
  **Selection rule, for reference:** comments come from posts whose **publish date** is
  inside `INBOX_SWEEP_LOOKBACK_DAYS` (30) with `minComments >= 1`, organic only
  (`isAd` skipped), platform in instagram/facebook/youtube/linkedin. `since` filters the
  POST, not the comment — a new comment on a 31-day-old post is invisible to the sweep.
  Conversations have no date filter at all; the lookback only decides when to stop
  paging.
  **Sweeps are serialised, and the writes are batched** (added once Meta's replay
  landed 499 threads / 5,101 messages and the naive per-row writers fell over).
  - **`run_inbox_sweep` holds a process-local `threading.Lock`** and returns early if a
    pass is already running. A full pass over 499 threads takes far longer than the
    `*/15` tick, so runs used to stack: duplicated fetches, doubled write load, and
    Supabase refusing connections — `Could not upsert inbox message: Server
    disconnected` — which the never-raise discipline swallowed as a warning while
    silently dropping rows. One thread was left with 0 of its 7 messages that way. A
    process-local lock is only sufficient because the service is **single replica**;
    that must become a Postgres advisory lock if it is ever scaled out.
  - **`inbox_comments` batches through a real PostgREST upsert; `inbox_messages`
    CANNOT.** `inbox_comments_key` is a FULL unique index so ON CONFLICT can infer it.
    Both `inbox_messages` indexes (and both `inbox_conversations` ones) are **PARTIAL**
    (`WHERE ... IS NOT NULL`), and Postgres refuses to infer a partial index unless the
    statement repeats its predicate — which PostgREST cannot emit (verified: `42P10`).
    So messages use a hand-rolled grouped SELECT → bulk INSERT → UPDATE-only-if-changed.
    **Do not "tidy" the two paths into one shape** — the difference is forced by the
    schema.
  - **Batches are bucketed by exact key set** (`_key_buckets`). PostgREST rejects a
    batch whose objects don't share keys, and the obvious fix — padding rows to a common
    key set — would be destructive here: these rows are built with skip-None, so an
    ABSENT key means "leave that column alone", not "write NULL".
  - Only `body`/`delivery_status`/`is_edited`/`is_deleted`/`sender_name`/`attachments`
    are compared to decide whether a mirrored message needs rewriting; everything else
    about a message is immutable. Re-sweeping a mirrored inbox costs a few SELECTs and
    zero writes. Measured on a 184-comment thread: 368 round trips → 9.
  - `upsert_inbox_conversation` takes a `known_id`; the sweep already holds every uuid
    from `get_inbox_conversation_state()`, so resolving each thread again was ~1,500
    wasted SELECTs per full pass.
  **DM history reaches back to 2021-03-14** — far deeper than the comment sweep's 30
  days — and **~half of all messages have no `body`** (attachment-only story replies and
  shared posts, which is normal, not the field-mapping bug above).

- **A true engagement rate on reach** (migration 036, nullable `engagement_on_reach`).
  Despite its name, `engagement_reach` is interactions / **views** — migration 026's
  proxy, chosen when reach wasn't available to us — and it stayed that way even after
  Outstand started handing us real `reach` in the same payload. New
  `engagement_on_reach` = interactions / reach, a 0-1 fraction like its siblings,
  written **only by `ingestion/outstand.py`**: reach is an owner-only Meta insight, so
  the Apify paths can't produce it and Meta doesn't attribute a collab post published
  from a guest's account to our account either. `engagement_reach` is deliberately
  **left as-is** rather than redefined — it's the only engagement figure every source
  can produce, so it remains the cross-source proxy (guest/collab posts and the admin
  bulk imports have no reach and would otherwise go blank). Prefer
  `engagement_on_reach` in the UI, fall back to `engagement_reach`.
  **The two are different questions, not two estimates of one** — `reach` counts unique
  accounts that saw the post, `views` counts plays including replays (~2.1 plays per
  reached account across our IG posts), so the reach-based rate runs roughly **2x** the
  views-based one (97 IG posts backfilled 2026-08-13: median 1.82%, max 10.98%, vs 1.14%
  average on views). Never plot them on one axis or read one as the other.
  **`engagement_audience` is now populated by Outstand too** (it previously always wrote
  None, so no post got an audience rate at all once the Apify self-IG channel was
  retired). The denominator is **followers-at-time**, from a new
  `get_follower_snapshot_history()` + `_followers_at()` pair reading our own
  `follower_snapshots` series (45,280 in April vs 48,691 in August — today's count would
  understate every older post). So both intended engagement metrics — on reach and on
  audience — are live for all 97 Outstand-covered posts.
  A `reach >= interactions` guard gates the write, since an account can't interact
  without being reached: the backfill initially stamped **125%** on a YouTube row whose
  `reach` was **8** against 201 views and 10 interactions. That value was *not* from a
  pipeline writer — `reach` on non-Outstand rows is admin-entered and not necessarily
  Meta's reach at all, so never assume a non-NULL `reach` is a credible denominator.
  Per the usual gotcha, `_content_stats_column_set()` caches the column list per
  process, so 036 must be applied before (or with) the deploy that restarts the service.

- **`likesCount = -1` means Instagram HID the like count** — not an import artefact.
  `_to_int` (`ingestion/competitors.py`) passed it through, making interactions negative
  and printing a **negative engagement rate**; it now maps negatives to None (every
  call site is a non-negative count). Note skip-None cannot *clear* an already-stored
  -1, so the two affected April rows were nulled by hand along with their engagement
  columns — comments-only engagement on a hidden-likes post reads as real but isn't.
  Related: 12 `youtube_shorts` rows have `likes` NULL with engagement computed from
  comments alone, so those rates are understated; left as-is deliberately (gating
  engagement on a known like count would blank all 12).

- **Collabs are common, and roughly a third are published from the partner's account.**
  Ownership was checked live on 41 IG rows (2026-08-13): 16 carry co-authors, of which
  **7 are owned by another account** (`nzhviva`, `ciara___anne`, `hannahkoumakis` ×3,
  `francescooknz`, `shityoushouldcareabout`) — several of them tagged
  `social_kind = 'own'` in the calendar. Consequences for reporting: those rows'
  likes/views belong to the partner's audience (6 guest-tagged rows alone were 8.3% of
  all IG likes), and a **sponsored** collab is still a paid deliverable, so it belongs
  in the sponsor report even though it must not count toward own-channel
  engagement-vs-followers. Collab posts also enter The Curve's own `competitor_posts`
  rows and therefore its brand `instagram_engagement_rate`, which is NOT gated —
  a 2,276-like partner-audience post inflated that figure while inside the 14-day window.

- **IG collab posts: plays-only `views`, and no `engagement_audience`.** Instagram
  **collab (co-author) posts appear on every co-author's grid**, so The Curve's `is_self`
  profile scrape returns posts *published from someone else's account* and writes
  `content_stats` rows for them. Live 2026-08-13, 5 of the 7 IG rows that carried
  `engagement_audience` were collabs owned by guests (`hannahkoumakis`, `ciara___anne`,
  `nzhviva`), several of them tagged `social_kind = 'own'` in the calendar — so
  `social_kind` is **not** a reliable "is this ours" signal; post ownership is.
  Two fixes in `ingestion/competitors.py`:
  1. **`views` must be the plays figure.** `videoViewCount` is the legacy 3-second-view
     metric and is orders of magnitude below the `videoPlayCount` figure Instagram now
     labels "views" (one reel: 928 vs 62557, against 471 likes). The old
     `videoViewCount or videoPlayCount` preference stored views *below* likes and pushed
     `engagement_rate`/`engagement_reach` (interactions ÷ views) over 100%.
     `_ig_view_count` now takes the **larger** of the two. Crucially the profile
     scrape's **`latestPosts` items carry `videoViewCount` only — no `videoPlayCount`**
     (verified live), so a `view_count_is_plays` flag rides along and the `is_self`
     content_stats path **omits `views` entirely** when the number is the legacy metric;
     skip-None then preserves Outstand's authoritative hourly value, and guest/collab
     posts get a real figure from the single-post guest sweep, whose payload does include
     `videoPlayCount`. `competitor_posts` still stores the legacy number, leaving the
     cards unchanged. Four rows were re-scraped by URL to correct them (153→24277,
     406→62557, 1251→28876, 607→39014).
  2. **`engagement_audience` is only written for posts we published.** It divides by OUR
     follower count, which means nothing for a post on the owner's account that
     Instagram delivered to the union of the co-authors' audiences — and no follower
     count for the owner is available: the post payload has **no follower field at all**
     (only `ownerUsername`/`ownerId` + a `coauthorProducers` list of usernames), so a
     guest denominator would need a second details-mode profile run, and
     followers-at-time can never be reconstructed retrospectively anyway. The
     `published_by_us` gate (owner == `handle`, no co-authors; owner `None` → assume ours,
     preserving TikTok/LinkedIn/YouTube behaviour) leaves it None otherwise, and the 5
     historical values were nulled by hand. `latestPosts` exposes `ownerUsername` but
     **not** `coauthorProducers`, so a co-authored post of *ours* is indistinguishable
     from a plain one in the profile scrape — ownership is the signal that always works.
     **`engagement_reach` (interactions ÷ plays) is the engagement rate to use for
     guest/collab posts**; it needs no follower count and is comparable with own posts.
  Also note `content_stats.views` has **three** writers with different provenance —
  Outstand (authoritative owner insights, also sets `reach`), this Apify path, and
  admin-side bulk imports (a 2026-08-01 batch, some rows with `likes = -1` and no
  `post_url`) — so `reach IS NULL` is a rough "not Outstand-enriched" filter, not proof a
  row came from Apify. Outstand does **not** carry collab posts (checked across
  2026-07-08→07-25): they are published from the guest's account, so Meta never
  attributes them to our IG account's insights, and Outstand's `post_id` is an 18-digit
  media id rather than the Apify format — the same physical post from both sources would
  land as two rows.

- **Guest-post stats — one Apify run per post URL** (`ingestion/guest_posts.py`).
  The admin's Content Calendar can hold posts published from a GUEST's own account
  (`content_calendar_items.social_kind = 'guest'`, admin-side migration); the guest
  may or may not be a tracked competitor, so no profile scrape covers them. New
  `POST /run/guest-post-stats?id=<calendar_item_id>` (id = the drawer's "Refresh
  stats"; omitted = sweep every guest item with a `post_url` whose `publish_date`
  is within `GUEST_POST_LOOKBACK_DAYS`, default 90) — the sweep also runs daily
  after the competitors stage (`scheduler.run_daily_pipeline`). Single-post actor
  inputs, all verified live 2026-08-12: IG `apify~instagram-api-scraper`
  `{"directUrls":[url],"resultsType":"posts"}` (post items directly — the
  `latestPosts` normalisers apply); TikTok `clockworks~tiktok-scraper`
  `{"postURLs":[url]}`; YouTube `streamers~youtube-scraper` `{"startUrls":
  [{"url":url}]}` (a /shorts/ URL works and stays platform `youtube`); LinkedIn
  needs a DIFFERENT actor from the profile run — new `APIFY_LINKEDIN_POST_ACTOR`
  (default `apimaestro~linkedin-post-detail`, `{"post_urls":[url]}`, its own
  normaliser: `post.id`/`stats.total_reactions`/`created_at.timestamp` in **ms**).
  Rows reuse the competitor normalisers + `store_competitor_image` (same
  deterministic thumbnail path) and are written via `upsert_self_content_stats`,
  which now accepts `calendar_item_id` per row — set on insert, **fill-only-if-null
  on update** (an existing admin link is never stolen; rows without the key behave
  exactly as before). The link must travel with the write because the admin's URL
  auto-linking only runs on item saves. `engagement_audience` stays None — a lone
  post carries no follower count. Failures follow the competitor pattern: per-item
  `logger.warning` + continue, one `source_runs` row (category `guest_post`) per run.

- **`content_stats` rows now carry a post thumbnail** (migration 035, nullable
  `thumbnail_url`). The Outstand hourly run persists each post's first media image
  (`containers[0].media[0].url` — the only media fields are `url`/`filename`, no
  separate thumbnail) into the `competitor-thumbnails` bucket at the **same**
  deterministic path the competitor card uses (`posts/{platform}_{post_id}.jpg`) — one
  stored object backs both `competitor_posts.thumbnail_url` and
  `content_stats.thumbnail_url`. `_attach_thumbnails` skips posts whose content_stats
  row already has a thumbnail and `_write_competitor_card` reuses the stamped URL
  instead of downloading again. **Outstand's media URLs are signed IG CDN links that
  expire within days** (live-verified 2026-08-11: a 90-day backfill 403'd on all but
  the 2 newest posts), so thumbnails are **forward-only** — captured while a post is
  fresh; posts older than `_THUMBNAIL_FETCH_MAX_AGE_DAYS` (14) are never attempted
  since their URLs are guaranteed dead and hourly retries would be pure churn. For
  reels the media URL may be the **mp4 itself**; `store_competitor_image` now rejects
  non-image content-types (video bytes under a .jpg path render as a broken image) and
  returns None. Historical gap: 38 pre-Outstand rows were backfilled one-off (2026-08-11
  SQL, joining `competitor_posts.thumbnail_url` on trimmed `post_url` — old Apify rows
  share URLs but not post-id format); ~100 older rows have no recoverable image short
  of an Apify re-scrape. The Apify self flow (`competitors.py`) passes the card-window
  posts' persisted thumbnails into its content_stats rows too (skip-None never blanks
  a stored value). Pre-035 the field is filtered out by `_content_stats_column_set()`
  — but the column set is **cached per process**, so apply the migration before (or
  with) the deploy that restarts the service.

- **`follower_snapshots` now carries Outstand's account-level engagement aggregate.**
  The hourly Outstand run fetched `GET /social-accounts/{id}/metrics` and threw the
  `engagement` block away; migration 034 adds nullable `views_30d / likes_30d /
  comments_30d / shares_30d / saves_30d / reach_30d / accounts_engaged_30d /
  total_interactions_30d` columns and `upsert_follower_snapshot` writes them when the
  caller passes `engagement_30d` (only `ingestion/outstand.py` does; the Apify flow in
  `competitors.py` passes nothing and never blanks Outstand-written values — omitted
  keys are simply not written). **These are trailing ~30-day rolling-window totals**
  (the payload's `period.since/until`), not daily activity: adjacent daily rows overlap
  by ~29 days, so charting the trend is fine but never SUM rows or diff two days to get
  a daily figure. Non-Outstand channels (Apify TikTok/LinkedIn/YouTube snapshots) and
  pre-034 rows leave them NULL. Outstand has **no audience-demographics endpoint**
  (live-probed 2026-08-10: `/audience`/`/insights`/`/demographics` all 404) — age/
  gender/geo would need a direct Meta Graph API integration, not Outstand.

- **Story continuation — a running story is now ONE cluster row that grows.** The old
  "week continuity" pass matched today's articles against the *names* of clusters created
  earlier this week and then minted a **brand-new `story_clusters` row per day** sharing
  that name, correlated only by the lowercase `weekly_story` slug. `clustering/cluster.py`
  now **extends the existing row in place**: `_fetch_open_clusters` offers Claude the open
  stories from a **rolling 7 days** (`article_count >= 2`; plus `article_count = 1`
  singletons from the last **3** days only — a full week of headline-named singletons is
  what snowballed the old prompt to ~1000 "week stories" over 2026-07-21→23). Candidates
  are gated at `relevance_score >= CANDIDATE_MIN_SCORE` (0.5) and **ordered by score, not
  by date**, with separate caps (150 multi + 50 singleton): the window holds ~480
  multi-article clusters and ~700 singletons, so a date-ordered cap spends its whole
  budget on the most recent day and the 7-day window becomes fiction — score-ordering
  makes trimming cost the least relevant story instead of the oldest one, and everything
  is already scored by the time a prior day's cluster is a candidate.
  `_call_continuation` returns a **1-based candidate index** (not the uuid — cheaper and a
  mangled uuid would silently drop the match) plus article_ids, under its own
  `_CONTINUATION_SCHEMA`; matches are applied in index order, so when Claude assigns one
  article to two stories (**it does**, despite the prompt) the higher-scoring story wins
  deterministically, and ids Claude invents (**it does that too**) are dropped by the
  `id_lookup` filter. `_extend_cluster` **claims the cluster first** (a single UPDATE
  re-filtered on `cluster_status in (pending,scored,researched)` + `published_at IS NULL`
  + `ready_for_content = false`, closing the race where an operator promotes the story
  during the Claude call), then repoints the articles and recomputes
  `article_count` with a **`count="exact"` COUNT**, never a read-then-increment.
  **`story_clusters.date` is no longer immutable** — an extended story's date is bumped
  forward (never backwards: `max(candidate.date, target_date)`, so a `--date` backfill
  can't yank a live story out of today's view), which is what makes every downstream
  `.eq("date", run_date)` stage pick it up again unchanged; `created_at` is the origin
  day. The cluster is reset to `pending`, so score → tag → research → brief all re-run —
  the **only** deliberate demotion in the codebase (`_advance_clusters_to_researched`
  still never demotes). Research only scrapes the new articles (its `scrape_status IS
  NULL/'failed'` filter already handles that). `briefed`/`archived`/`ready_for_content`
  clusters are never extended, so the content studio's manual output is untouchable.
  **The live `cluster_status` enum is `(pending, scored, researched, ready_for_content,
  archived, briefed)`** — migration 021's header still lists `scoring`/`accepted`/
  `rejected`/`published` but the type was since recreated without them, and passing a
  non-existent label makes PostgREST reject the **whole** query, so never add one to a
  status filter. (`custom_clustering/custom_cluster.py` still writes `'rejected'` and is
  therefore broken if ever revived.) `ready_for_content` exists both as a status and as a
  separate boolean column; both block extension.
  **Re-briefing** is driven by a new `last_article_at` column (migration 031, stamped on
  every create *and* extend): `run_briefing` skips a cluster only when its brief is
  **current** (`_brief_is_current` — `briefed_at >= last_article_at`; NULL/unparseable
  = current, i.e. pre-031 behaviour), so an extended story re-briefs while an unchanged
  one keeps the idempotent skip. Two pre-existing bugs became load-bearing and are fixed
  in the same change: `_fetch_included_articles` now filters `cluster_id IS NULL` (without
  it a second run re-extends today's own clusters and mints duplicate rows that leave
  zero-article ghosts) and **paginates** (the 1000-row PostgREST cap was silently dropping
  the tail of big days); and `reset_date.py` — whose article-side UPDATE filtered
  `status in ('accepted','briefed','published')` and therefore **matched zero rows**,
  since clustering leaves articles at `included` — now drives off `cluster_id` and splits
  the date's clusters into **born** (created that day → detach all articles, delete) vs
  **extended** (grew into that day → detach only that day's articles, recount, roll `date`
  back to `created_at`, delete only if empty), with a `--dry-run`. Deleting an extended
  cluster by date would otherwise orphan the earlier days' articles forever.
  `weekly_story` is **deprecated** — still written as `lower(name)` for Admin back-compat,
  but the unbounded name-keyed backfill UPDATE is gone and nothing should group by it.
  **Admin app (separate repo) must be checked:** any query listing a story's articles by a
  `fetched_at` window instead of `.eq('cluster_id', …)` will now under-report, `date` and
  `created_at` diverge for running stories, and per-day `article_count` no longer sums to
  the day's ingest.

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
  automated scrape returns **`bot_wall`** it **auto-demotes** the domain to
  `manual` (`_demote_domain`, `DEMOTE_STATUSES`), so a hostile publisher is hit with the
  login **at most once**. `paywalled` does **not** demote — it means we lack a subscription
  (or the session lapsed), the read was served normally, and nothing flagged us as a bot, so
  there is no account risk to back away from. (It did demote until 2026-08-02, which
  stranded `ft.com`, `bbc.co.uk` and `economist.com` in the manual lane — `bbc.co.uk` has no
  paywall at all, so that one was a pure misdetection.) `bloomberg.com` is seeded `manual`. Toggle via `POST /sources/scrape-mode?domain=&mode=`
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
