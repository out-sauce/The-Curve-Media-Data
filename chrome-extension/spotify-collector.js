// Spotify for Creators collector.
//
// Orchestrated from the POPUP, but every request is executed INSIDE THE DASHBOARD TAB via
// a transport the popup injects (see makeGraphTransport in popup.js). The Origin header
// decides this: a popup-context fetch sends Origin: chrome-extension://<id> and Spotify
// answers 403 (verified live 2026-09-04); a fetch from the tab sends
// Origin: https://creators.spotify.com, matching the dashboard's own calls. The isolated
// world is fine for that — MAIN was only ever needed to PATCH fetch, which we no longer do.
// The bearer comes from background.js, which observes it via chrome.webRequest.
//
// TRANSPORT (verified live 2026-09-04 from DevTools):
//   POST https://creators-graph.spotify.com/v2/graph-pq
//   body { operationName, variables, extensions:{persistedQuery:{version:1,sha256Hash}} }
//   Apollo persisted queries — only REGISTERED hashes are accepted, so these are pinned,
//   not derived. They live in Spotify's JS bundle and WILL rotate on a frontend release.
//   We deliberately do NOT scrape them out of the bundle at runtime; a rotation surfaces
//   as PERSISTED_QUERY_NOT_FOUND and is reported as "re-run discovery" — a loud, correct
//   failure rather than a silent zero.
//
// Ids are URIs: spotify:show:<id> / spotify:episode:<id>, <id> from /dash/show/{showId}.

const SPOTIFY_GRAPH = {
  endpoint: "https://creators-graph.spotify.com/v2/graph-pq",
  ops: {
    // CONFIRMED. -> data.showByShowUri.episodesV2.{items[], pagination{totalPages,totalItems}}
    // Items carry starts/streams/listeners themselves, so bulk metrics cost ~17 requests
    // for 424 episodes rather than one call each.
    episodeList: { name: "GetEpisodeList", hash: "164c5aae8eb14c569ac351193f8097dbb00a7288f22486ecf9efbf0cc57f3fc5" },
    // CONFIRMED. -> data.episodeByUri.{playsDaily, audienceDaily, audienceTotal}
    episodeStats: { name: "getEpisodePerformanceStats", hash: "9b199a61871d44ff697fa5f7e83de1f83b0822550b4f84693c871e1d1c0dc017" },
    // Payload confirmed; the consumptionTimeDaily response was captured but not positively
    // tied to this operation name. Best-effort: failure here is a partial episode, not a
    // failed one.
    episodeAllTime: { name: "getEpisodePerformanceAllTime", hash: "005657984f013085ac61760b836303f22e27dec6b386af1cc7c22ac6656fee26" },
    // Harvested 2026-09-05 from Spotify's own JS bundle, which ships the full GraphQL
    // ASTs (`__meta__:{hash}` + OperationDefinition + selection set). Names, variables
    // and response fields are read from source, not guessed at.
    //
    // ⚠️ HASHES MUST BE THE FULL 64 HEX CHARS. A truncated hash does not error — the
    // server simply matches nothing and returns an empty data object, which looks
    // exactly like "this show has no data". That cost a whole run: the first cut of
    // this table was pasted from a 16-char display slice.
    episodeStreams: { name: "getEpisodeStreams", hash: "afa55877d7776b279b7dd75e18b54729a87e971bfc14a7c262ca88fda08dfb94" },
    showGender: { name: "getShowPlaysByGender", hash: "bf4c1c241e1ba4b321dd1ff5dd228db191639d52e39e551deac625aa49b080b7" },
    showAge: { name: "getShowPlaysByAge", hash: "98f15a89cb2500253b160b62397617c64b79938ed3f4812eb4d0b605fa2e73d5" },
    // audience_demographics stores country as ISO-2 (verified live: instagram rows are
    // NZ/GB/AU/US/CA/IN/IE/DE…, hand-entered podcast rows NZ/AU/GB/US/ROW). Spotify's
    // geos[] carry a displayName ("New Zealand"), so a code must be DERIVED — writing
    // display names would make country ungroupable across platforms. We derive it from
    // flagUrl (…/nz.svg) or a 2-char navigationName, and WRITE NOTHING if neither yields
    // a code, recording the entry shape instead so the field can be identified.
    showGeo: { name: "getShowPlaysByGeo", hash: "3015accdc4ec210d01ff9468e4daa8b4c95109488f96a4d2aa4438e01b4e9955" },
    // Per-episode demographics -> podcast_episodes.spotify_* columns (migration 044).
    // Same response shapes as their show-level twins, keyed on episodeUri.
    episodeAge: { name: "getEpisodePlaysByAge", hash: "afa9560c56ec2e404d2042f8019d03bc799ceed49caedd4599702721745db024" },
    episodeGender: { name: "getEpisodePlaysByGender", hash: "51e1af212dc906a22afd0f53e2b362c818b0f1e37a8f63e57f18de3bd3e54ecc" },
    episodeGeo: { name: "getEpisodeSpotifyPlaysByCountry", hash: "8ae8a678646f88b9639408a3bb188467726a9fd8a88896b5790a5bf79587d196" },
  },
  // RESOLVED 2026-09-05: getShowPlaysByGender's genderBreakdown.counts[] carries BOTH
  // `count` (absolute) and `percent` on every bucket, so there is nothing to infer — we
  // read counts. This was the single fact blocking demographics from the start.
  //
  // FOLLOWERS ARE NOT AVAILABLE. No operation, and no selection-set field, containing
  // "follow" exists in any bundle on the show audience page (32 scripts scanned).
  // Spotify follower_snapshots stay hand-entered.
  demographicsValueType: "count",
};

// ⚠️ The *Daily series are CUMULATIVE running totals, not per-day values.
// Verified: audienceTotal (1696) equals the LAST point of audienceDaily, not the sum, and
// playsDaily climbs monotonically 142 -> 2659. Summing would be wildly wrong; true daily
// figures are the diff of consecutive points.
function lastPoint(node) {
  const inner = node && node.analyticsValue && node.analyticsValue.analyticsValue;
  if (!inner) return null;
  if (inner.value != null && !Array.isArray(inner.points)) return inner.value; // SingleValueLong
  const points = inner.points || [];
  if (!points.length) return null;
  const v = points[points.length - 1].value;
  if (v == null) return null;
  return v.value != null ? v.value : (v.totalConsumptionHours != null ? v.totalConsumptionHours : null);
}

// The confirmed ops nest under analyticsValue.analyticsValue, but that depth is NOT
// verified for the newly harvested ones — and every field bug so far came from assuming
// a shape. Walk the tree for the key instead of hard-coding a path.
function findDeep(node, key, depth = 0) {
  if (!node || typeof node !== "object" || depth > 8) return null;
  if (Object.prototype.hasOwnProperty.call(node, key) && node[key] != null) return node[key];
  for (const v of Object.values(node)) {
    const hit = findDeep(v, key, depth + 1);
    if (hit != null) return hit;
  }
  return null;
}

// Some series are cumulative running totals (playsDaily, audienceDaily — verified) and
// some are per-day (episodeStreamsDaily — inferred live: streams/plays fell 0.63 -> 0.02
// as episodes got older, and a reference payload showed streams ~= 94% of starts, not 6%).
// Reading one wrongly is a silent order-of-magnitude error, so detect rather than assume:
// a non-decreasing series is cumulative (take the last point), anything else is per-day
// (sum it). Returns {value, mode} so the choice is recorded in vendor_raw.
function seriesTotal(node) {
  const inner = node && node.analyticsValue && node.analyticsValue.analyticsValue;
  if (!inner) return { value: null, mode: "none" };
  if (inner.value != null && !Array.isArray(inner.points)) return { value: inner.value, mode: "single" };
  const pts = (inner.points || [])
    .map((p) => (p && p.value ? (p.value.value != null ? p.value.value : p.value.totalConsumptionHours) : null))
    .filter((v) => typeof v === "number");
  if (!pts.length) return { value: null, mode: "empty" };
  let monotonic = true;
  for (let i = 1; i < pts.length; i++) if (pts[i] < pts[i - 1]) { monotonic = false; break; }
  return monotonic
    ? { value: pts[pts.length - 1], mode: "cumulative", n: pts.length }
    : { value: pts.reduce((a, b) => a + b, 0), mode: "perDay", n: pts.length };
}

function genderRows(payload) {
  const b = findDeep(payload, "genderBreakdown");
  const counts = b && b.counts;
  if (!Array.isArray(counts)) return null;
  const rows = counts.filter((c) => c && c.gender != null && c.count != null)
                     .map((c) => ({ gender: c.gender, count: c.count }));
  return rows.length && rows.some((r) => r.count > 0) ? rows : null;
}

function ageRows(payload) {
  const brackets = findDeep(payload, "ageBreakdown");
  if (!Array.isArray(brackets)) return null;
  const rows = brackets
    .map((b) => ({
      age: b.ageBracket || b.displayName,
      count: (b.genderBreakdown && b.genderBreakdown.total) != null ? b.genderBreakdown.total : b.total,
    }))
    .filter((r) => r.age != null && r.count != null);
  return rows.length && rows.some((r) => r.count > 0) ? rows : null;
}

// Geo entries are NOT what they look like (verified against the live response):
//   {displayName: "Australia", flagUrl: ".../geo_2/australia.png", value: 0.1371499}
// - flagUrl carries a country NAME, not an ISO-2 code. An earlier regex pulled the last
//   two letters before ".png", which turns "Australia" into "IA". There is no code
//   anywhere in the payload.
// - `value` is a FRACTION of plays (0.137 = 13.7%), not a count. podcast_episodes'
//   geo_plays_* columns are bigint counts, so it is multiplied by the episode's plays.
// Only four countries have columns, so a full name->ISO map is unnecessary: everything
// else folds into ROW, which is exactly what the schema wants.
const GEO_NAME_TO_CODE = {
  "new zealand": "NZ",
  "australia": "AU",
  "united kingdom": "GB",
  "united states": "US",
};

function geoRows(payload, totalPlays) {
  const geos = findDeep(payload, "geos");
  if (!Array.isArray(geos) || !geos.length) return { rows: null, probe: null };
  const values = geos.map((e) => Number(e.value)).filter((v) => !Number.isNaN(v));
  if (!values.length) return { rows: null, probe: geos.slice(0, 3) };
  // Detect fractions vs counts rather than assuming: every value <= 1 means shares.
  const areFractions = values.every((v) => v <= 1);
  if (areFractions && !totalPlays) return { rows: null, probe: geos.slice(0, 3) };
  const rows = geos
    .map((e) => {
      const v = Number(e.value);
      if (Number.isNaN(v)) return null;
      return {
        country: GEO_NAME_TO_CODE[String(e.displayName || "").trim().toLowerCase()] || "ROW",
        count: areFractions ? Math.round(v * totalPlays) : Math.round(v),
      };
    })
    .filter(Boolean);
  return { rows: rows.length ? rows : null, probe: geos.slice(0, 3), mode: areFractions ? "fraction" : "count" };
}

const spotifySleep = (ms) => new Promise((r) => setTimeout(r, ms));

// `graph` is a transport: (op, variables) -> {status, body, error}.
// It is supplied by popup.js and runs the request INSIDE THE DASHBOARD TAB, because the
// request's Origin matters: a popup-context fetch sends Origin: chrome-extension://<id>
// and Spotify answers 403 (verified live 2026-09-04). A fetch from the tab — isolated
// world is fine, we no longer patch anything — carries Origin: https://creators.spotify.com,
// which is what the dashboard's own calls send.
async function spotifyGraph(graph, op, variables, errors) {
  if (!/^[0-9a-f]{64}$/.test(op.hash)) {
    throw new Error(`${op.name}: persisted-query hash is not 64 hex chars (got ${op.hash.length}) — truncated hashes match nothing and return empty data`);
  }
  const resp = await graph(op, variables);
  if (resp.error) throw new Error(`${op.name}: ${resp.error}`);
  if (resp.status === 401) {
    throw new Error(`401 — Spotify session expired; reload the dashboard and retry`);
  }
  if (resp.status === 403) {
    throw new Error(`403 on ${op.name} — refused: ${String(resp.raw || "").slice(0, 160)}`);
  }
  if (resp.status < 200 || resp.status >= 300) {
    throw new Error(`${resp.status} on ${op.name}: ${String(resp.raw || "").slice(0, 160)}`);
  }
  const body = resp.body || {};
  const errs = body.errors || [];
  if (errs.some((e) => /PERSISTED_QUERY_NOT_FOUND/i.test(JSON.stringify(e)))) {
    throw new Error(`${op.name}: Spotify rotated its query hashes — re-run endpoint discovery`);
  }
  // Partial failures are normal (RpcMethodExecutionException on analytics subfields).
  // Surface them, keep whatever data did arrive.
  if (errs.length && errors) errors.push(`${op.name}: ${errs.length} partial error(s)`);
  return body.data || {};
}

// Every episode of the show, newest first, with whatever metrics the listing carries.
async function listSpotifyEpisodes(graph, showId, onProgress) {
  const errors = [];
  const episodes = [];
  const showUri = `spotify:show:${showId}`;
  let page = 1, totalPages = 1;
  do {
    const data = await spotifyGraph(graph, SPOTIFY_GRAPH.ops.episodeList,
      { showUri, currentPage: page, pageSize: 25 }, errors);
    const v2 = (data.showByShowUri || {}).episodesV2 || {};
    totalPages = (v2.pagination || {}).totalPages || 1;
    for (const e of v2.items || []) {
      const id = e.episodeId || e.id || (e.uri || "").split(":").pop() || null;
      if (!id) continue;
      // VERIFIED SHAPES (live 2026-09-04, read back out of vendor_raw):
      //   publishedOn : { seconds: 1791417600 }   protobuf Timestamp, NOT a date string
      //   asset       : { durationMs: 3010000 }   duration is NOT a top-level field
      // The listing carries no description and no shareUrl at all, so caption falls back
      // to the title server-side and the URL is synthesised from the id.
      const publishedSec = e.publishedOn && e.publishedOn.seconds != null
        ? Number(e.publishedOn.seconds) : null;
      const durationMs = e.asset && e.asset.durationMs != null
        ? Number(e.asset.durationMs) : null;
      episodes.push({
        episode_id: id,
        title: e.title || e.name || null,
        release_date: publishedSec ? new Date(publishedSec * 1000).toISOString().slice(0, 10) : null,
        duration_ms: durationMs,
        url: e.shareUrl || e.url || `https://open.spotify.com/episode/${id}`,
        description: e.description || null,
        // Both spellings were seen in one session; take whichever is present.
        metrics: {
          plays: e.starts != null ? e.starts : e.analyticsStartsV2,
          starts: e.starts != null ? e.starts : e.analyticsStartsV2,
          streams: e.streams != null ? e.streams : e.analyticsStreams,
          listeners: e.listeners != null ? e.listeners : e.analyticsListeners,
        },
        raw: e,
      });
    }
    if (onProgress) onProgress(page, totalPages, episodes.length);
    if (!(v2.items || []).length) break;
    page += 1;
    if (page <= totalPages) await spotifySleep(250);
  } while (page <= totalPages && page <= 60);
  return { episodes, errors, totalPages };
}

// Exact totals + listen time for one episode. The listing lags ~a day, so this runs even
// when listing metrics are present.
async function enrichSpotifyEpisode(graph, ep) {
  const errors = [];
  const out = { ...ep, raw: { listing: ep.raw } };
  const metrics = { ...(ep.metrics || {}) };
  const uri = `spotify:episode:${ep.episode_id}`;
  try {
    const stats = await spotifyGraph(graph, SPOTIFY_GRAPH.ops.episodeStats,
      { episodeUri: uri, dateRangeWindow: "WINDOW_SINCE_PUBLISHED" }, errors);
    const node = stats.episodeByUri || {};
    // Same cap discipline: keep the tail of each series, not the whole thing.
    const tail = (n) => {
      const pts = ((n || {}).analyticsValue || {}).analyticsValue || {};
      if (!Array.isArray(pts.points)) return pts;
      return { ...pts, points: pts.points.slice(-3), pointCount: pts.points.length };
    };
    out.raw.stats = {
      playsDaily: tail(node.playsDaily),
      audienceDaily: tail(node.audienceDaily),
      audienceTotal: ((node.audienceTotal || {}).analyticsValue || {}).analyticsValue || null,
      uri: node.uri,
    };
    const plays = lastPoint(node.playsDaily);
    const total = lastPoint(node.audienceTotal);
    const listeners = total != null ? total : lastPoint(node.audienceDaily);
    if (plays != null) { metrics.plays = plays; metrics.starts = plays; }
    if (listeners != null) metrics.listeners = listeners;

    await spotifySleep(250);
    try {
      const all = await spotifyGraph(graph, SPOTIFY_GRAPH.ops.episodeAllTime, { episodeUri: uri }, errors);
      const anode = all.episodeByUri || {};
      // getEpisodePerformanceAllTime returns the RETENTION curve — episodePerformance-
      // TotalAllTime.analyticsValue.analyticsValue = {points[], percentiles[],
      // sampleRateMs, maxSampleCount, medianCompletionSeconds}. (An earlier guess that
      // it returned consumptionTimeDaily was wrong; that came from another operation.)
      const perf = ((anode.episodePerformanceTotalAllTime || {}).analyticsValue || {}).analyticsValue || {};
      // ⚠️ ZERO MEANS ABSENT, as everywhere else in this codebase: an episode with no
      // retention data yet reports medianCompletionSeconds 0 AND maxSampleCount 0
      // (3 of the first 4 live rows). Writing that would stamp 0% completion on every
      // freshly published episode.
      const median = Number(perf.medianCompletionSeconds) || null;
      const samples = Number(perf.maxSampleCount) || null;
      if (median && samples) {
        // NOTE: this is a MEDIAN, not a mean — the point where the retention curve
        // crosses 50%. It lands in podcast_episodes.spotify_avg_listen_minutes, whose
        // name says "avg"; the column pre-dates us and Spotify publishes only the median.
        // Verified against Spotify's own chart: 40.3 min on a 42.3 min episode is exactly
        // where that episode's curve drops through half.
        metrics.avg_listen_sec = median;
      }
      // COMPLETION RATE comes from `percentiles`, NOT from median/duration.
      // Verified against Spotify's UI for "7 Questions to Ask Before You Buy an
      // Investment Property" (2026-09-05), which showed 80/69/60 quartiles and a 51%
      // completion rate. The array reproduces that exactly:
      //   {completionPercentage:25, audiencePercentage:80}  1st quartile
      //   {completionPercentage:50, audiencePercentage:69}  2nd
      //   {completionPercentage:75, audiencePercentage:60}  3rd
      //   {completionPercentage:95, audiencePercentage:51}  <- Spotify's "Completion rate"
      //   {completionPercentage:100,audiencePercentage:32}
      // SPOTIFY DEFINES COMPLETION AS REACHING 95% OF THE EPISODE, not 100% — reading
      // the 100 bucket would understate every episode (32 vs 51 here).
      // The previous median/duration formula gave 95.2% against a true 51%.
      const pcts = Array.isArray(perf.percentiles) ? perf.percentiles : [];
      const at95 = pcts.find((p) => p && Number(p.completionPercentage) === 95);
      const completion = at95 ? Number(at95.audiencePercentage) : null;
      // All-zero percentiles mean no retention data yet (a fresh episode), not 0%.
      if (completion && pcts.some((p) => Number(p.audiencePercentage) > 0)) {
        metrics.completion_pct = completion;
      }
      // Keep the curve's SCALARS and percentiles, never its ~600 `points`: the raw blob
      // is capped at ~50KB server-side and the full curve blew it, so 46 of the first 50
      // episodes stored no vendor_raw at all — losing exactly the re-parse safety net it
      // exists to provide.
      await spotifySleep(250);
      // The response key for streams is UNKNOWN — `streamsDaily` was a guess and it
      // found nothing (the call succeeded, so it is the key that is wrong, not the
      // request). Try both plausible windows, take whichever returns data, and always
      // record the response's key names so the real field can be read back out of
      // vendor_raw rather than guessed at a third time.
      for (const win of ["WINDOW_ALL_TIME", "WINDOW_SINCE_PUBLISHED"]) {
        try {
          const st = await spotifyGraph(graph, SPOTIFY_GRAPH.ops.episodeStreams,
            { episodeUri: uri, dateRangeWindow: win }, errors);
          const node = st.episodeByUri || {};
          out.raw.streams_probe = { window: win, keys: Object.keys(node) };
          for (const k of Object.keys(node)) {
            if (k === "uri") continue;
            const t = seriesTotal(node[k]);
            if (t.value) {
              metrics.streams = t.value;
              out.raw.streams_probe.usedKey = k;
              out.raw.streams_probe.mode = t.mode;
              out.raw.streams_probe.points = t.n;
              break;
            }
          }
          if (metrics.streams) break;
        } catch (e) {
          out.raw.streams_error = `${win}: ${e.message}`;
        }
        await spotifySleep(200);
      }

      out.raw.all_time = {
        percentiles: perf.percentiles || null,
        medianCompletionSeconds: perf.medianCompletionSeconds,
        sampleRateMs: perf.sampleRateMs,
        maxSampleCount: perf.maxSampleCount,
        pointCount: Array.isArray(perf.points) ? perf.points.length : 0,
      };
    } catch (e) {
      out.raw.all_time_error = e.message;
    }

    // Per-episode demographics -> podcast_episodes.spotify_* columns.
    // Three extra calls per episode: this roughly doubles a full backfill's runtime.
    // ── Per-episode demographics ──────────────────────────────────────────────
    // WINDOW IS WINDOW_LAST_THIRTY_DAYS, read off the dashboard's own request (2026-09-05).
    // Not a guess and not interchangeable: SINCE_PUBLISHED and ALL_TIME both return the
    // full structure with totalValue 0, which is indistinguishable from "no data" — that
    // cost three runs. These operations serve rolling windows only; there is no lifetime
    // per-episode breakdown to be had.
    //
    // These are LIFETIME figures, directly comparable with the hand-entered values
    // already in podcast_episodes — so there is NO age gate. An earlier version gated on
    // episode age after measuring that the back catalogue gets ~0 plays/day; that is true
    // but irrelevant to an all-time window, and it would have skipped every episode worth
    // backfilling. The dashboard's own request is the authority here.
    //
    // The only guard is a small-sample one: a brand-new episode can return a breakdown
    // over a handful of listeners, and percentages from that are noise.
    const MIN_DEMOGRAPHIC_SAMPLE = 50;
    const demo = {};
    const probe = {};
    {
    for (const [key, op, reader] of [
      ["gender", SPOTIFY_GRAPH.ops.episodeGender, genderRows],
      ["age", SPOTIFY_GRAPH.ops.episodeAge, ageRows],
      ["geo", SPOTIFY_GRAPH.ops.episodeGeo, null],
    ]) {
      // Gender and age answer WINDOW_ALL_TIME (read off the dashboard's request). Geo
      // returns an EMPTY geos[] for that window, so it is tried across the others —
      // same per-operation window quirk, which surfaces as empty data rather than an
      // error. Whichever produces entries wins; the probe records which.
      const windows = key === "geo"
        ? ["WINDOW_ALL_TIME", "WINDOW_LAST_THIRTY_DAYS", "WINDOW_SINCE_PUBLISHED", "WINDOW_LAST_NINETY_DAYS"]
        : ["WINDOW_ALL_TIME"];
      for (const win of windows) {
        await spotifySleep(200);
        try {
          const r = await spotifyGraph(graph, op,
            { episodeUri: uri, dateRangeWindow: win }, errors);
          const node = r.episodeByUri || r;
          // Record whether the container is merely PRESENT or actually POPULATED.
          // "episodePlaysByGender exists but is empty" (Spotify withholds breakdowns
          // below an audience threshold) and "wrong window" look identical otherwise.
          const dataKey = Object.keys(node || {}).find((k) => k !== "uri");
          const inner = dataKey ? node[dataKey] : null;
          const av = inner && inner.analyticsValue && inner.analyticsValue.analyticsValue;
          probe[key] = {
            window: win,
            keys: Object.keys(node || {}),
            containerNull: inner == null,
            innerKeys: av ? Object.keys(av) : (inner ? Object.keys(inner) : null),
            totalValue: av ? (av.totalValue ?? null) : null,
          };
          // SAMPLE-SIZE GATE. The age gate above is only a cost saver; THIS is the
          // correctness one. Measured live: episodes past ~90 days get a median of ZERO
          // plays per day (291 of 317 over-a-year episodes had none yesterday), so a
          // trailing-30-day breakdown on an older episode is computed over a handful of
          // listeners. A gender split from a sample of 12 is noise, and it would be
          // written over a hand-entered lifetime figure. Require a real sample instead.
          const total = av && av.totalValue != null ? Number(av.totalValue) : null;
          if (total != null && total < MIN_DEMOGRAPHIC_SAMPLE) {
            probe[key].rejectedSmallSample = total;  // lifetime plays below MIN_DEMOGRAPHIC_SAMPLE
            break;
          }
          if (key === "geo") {
            const g = geoRows(r, metrics.plays);
            if (g.mode) probe[key].valueMode = g.mode;
            if (g.rows) { demo.geo = g.rows; break; }
            if (g.probe) probe.geo_entries = g.probe;
          } else {
            const rows = reader(r);
            if (rows) { demo[key] = rows; break; }
          }
        } catch (e) {
          out.raw[`${key}_error`] = e.message;
          break;
        }
      }
    }
    out.raw.demo_probe = probe;
    }
    if (Object.keys(demo).length) out.demographics = demo;
  } catch (e) {
    out.error = e.message;
  }
  out.metrics = metrics;
  return { episode: out, errors };
}


// ── Show-level demographics ──────────────────────────────────────────────────
// Gender and age only. Counts, not percentages — the payload carries both, so there is
// no value_type ambiguity. Returns the `show` block /podcast/import expects, or null.
async function collectSpotifyShow(graph, showId) {
  const errors = [];
  const showUri = `spotify:show:${showId}`;
  // WINDOW_SINCE_PUBLISHED is an EPISODE concept — a show is never "published" — which
  // is the likeliest reason the first attempt returned nothing. Try all-time first.
  const windows = ["WINDOW_ALL_TIME", "WINDOW_LAST_THIRTY_DAYS"];
  const out = { raw: { probe: [] }, demographics: { value_type: "count" } };

  let vars = null;
  for (const win of windows) {
    try {
      const g = await spotifyGraph(graph, SPOTIFY_GRAPH.ops.showGender,
        { showUri, dateRangeWindow: win }, errors);
      out.raw.probe.push({ window: win, keys: Object.keys(g || {}),
                           showKeys: Object.keys((g || {}).showByShowUri || {}) });
      if (findDeep(g, "genderBreakdown")) { vars = { showUri, dateRangeWindow: win }; break; }
    } catch (e) {
      out.raw.probe.push({ window: win, error: e.message });
    }
    await spotifySleep(200);
  }
  if (!vars) return { show: { raw: out.raw }, errors, probeOnly: true };

  try {
    const g = await spotifyGraph(graph, SPOTIFY_GRAPH.ops.showGender, vars, errors);
    const rows = genderRows(g);
    if (rows) { out.demographics.gender = rows; out.raw.gender = { counts: rows }; }
  } catch (e) {
    errors.push(`gender: ${e.message}`);
  }

  await spotifySleep(250);
  try {
    const a = await spotifyGraph(graph, SPOTIFY_GRAPH.ops.showAge, vars, errors);
    const brackets = findDeep(a, "ageBreakdown");
    if (Array.isArray(brackets)) {
      // Each bracket carries its own genderBreakdown; the bracket total is that total.
      out.demographics.age = brackets
        .map((b) => ({
          age: b.ageBracket || b.displayName,
          count: (b.genderBreakdown && b.genderBreakdown.total) != null
            ? b.genderBreakdown.total : b.total,
        }))
        .filter((b) => b.age != null && b.count != null);
      out.raw.age = brackets;
    }
  } catch (e) {
    errors.push(`age: ${e.message}`);
  }

  await spotifySleep(250);
  try {
    const g = await spotifyGraph(graph, SPOTIFY_GRAPH.ops.showGeo,
      { ...vars, metricType: "METRIC_TYPE_PLAYS" }, errors);
    const geos = findDeep(g, "geos");
    if (Array.isArray(geos) && geos.length) {
      // Probe first: record the raw shape of a few entries so the ISO-2 field can be
      // identified from stored data if the derivation below misses.
      out.raw.geo_probe = geos.slice(0, 3);
      const iso = (e) => {
        const m = String(e.flagUrl || "").match(/([a-zA-Z]{2})\.(?:svg|png|jpg)(?:$|\?)/);
        if (m) return m[1].toUpperCase();
        if (typeof e.navigationName === "string" && /^[a-zA-Z]{2}$/.test(e.navigationName)) {
          return e.navigationName.toUpperCase();
        }
        return null;
      };
      const rows = geos
        .map((e) => ({ country: iso(e), count: e.value }))
        .filter((r) => r.country && r.count != null);
      // All-or-nothing: a partial map would silently drop countries from the totals.
      if (rows.length === geos.length) out.demographics.country = rows;
      else errors.push(`country: ISO-2 derivable for only ${rows.length}/${geos.length} — not written, probe stored`);
    }
  } catch (e) {
    errors.push(`geo: ${e.message}`);
  }

  const has = (out.demographics.gender || []).length || (out.demographics.age || []).length
    || (out.demographics.country || []).length;
  return { show: has ? out : null, errors };
}
