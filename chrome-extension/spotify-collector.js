// Spotify for Creators collector — injected into the creators.spotify.com tab by
// popup.js (loaded there via <script src>, so popup.js can pass collectSpotifyAnalytics
// to chrome.scripting.executeScript as `func`). Everything the injected function needs
// travels in via `args` — an injected func is serialized and cannot close over this
// file's scope.
//
// ⚠️ EVERY PATH IN SPOTIFY_ENDPOINTS IS A CANDIDATE, RECORDED FROM HISTORICAL
// (Anchor-heritage) KNOWLEDGE, NOT VERIFIED LIVE. This repo's standing lesson (Zernio,
// Apify — see CLAUDE.md) is that unverified vendor shapes are always wrong somewhere.
// Discovery procedure when a fetch fails or a field comes back empty:
//   1. Open the Creators episode/analytics pages with DevTools → Network (Fetch/XHR).
//   2. Find the request that feeds the number/chart on screen; note host, full path,
//      query params (date ranges!), and whether it sent a Bearer token.
//   3. Correct the entry below (and the token URL if auth differs); field-name fixes
//      go in ingestion/podcast.py's _pick() lists — the raw blobs are already stored,
//      so parsed history can be recovered without re-fetching.
// The collector never throws for one episode: a failed episode is reported as
// {episode_id, error} and the batch continues.

const SPOTIFY_ENDPOINTS = {
  // Cookie-authed token mint the dashboard itself uses; the Bearer it returns is what
  // the wg.spotify.com analytics endpoints expect. If this 404s, find the real one via
  // discovery — it is the first request with an `authorization` header's origin story.
  tokenUrl: "https://generic.wg.spotify.com/creator-auth-proxy/v1/web/token",
  apiBase: "https://generic.wg.spotify.com/podcasters/v0",
  // {show} = show id from the dashboard URL; {episode} = Spotify episode id;
  // {start}/{end} = YYYY-MM-DD.
  episodes: "/shows/{show}/episodes?end={limit}&start=0&sortBy=releaseDate&sortOrder=descending&filter=",
  episodeAggregate: "/shows/{show}/episodes/{episode}/aggregate?start={start}&end={end}",
  episodePerformance: "/shows/{show}/episodes/{episode}/performance",
  showFollowers: "/shows/{show}/followers?start={start}&end={end}",
  showGender: "/shows/{show}/gender?start={start}&end={end}",
  showAge: "/shows/{show}/age?start={start}&end={end}",
  showCountry: "/shows/{show}/country?start={start}&end={end}",
  // Set after discovery confirms whether the demographics endpoints return absolute
  // counts ("count") or percentages ("percent"). While null, the server refuses to
  // write demographics (by design — writing one as the other corrupts the table).
  demographicsValueType: null,
};

// Runs inside the tab. phase "list" → {episodes:[...]} (newest first);
// phase "collect" → the /podcast/import payload for the given episodes
// (+ show block when includeShow). Never rejects for one episode.
async function collectSpotifyAnalytics(cfg) {
  const out = { errors: [] };
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  const delay = () => sleep(300 + Math.floor(Math.random() * 200));

  const showMatch = location.pathname.match(/\/pod\/show\/([^/]+)/);
  if (!showMatch) {
    return { error: "Not on a show page — open your show's dashboard (URL should contain /pod/show/…) and retry." };
  }
  const show = showMatch[1];

  // Best-effort Bearer; the analytics endpoints may accept cookies alone.
  let bearer = null;
  try {
    const resp = await fetch(cfg.endpoints.tokenUrl, { method: "POST", credentials: "include" });
    if (resp.ok) {
      const body = await resp.json().catch(() => ({}));
      bearer = body.token || body.accessToken || body.access_token || null;
    } else {
      out.errors.push(`token fetch ${resp.status} — trying cookie auth only`);
    }
  } catch (e) {
    out.errors.push(`token fetch failed (${e.message}) — trying cookie auth only`);
  }

  const api = async (template, vars) => {
    let path = template;
    for (const [k, v] of Object.entries(vars)) path = path.split(`{${k}}`).join(encodeURIComponent(v));
    const url = path.startsWith("http") ? path : cfg.endpoints.apiBase + path;
    const resp = await fetch(url, {
      credentials: "include",
      headers: bearer ? { authorization: `Bearer ${bearer}` } : {},
    });
    if (!resp.ok) throw new Error(`${resp.status} ${url.split("?")[0]}`);
    return resp.json();
  };

  const today = new Date().toISOString().slice(0, 10);
  const epoch = "2015-01-01"; // before any Curve episode — "all time" for aggregates
  const vars = (extra) => ({ show, start: epoch, end: today, limit: 500, ...extra });

  if (cfg.phase === "list") {
    try {
      const body = await api(cfg.endpoints.episodes, vars({}));
      const items = body.episodes || body.items || body.data || [];
      out.episodes = items.map((e) => ({
        episode_id: e.episodeId || e.id || e.spotifyId || null,
        title: e.title || e.name || null,
        release_date: e.releaseDate || e.publishOn || e.published || null,
        duration_ms: e.duration || e.durationMs || null,
        url: e.shareUrl || e.url || null,
        raw: e,
      }));
    } catch (e) {
      out.error = `episode list failed: ${e.message}`;
    }
    return out;
  }

  // phase "collect"
  out.source = "spotify";
  out.episodes = [];
  for (const ep of cfg.episodes || []) {
    const collected = { ...ep };
    try {
      const aggregate = await api(cfg.endpoints.episodeAggregate, vars({ episode: ep.episode_id }));
      collected.metrics = aggregate.counts || aggregate.metrics || aggregate;
      collected.raw = { listing: ep.raw, aggregate };
      await delay();
      try {
        const performance = await api(cfg.endpoints.episodePerformance, vars({ episode: ep.episode_id }));
        collected.retention = performance;
        collected.raw.performance = performance;
      } catch (e) {
        // Retention missing is a partial result, not a failed episode.
        collected.raw.performance_error = e.message;
      }
    } catch (e) {
      collected.error = e.message;
    }
    out.episodes.push(collected);
    await delay();
  }

  if (cfg.includeShow) {
    const showBlock = { raw: {} };
    try {
      const followers = await api(cfg.endpoints.showFollowers, vars({}));
      showBlock.raw.followers = followers;
      const counts = followers.counts || followers.followers || [];
      const latest = Array.isArray(counts) ? counts[counts.length - 1] : counts;
      showBlock.followers = (latest && (latest.count ?? latest.total ?? latest)) || followers.total || null;
    } catch (e) {
      out.errors.push(`followers: ${e.message}`);
    }
    const demographics = { value_type: cfg.endpoints.demographicsValueType };
    for (const [dimension, template] of [
      ["gender", cfg.endpoints.showGender],
      ["age", cfg.endpoints.showAge],
      ["country", cfg.endpoints.showCountry],
    ]) {
      try {
        const body = await api(template, vars({}));
        demographics[dimension] = body.counts || body.buckets || body.data || body;
        showBlock.raw[dimension] = body;
        await delay();
      } catch (e) {
        out.errors.push(`${dimension}: ${e.message}`);
      }
    }
    if (demographics.gender || demographics.age || demographics.country) {
      showBlock.demographics = demographics;
    }
    if (showBlock.followers != null || showBlock.demographics) out.show = showBlock;
  }
  return out;
}
