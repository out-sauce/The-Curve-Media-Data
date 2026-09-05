// Reads the active tab's cookies (incl. HttpOnly, via chrome.cookies) + localStorage,
// then POSTs them to <apiBase>/site-auth/import. The publisher never sees automation —
// this is your real, logged-in browser session, lifted after the fact.
//
// "Send article content" pushes the current tab's rendered HTML to <apiBase>/research/import
// (keyed by the page URL — the server resolves it to the matching pipeline article), so an
// article you're already reading can be researched without round-tripping through Admin.
//
// "Send podcast stats" (shown only on creators.spotify.com / podcasters.spotify.com)
// injects spotify-collector.js's collector into the tab, where it calls the Creators
// GraphQL API (creators-graph.spotify.com/v2/graph-pq) reusing the dashboard's own
// bearer token, and POSTs the collected
// per-episode analytics to <apiBase>/podcast/import in batches. The popup must stay
// open while it runs — closing it stops the loop (already-sent batches are kept;
// re-running is idempotent, so just press it again).

const $ = (id) => document.getElementById(id);

let activeTab = null;

function setStatus(msg, cls = "") {
  const el = $("status");
  el.textContent = msg;
  el.className = cls;
}

// Merge cookies applicable to the page URL (catches parent-domain cookies like
// ".afr.com") with those scoped to the exact host and its subdomains, deduped on
// name+domain+path. Between them these cover where publisher auth cookies actually live.
async function collectCookies(tab) {
  const url = new URL(tab.url);
  const [byUrl, byDomain] = await Promise.all([
    chrome.cookies.getAll({ url: tab.url }),
    chrome.cookies.getAll({ domain: url.hostname }),
  ]);
  const seen = new Map();
  for (const c of [...byUrl, ...byDomain]) {
    seen.set(`${c.name} ${c.domain} ${c.path}`, c);
  }
  return [...seen.values()];
}

// Read the top-frame localStorage. Best-effort — fails silently on pages that block
// injection (chrome://, some CSP), since cookies alone are usually enough.
async function collectLocalStorage(tab) {
  try {
    const [{ result } = {}] = await chrome.scripting.executeScript({
      target: { tabId: tab.id },
      func: () => {
        const out = {};
        for (let i = 0; i < localStorage.length; i++) {
          const k = localStorage.key(i);
          out[k] = localStorage.getItem(k);
        }
        return out;
      },
    });
    return result || {};
  } catch (e) {
    return {};
  }
}

async function init() {
  const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
  activeTab = tab;
  if (!tab || !tab.url || !/^https?:/.test(tab.url)) {
    setStatus("Open a publisher page (http/https) first.", "err");
    return;
  }
  const host = new URL(tab.url).hostname;
  $("domain").textContent = host;

  const { apiBase, apiKey, podcastLookback } = await chrome.storage.local.get([
    "apiBase", "apiKey", "podcastLookback",
  ]);
  if (!apiBase || !apiKey) {
    setStatus("Set the pipeline URL + API key first.", "err");
    const a = document.createElement("a");
    a.textContent = "Open settings";
    a.onclick = () => chrome.runtime.openOptionsPage();
    $("status").appendChild(document.createElement("br"));
    $("status").appendChild(a);
    return;
  }
  $("capture").disabled = false;
  $("sendArticle").disabled = false;

  if (SPOTIFY_HOSTS.includes(host)) {
    $("podcastSection").style.display = "block";
    $("sendPodcast").disabled = false;
    if (podcastLookback) $("podcastLookback").value = String(podcastLookback);
  }
}

async function capture() {
  $("capture").disabled = true;
  setStatus("Collecting session…");
  try {
    const { apiBase, apiKey } = await chrome.storage.local.get(["apiBase", "apiKey"]);
    const host = new URL(activeTab.url).hostname;

    const cookies = await collectCookies(activeTab);
    const localStorageData = await collectLocalStorage(activeTab);
    const origins = Object.keys(localStorageData).length
      ? [{ origin: new URL(activeTab.url).origin, localStorage: localStorageData }]
      : [];

    if (!cookies.length) {
      setStatus("No cookies found for this site — are you logged in?", "err");
      $("capture").disabled = false;
      return;
    }

    setStatus(`Sending ${cookies.length} cookies…`);
    const resp = await fetch(`${apiBase}/site-auth/import`, {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": apiKey },
      // No label — that's Admin-owned metadata; the server leaves it untouched.
      body: JSON.stringify({ domain: host, cookies, origins }),
    });

    const body = await resp.json().catch(() => ({}));
    if (!resp.ok) {
      setStatus(`Failed (${resp.status}): ${body.detail || resp.statusText}`, "err");
      $("capture").disabled = false;
      return;
    }
    setStatus(
      `✓ Imported for ${body.domain}\n${body.cookies} cookies, ${body.origins} origin(s).`,
      "ok"
    );
  } catch (e) {
    setStatus(`Error: ${e.message}`, "err");
    $("capture").disabled = false;
  }
}

// Push the current tab's rendered HTML to /research/import. The server matches the URL
// to a pipeline article (404 when the page isn't one) and runs the same extract +
// summarise path as the queue lane — including closing any outstanding queue row.
async function sendArticle() {
  $("sendArticle").disabled = true;
  setStatus("Reading page…");
  try {
    const { apiBase, apiKey } = await chrome.storage.local.get(["apiBase", "apiKey"]);

    const [{ result: html } = {}] = await chrome.scripting.executeScript({
      target: { tabId: activeTab.id },
      func: () => document.documentElement.outerHTML,
    });
    if (!html) {
      setStatus("Could not read this page (injection blocked).", "err");
      $("sendArticle").disabled = false;
      return;
    }

    setStatus("Sending article…");
    const resp = await fetch(`${apiBase}/research/import`, {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": apiKey },
      body: JSON.stringify({ url: activeTab.url, html }),
    });

    const body = await resp.json().catch(() => ({}));
    if (resp.status === 404) {
      setStatus("This page doesn't match any pipeline article.", "err");
    } else if (!resp.ok) {
      setStatus(`Failed (${resp.status}): ${body.detail || resp.statusText}`, "err");
    } else if (body.status === "scraped") {
      setStatus(`✓ Article imported${body.summarised ? " + summarised" : ""}.`, "ok");
    } else {
      // Import ran but trafilatura couldn't extract an article from the HTML.
      setStatus(`Extraction failed (${body.status}) — is the full article visible?`, "err");
    }
  } catch (e) {
    setStatus(`Error: ${e.message}`, "err");
  }
  $("sendArticle").disabled = false;
}

// ── Spotify for Creators podcast analytics ──────────────────────────────────
// The calls run HERE, in the popup, not injected into the page: extension-context
// fetches carry `<all_urls>` host permissions so CORS does not apply, and there is no
// MAIN/ISOLATED world to lose to (see background.js for that whole saga). The bearer is
// observed by background.js via chrome.webRequest.
//
// The popup must stay open while it runs — closing it stops the loop. Already-sent
// batches are kept and the server upserts are idempotent, so re-running heals gaps.

const SPOTIFY_HOSTS = ["creators.spotify.com", "podcasters.spotify.com"];
const PODCAST_BATCH_SIZE = 25;

async function getSpotifyBearer() {
  const resp = await chrome.runtime.sendMessage({ type: "curve:getSpotifyBearer" });
  if (resp && resp.stale) return { bearer: null, stale: true };
  return { bearer: (resp && resp.bearer) || null, stale: false };
}

// GraphQL transport. The fetch runs in the dashboard TAB (default isolated world — we
// only ever needed MAIN to patch fetch, which we no longer do) so the request carries
// Origin: https://creators.spotify.com. A popup-context fetch sends
// Origin: chrome-extension://<id> and Spotify answers 403.
function makeGraphTransport(tabId, bearer) {
  return async (op, variables) => {
    const [{ result } = {}] = await chrome.scripting.executeScript({
      target: { tabId },
      args: [{
        endpoint: SPOTIFY_GRAPH.endpoint,
        bearer,
        operationName: op.name,
        hash: op.hash,
        variables,
      }],
      func: async (cfg) => {
        try {
          // NO `credentials: "include"`. Spotify answers the preflight with a wildcard
          // Access-Control-Allow-Origin, which the browser refuses to pair with a
          // credentialed request — the fetch is blocked before it is ever sent, surfacing
          // only as "Failed to fetch". Verified live 2026-09-04: same request WITH
          // credentials -> blocked; WITHOUT -> HTTP 401 from Spotify (dummy token).
          // Auth is the bearer, not cookies, so nothing is lost by omitting them.
          const r = await fetch(cfg.endpoint, {
            method: "POST",
            headers: { "content-type": "application/json", authorization: cfg.bearer },
            body: JSON.stringify({
              operationName: cfg.operationName,
              variables: cfg.variables,
              extensions: { persistedQuery: { version: 1, sha256Hash: cfg.hash } },
            }),
          });
          const raw = await r.text();
          let body = null;
          try { body = JSON.parse(raw); } catch (e) { /* keep raw for the error message */ }
          return { status: r.status, body, raw: body ? "" : raw.slice(0, 300) };
        } catch (e) {
          return { status: 0, error: String(e && e.message ? e.message : e) };
        }
      },
    });
    return result || { status: 0, error: "injection returned nothing" };
  };
}

function showIdFromUrl(url) {
  const m = String(url || "").match(/\/dash\/show\/([^/?#]+)/);
  return m ? m[1] : null;
}

async function sendPodcastStats(fullBackfill) {
  $("sendPodcast").disabled = true;
  try {
    const { apiBase, apiKey } = await chrome.storage.local.get(["apiBase", "apiKey"]);
    const lookback = parseInt($("podcastLookback").value, 10) || 10;
    chrome.storage.local.set({ podcastLookback: lookback });

    const showId = showIdFromUrl(activeTab.url);
    if (!showId) {
      setStatus("Open your show's dashboard first (the URL should contain /dash/show/…).", "err");
      return;
    }
    const { bearer, stale } = await getSpotifyBearer();
    if (!bearer) {
      setStatus(
        stale
          ? "Spotify session is over an hour old — reload the dashboard tab, then press again."
          : "No Spotify session seen yet — reload the dashboard tab, let it load, then press again.",
        "err"
      );
      return;
    }

    const graph = makeGraphTransport(activeTab.id, bearer);

    setStatus("Listing episodes…");
    let listing;
    try {
      listing = await listSpotifyEpisodes(graph, showId, (page, total, count) => {
        setStatus(`Listing episodes… page ${page}/${total} (${count} so far)`);
      });
    } catch (e) {
      setStatus(`Episode list failed: ${e.message}`, "err");
      return;
    }
    let episodes = listing.episodes;
    if (!episodes.length) {
      setStatus("No episodes found — the endpoint table may be stale (see spotify-collector.js).", "err");
      return;
    }
    if (!fullBackfill) episodes = episodes.slice(0, lookback);

    const warnings = [...listing.errors];
    let written = 0, matched = 0;

    for (let i = 0; i < episodes.length; i += PODCAST_BATCH_SIZE) {
      const slice = episodes.slice(i, i + PODCAST_BATCH_SIZE);
      const batchNo = Math.floor(i / PODCAST_BATCH_SIZE) + 1;
      const batchTotal = Math.ceil(episodes.length / PODCAST_BATCH_SIZE);
      const collected = [];
      for (let j = 0; j < slice.length; j++) {
        setStatus(`Batch ${batchNo}/${batchTotal} — episode ${j + 1}/${slice.length}…\n(keep this popup open)`);
        const { episode, errors } = await enrichSpotifyEpisode(graph, slice[j]);
        collected.push(episode);
        warnings.push(...errors);
      }

      // Show-level demographics ride on the first batch only.
      let showBlock = null;
      if (i === 0) {
        setStatus(`Batch ${batchNo}/${batchTotal} — show demographics…`);
        try {
          const res = await collectSpotifyShow(graph, showId);
          warnings.push(...res.errors);
          // The server persists EPISODE raw only, so park the show probe on the first
          // episode either way — otherwise it disappears precisely when the show block
          // IS written, which is when a skipped dimension still needs explaining.
          if (res.show && collected[0]) {
            collected[0].raw = { ...(collected[0].raw || {}), show_probe: res.show.raw };
          }
          if (res.probeOnly) {
            warnings.push("show demographics: no breakdown returned — probe attached to first episode raw");
          } else {
            showBlock = res.show;
          }
        } catch (e) {
          warnings.push(`show demographics: ${e.message}`);
        }
      }

      const resp = await fetch(`${apiBase}/podcast/import`, {
        method: "POST",
        headers: { "content-type": "application/json", "x-api-key": apiKey },
        body: JSON.stringify({
          source: "spotify",
          show: showBlock,
          episodes: collected,
          batch: { index: batchNo, total: batchTotal, mode: fullBackfill ? "backfill" : "recent" },
        }),
      });
      const body = await resp.json().catch(() => ({}));
      if (!resp.ok) {
        // Report and continue — server upserts are idempotent, a re-run heals gaps.
        warnings.push(`batch ${batchNo} failed (${resp.status}): ${body.detail || resp.statusText}`);
        continue;
      }
      written += body.episodes_written || 0;
      matched += body.episodes_matched || 0;
      warnings.push(...(body.warnings || []));
      setStatus(`Batch ${batchNo}/${batchTotal} — ${written} written, ${matched} matched.`);
    }

    const summary = `✓ ${written} episodes written, ${matched} matched to podcast_episodes.`;
    if (warnings.length) {
      const shown = warnings.slice(0, 5).join("\n");
      setStatus(`${summary}\n⚠ ${warnings.length} warning(s):\n${shown}${warnings.length > 5 ? "\n…" : ""}`, "err");
    } else {
      setStatus(summary, "ok");
    }
  } catch (e) {
    setStatus(`Error: ${e.message}`, "err");
  } finally {
    $("sendPodcast").disabled = false;
  }
}

document.addEventListener("DOMContentLoaded", init);
$("capture").addEventListener("click", capture);
$("sendArticle").addEventListener("click", sendArticle);
$("sendPodcast").addEventListener("click", () => sendPodcastStats(false));
$("podcastBackfill").addEventListener("click", () => {
  if (confirm("Collect analytics for EVERY episode? Takes a few minutes — keep the popup open.")) {
    sendPodcastStats(true);
  }
});
