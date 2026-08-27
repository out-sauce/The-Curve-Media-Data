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
// dashboard's own JSON endpoints with your live session, and POSTs the collected
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
// Two-phase: one injection lists the show's episodes, then batches of 25 are
// re-injected to collect analytics and POSTed to /podcast/import. Sequential and
// popup-bound on purpose — the operator watches the batch counter; if a backfill
// ever proves too long for an open popup, the loop moves to background.js (the
// research queue pattern), not to parallel fetches Spotify might rate-limit.

const SPOTIFY_HOSTS = ["creators.spotify.com", "podcasters.spotify.com"];
const PODCAST_BATCH_SIZE = 25;

async function injectCollector(cfg) {
  const [{ result } = {}] = await chrome.scripting.executeScript({
    target: { tabId: activeTab.id },
    func: collectSpotifyAnalytics,
    args: [{ endpoints: SPOTIFY_ENDPOINTS, ...cfg }],
  });
  return result || { error: "Injection returned nothing (page blocked scripting?)" };
}

async function sendPodcastStats(fullBackfill) {
  $("sendPodcast").disabled = true;
  try {
    const { apiBase, apiKey } = await chrome.storage.local.get(["apiBase", "apiKey"]);
    const lookback = parseInt($("podcastLookback").value, 10) || 10;
    chrome.storage.local.set({ podcastLookback: lookback });

    setStatus("Listing episodes…");
    const listing = await injectCollector({ phase: "list" });
    if (listing.error) {
      setStatus(`Episode list failed: ${listing.error}`, "err");
      return;
    }
    let episodes = (listing.episodes || []).filter((e) => e.episode_id);
    if (!episodes.length) {
      setStatus("No episodes found — check the endpoint table in spotify-collector.js (see its discovery notes).", "err");
      return;
    }
    if (!fullBackfill) episodes = episodes.slice(0, lookback);

    const batches = [];
    for (let i = 0; i < episodes.length; i += PODCAST_BATCH_SIZE) {
      batches.push(episodes.slice(i, i + PODCAST_BATCH_SIZE));
    }

    let written = 0;
    let matched = 0;
    const warnings = [];
    for (let i = 0; i < batches.length; i++) {
      setStatus(
        `Batch ${i + 1}/${batches.length} — collecting ${batches[i].length} episodes…\n(keep this popup open)`
      );
      // Show-level data (followers, demographics) rides on the first batch only.
      const collected = await injectCollector({
        phase: "collect",
        episodes: batches[i],
        includeShow: i === 0,
      });
      warnings.push(...(collected.errors || []));

      const resp = await fetch(`${apiBase}/podcast/import`, {
        method: "POST",
        headers: { "content-type": "application/json", "x-api-key": apiKey },
        body: JSON.stringify({
          source: "spotify",
          show: collected.show || null,
          episodes: collected.episodes || [],
          batch: { index: i + 1, total: batches.length, mode: fullBackfill ? "backfill" : "recent" },
        }),
      });
      const body = await resp.json().catch(() => ({}));
      if (!resp.ok) {
        // Report and continue — server upserts are idempotent, a re-run heals gaps.
        warnings.push(`batch ${i + 1} failed (${resp.status}): ${body.detail || resp.statusText}`);
        continue;
      }
      written += body.episodes_written || 0;
      matched += body.episodes_matched || 0;
      warnings.push(...(body.warnings || []));
      setStatus(`Batch ${i + 1}/${batches.length} — ${written} episodes written, ${matched} matched.`);
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
  if (confirm("Collect analytics for EVERY episode? ~4 requests per episode — takes a while, keep the popup open.")) {
    sendPodcastStats(true);
  }
});
