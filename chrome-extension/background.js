// Research content-grab lane.
//
// Polls the pipeline for articles an editor has queued (Admin "Research" button), opens
// each in a background tab of THIS real, logged-in browser, reads the rendered article
// HTML, and posts it back to /research/import. Because the page is fetched by a human's
// actual browser — real IP, real fingerprint, real subscriber session — there is nothing
// for a bot detector (PerimeterX/DataDome) to flag, so paywalled/anti-bot publishers that
// the server-side scraper cannot touch are researched here instead.
//
// Enabled only when the pipeline URL + API key are set (options page) and the
// "auto-research" toggle is on. Runs on a chrome.alarms tick so it survives the MV3
// service-worker lifecycle.

const POLL_ALARM = "curve-research-poll";
const POLL_MINUTES = 1;          // chrome.alarms floor; ~1 min latency, fine for editorial
const CLAIM_LIMIT = 3;           // articles grabbed per tick
const LOAD_TIMEOUT_MS = 30000;   // max wait for a tab to finish loading
const SETTLE_MS = 4000;          // let late-loading article/paywall scripts render

let busy = false;                // guard against overlapping ticks within one SW instance

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function getConfig() {
  const { apiBase, apiKey, researchEnabled } = await chrome.storage.local.get([
    "apiBase",
    "apiKey",
    "researchEnabled",
  ]);
  // Default on when configured, so upgrading the extension doesn't silently disable it.
  const enabled = researchEnabled === undefined ? true : !!researchEnabled;
  return { apiBase, apiKey, enabled };
}

async function claimItems(apiBase, apiKey) {
  const resp = await fetch(`${apiBase}/research/queue/claim?limit=${CLAIM_LIMIT}`, {
    method: "POST",
    headers: { "x-api-key": apiKey },
  });
  if (!resp.ok) throw new Error(`claim ${resp.status}`);
  const body = await resp.json().catch(() => ({}));
  return body.items || [];
}

// Resolve once the tab reports status 'complete', or after LOAD_TIMEOUT_MS.
async function waitForLoad(tabId) {
  const deadline = Date.now() + LOAD_TIMEOUT_MS;
  while (Date.now() < deadline) {
    let tab;
    try {
      tab = await chrome.tabs.get(tabId);
    } catch (e) {
      return; // tab gone
    }
    if (tab.status === "complete") return;
    await sleep(500);
  }
}

async function grabHtml(tabId) {
  const [{ result } = {}] = await chrome.scripting.executeScript({
    target: { tabId },
    func: () => document.documentElement.outerHTML,
  });
  return result || "";
}

async function importArticle(apiBase, apiKey, item, html) {
  const resp = await fetch(`${apiBase}/research/import`, {
    method: "POST",
    headers: { "content-type": "application/json", "x-api-key": apiKey },
    body: JSON.stringify({
      queue_id: item.queue_id,
      article_id: item.article_id,
      html,
    }),
  });
  const body = await resp.json().catch(() => ({}));
  if (!resp.ok) throw new Error(`import ${resp.status}: ${body.detail || ""}`);
  return body;
}

async function processItem(apiBase, apiKey, item) {
  if (!/^https?:/.test(item.url || "")) return;
  const tab = await chrome.tabs.create({ url: item.url, active: false });
  try {
    await waitForLoad(tab.id);
    await sleep(SETTLE_MS);
    const html = await grabHtml(tab.id);
    const res = await importArticle(apiBase, apiKey, item, html);
    console.log(`Curve research: article ${item.article_id} → ${res.status}`);
  } catch (e) {
    // The server marks the queue row failed only on a successful import call; a thrown
    // error here (tab/load/network) leaves the row 'claimed' — re-enqueue from Admin to
    // retry. Logged for the operator.
    console.warn(`Curve research: article ${item.article_id} failed — ${e.message}`);
  } finally {
    try {
      await chrome.tabs.remove(tab.id);
    } catch (e) {
      /* already closed */
    }
  }
}

async function poll() {
  if (busy) return;
  busy = true;
  try {
    const { apiBase, apiKey, enabled } = await getConfig();
    if (!enabled || !apiBase || !apiKey) return;
    const items = await claimItems(apiBase, apiKey);
    for (const item of items) {
      await processItem(apiBase, apiKey, item); // sequential: one tab at a time
    }
  } catch (e) {
    console.warn(`Curve research poll error: ${e.message}`);
  } finally {
    busy = false;
  }
}

function ensureAlarm() {
  chrome.alarms.create(POLL_ALARM, { periodInMinutes: POLL_MINUTES });
}

chrome.runtime.onInstalled.addListener(ensureAlarm);
chrome.runtime.onStartup.addListener(ensureAlarm);
chrome.alarms.onAlarm.addListener((alarm) => {
  if (alarm.name === POLL_ALARM) poll();
});
