// Persist the pipeline URL + API key to chrome.storage.local (this browser only).
const $ = (id) => document.getElementById(id);

async function load() {
  const { apiBase = "", apiKey = "" } = await chrome.storage.local.get(["apiBase", "apiKey"]);
  $("apiBase").value = apiBase;
  $("apiKey").value = apiKey;
}

async function save() {
  const apiBase = $("apiBase").value.trim().replace(/\/+$/, ""); // strip trailing slashes
  const apiKey = $("apiKey").value.trim();
  await chrome.storage.local.set({ apiBase, apiKey });
  $("status").textContent = "Saved.";
  setTimeout(() => ($("status").textContent = ""), 1500);
}

document.addEventListener("DOMContentLoaded", load);
$("save").addEventListener("click", save);
