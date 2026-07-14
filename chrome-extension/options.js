// Persist the pipeline URL + API key to chrome.storage.local (this browser only).
const $ = (id) => document.getElementById(id);

async function load() {
  const {
    apiBase = "",
    apiKey = "",
    researchEnabled = true,
  } = await chrome.storage.local.get(["apiBase", "apiKey", "researchEnabled"]);
  $("apiBase").value = apiBase;
  $("apiKey").value = apiKey;
  $("researchEnabled").checked = researchEnabled;
}

async function save() {
  const apiBase = $("apiBase").value.trim().replace(/\/+$/, ""); // strip trailing slashes
  const apiKey = $("apiKey").value.trim();
  const researchEnabled = $("researchEnabled").checked;
  await chrome.storage.local.set({ apiBase, apiKey, researchEnabled });
  $("status").textContent = "Saved.";
  setTimeout(() => ($("status").textContent = ""), 1500);
}

document.addEventListener("DOMContentLoaded", load);
$("save").addEventListener("click", save);
