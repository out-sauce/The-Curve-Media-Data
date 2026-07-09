# Curve Auth Capture (Chrome extension)

Lifts a logged-in publisher session (cookies incl. HttpOnly + localStorage) from **your
real browser** and POSTs it to the pipeline's `/site-auth/import` endpoint, which stores
it as the Playwright `storage_state` the research scraper reads to beat paywalls.

Because the login happens in your normal browser and we only read the cookie jar
afterward, **no automation ever touches the publisher** — there is nothing for a bot
detector (PerimeterX/DataDome etc.) to flag. This is the durable alternative to the
Browserbase remote-login flow for sites hostile to remote browsers (WSJ, AFR, …).

## Install (load unpacked)

1. Chrome → `chrome://extensions` → toggle **Developer mode** on (top right).
2. **Load unpacked** → select this `chrome-extension/` folder.
3. Click the extension → it opens **Settings** the first time (or right-click →
   Options). Enter:
   - **Pipeline base URL** — e.g. `https://the-curve-media-data.up.railway.app` (no
     trailing slash).
   - **Pipeline API key** — the `PIPELINE_API_KEY` value. Stored only in this browser.

## Use

1. Log into the publisher normally (e.g. https://www.afr.com) — real human login.
2. While on a page of that site, click the extension → **Capture & send**.
3. It reports `✓ Imported for afr.com` with the cookie/origin counts. Done.

The session is keyed server-side by the **registrable base domain** (e.g. `afr.com`),
the same key the scraper reads — so `www.afr.com`, `afr.com`, etc. all resolve to one row.

## Notes / limits

- **Re-run when it expires.** Subscriber sessions lapse (~7–30 days); just capture again.
- **Where the auth cookie lives.** The popup grabs cookies applicable to the current page
  URL *and* the exact host + subdomains. If a site keeps its auth cookie on a different
  subdomain (e.g. an `accounts.` SSO host), navigate to a normal article page after
  logging in before capturing.
- **localStorage** is best-effort (top frame only); cookies alone are usually enough.
- **Permissions**: `cookies` (read HttpOnly session cookies), `scripting`/`activeTab`
  (read localStorage on the current tab), `storage` (save your URL + key), `<all_urls>`
  (publishers vary). Internal tool — not for the Web Store.
