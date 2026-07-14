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
   - **Pipeline base URL** — `https://the-curve-media-data-production.up.railway.app`
     (include `https://`, no trailing slash).
   - **Pipeline API key** — the `PIPELINE_API_KEY` value. Stored only in this browser.

## Use

1. Log into the publisher normally (e.g. https://www.afr.com) — real human login.
2. While on a page of that site, click the extension → **Capture & send**.
3. It reports `✓ Imported for afr.com` with the cookie/origin counts. Done.

The session is keyed server-side by the **registrable base domain** (e.g. `afr.com`),
the same key the scraper reads — so `www.afr.com`, `afr.com`, etc. all resolve to one row.

## Auto-research (content-grab lane)

Beyond capturing logins, this extension also fetches **article text** for publishers the
server-side scraper can't reach (paywalled / anti-bot sites marked `manual` in the
pipeline). It works the same way — the page is loaded in *your* real, logged-in browser,
so there is nothing for a bot detector to flag.

Flow: an editor clicks **Research** on an article in the Admin app → the pipeline queues
it → this extension (running in your browser) polls the queue about once a minute, opens
the article in a **background tab** using your session, reads the rendered text, and sends
it back to `<base>/research/import`. The editor never leaves Admin; the article fills in a
minute or so later.

Enable it on the Settings page ("Enable auto-research", on by default once the URL + key
are set). **Keep this browser open and logged into the publishers you cover** — the lane
only runs while the browser is running. Turn the toggle off to stop polling.

### Send the article you're reading (popup button)

Already on the article page? Click the extension → **Send article content**. It posts the
current tab's rendered HTML straight to `/research/import`, keyed by the page URL — the
server matches it to the pipeline article (you'll get an error if the page isn't one),
runs the same extract + summarise path, and closes any outstanding queue request for that
article. No Admin round-trip, no background tab.

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
