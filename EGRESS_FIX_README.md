# Railway egress fix — ETag/304 + market-hours SSE gating

## What this fixes

Railway egress (not compute) is driving the bill toward ~$88/mo. The backend
sends ~3 MB/s to the public internet because the dashboard **re-polls the same
read endpoints far more often than the data changes** — gzip was already
deployed, so compression was not the missing piece.

Two changes:

1. **`aignitequant/app/middleware.py` (new) — `ETagMiddleware`.**
   Adds an `ETag` to every `GET`/`HEAD` response and returns **`304 Not Modified`**
   (empty body) when the client re-polls unchanged data. A repeat poll drops from
   the full payload to a few header bytes. The SSE `/events` stream is passed
   through untouched, and it sits inside gzip so the ETag is computed on the
   stable uncompressed body.

2. **`aignitequant/app/main.py` — gate the market-pulse SSE publish.**
   `publish_update("market_pulse")` now fires **only while the market is open**.
   Previously it fired every cycle 24/7, making every open browser tab re-fetch
   the strategy tables overnight and on weekends (~65% of the week).

Combined with the gzip already in place, unchanged polls cost ~0 egress, which
should pull the projection back toward the ~$15/mo your own deploy plan targets.

## Your local git was corrupted — now repaired

`.git/config` had been zeroed with null bytes and `aignitequant/app/main.py` was
truncated — likely a cloud-sync tool (OneDrive/Dropbox) or an unclean write
mangling the `.git` folder. Your **deployed** app was unaffected (it runs from
GitHub `origin/main`).

I repaired `.git/config` in place, so **git works again — no re-clone needed**.
The rest of the working tree still has a cosmetic line-ending (CRLF) flip across
~34 files; the steps below discard that noise and land just the egress fix.

Run these in the repo root on your machine (PowerShell or Git Bash):

```bash
del .git\index.lock                   # clear a stale lock left by the in-place repair
git reset --hard origin/main          # clean deployed tree: drops the CRLF flip + restores main.py
git checkout -b cost-egress-fix
del aignitequant\app\middleware.py    # remove the stray copy so the patch recreates it cleanly
git apply egress-fix.patch            # adds middleware.py + edits main.py (the egress fix)
git add aignitequant/app/middleware.py aignitequant/app/main.py
git commit -m "Cut egress: ETag/304 conditional caching + gate market-pulse SSE to market hours"
git push -u origin cost-egress-fix
```

Then open a PR to `main`; Railway redeploys on merge. The patch was generated
against `origin/main` (the deployed commit), so it applies cleanly.

> To stop this recurring: make sure `C:\Projects\aignitequant_back_end` is **not**
> inside a OneDrive/Dropbox-synced folder — syncing a live `.git` directory is the
> usual cause of exactly this corruption.

## Verify after deploy

- First fetch of e.g. `/canslim_db` returns `200` with an `ETag` header.
- A re-fetch with `If-None-Match: <that etag>` returns `304` with no body.
- The SSE stream at `/events` still streams normally.
- Railway → service → Metrics: Public Network egress should fall sharply,
  especially overnight/weekends.

## Frontend (`aignitequant_ui`) — reviewed, no changes needed

I checked the UI code. It's already fully compatible with the ETag/304 path:

- All API calls use plain `fetch(API_BASE_URL + endpoint)` — no `cache: 'no-store'`,
  no `Cache-Control`/`Pragma` headers, and no cache-busting query params. So the
  browser is free to store responses and revalidate with `If-None-Match`.
- The SSE handler in `App.js` already refetches **only the strategy named in the
  event** (`forceRefetchStrategy`), and already skips refetches outside market
  hours (`isWithinMarketDataHours()`).
- `MarketIndices.js` fetches `/market-pulse` on mount and on SSE `market_pulse`
  events only — no rogue `setInterval` polling loop.

Because the browser revalidates automatically, the backend `Cache-Control: no-cache`
+ `ETag` (set by `ETagMiddleware`) is all that's required — those repeat polls
become `304`s with no frontend change. Nothing to deploy on the UI side.
