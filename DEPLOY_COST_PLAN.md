# AigniteQuant — Path to ≤ $20/mo on Railway

Goal: keep Railway under **$20/month** at full performance (Polygon API is a
separate $30/mo cost). Last cycle ran ~$37. Here's how we get well under $20.

## 1. Deploy what's already built (this is the main win)

These changes are done in the working tree but **not yet committed/deployed**.
Deploying them is what actually lands you under $20.

| Change | Repo | Saves | Status |
|---|---|---|---|
| gzip API responses | back_end | ~$10/mo (egress −70-85%) | merged (PR #33) ✅ |
| Frontend memory fix (`server.js`, 800MB→~70MB) | ui | ~$7/mo (memory) | **pending — commit & deploy** |
| US market-holiday gating (`market_calendar.py` + task guards) | back_end | ~$1-2/mo | **pending — commit & deploy** |
| Beat embedded; standalone `celery-beat` deleted | back_end | one always-on container | done ✅ |

Deploy steps (from your machine, both repos):

```bash
# aignitequant_ui
git add server.js package.json railway.json
git commit -m "Cost: replace npx serve with low-memory static server"
git push           # triggers Railway deploy

# aignitequant_back_end
git add aignitequant/market_calendar.py aignitequant/tasks/strategy_tasks.py \
        aignitequant/app/main.py start_celery_worker.py
git commit -m "Cost: skip US market holidays; worker scale-to-zero (opt-in)"
git push
```

After the UI redeploys, confirm the new server in the deploy logs:
`Static server serving /app/build on port …` (not the old `serve` banner).

**Expected run-rate after step 1: ~$14–15/mo.** Already under $20.

## 2. (Optional) Worker scale-to-zero — extra margin

The `celery-worker` does no useful work nights/weekends/holidays. It's now
wired to power down off-hours via Railway cron, **off by default**. Enabling it
saves ~$1/mo more and — more usefully — restarts the worker fresh each morning,
which caps its memory growth.

Enable (do BOTH together, on the `celery-worker` service in Railway):

1. Variables → add `WORKER_SCALE_TO_ZERO = 1`
2. Settings → Cron Schedule → `0 8 * * 1-5`
   (UTC; = ~4 AM ET weekday start. Worker self-terminates at 8 PM ET and
   exits instantly on holidays.)

⚠️ Enable the env var and the cron **at the same time**. With the env var on
but no cron, nothing restarts the worker after it self-exits. To roll back,
remove the Cron Schedule (and/or set the var to 0).

Trade-off to know: in cron mode, Railway does not auto-restart a mid-session
crash — the worker would wait until the next morning's cron. Crash risk is low
(Celery isolates task errors), but this is the cost of true scale-to-zero. If
you'd rather not take that risk for ~$1/mo, leave this off; step 1 alone keeps
you under $20.

## 3. Guardrails

- Leave the **$40 compute cap** in place as a safety net (you're far under it).
- Keep the dashboard (frontend + API) always-on per your call — only the
  worker scales down.
- Ignore the Railway Agent's staged "increase memory to 1.5 GB" suggestion; it
  would raise the bill.

## Where the money goes after step 1 (est.)

| Service | Always-on memory | ~$/mo |
|---|---|---|
| Postgres | ~350 MB | ~$3.5 |
| celery-worker (market-hours work) | avg ~250 MB | ~$2.6 |
| stock_analizer (API) | ~150 MB | ~$1.5 |
| Redis | ~80 MB | ~$0.8 |
| stock_analizer_ui | ~70 MB | ~$0.7 |
| CPU (fetch + strategies, market hours) | — | ~$2-4 |
| Egress (gzipped) | — | ~$2-3 |
| **Total** | | **~$14-15** |
