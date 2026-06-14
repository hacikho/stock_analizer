# Railway Cost Reduction Analysis — `fearless-sparkle` / AigniteQuant

_Generated 2026-06-13 from the Railway dashboard + this codebase._

## First: a reality check on the $1,200 number

I could not find anything in your Railway account near $1,200/month. The actual numbers on your Usage page are:

| Metric | Value |
|---|---|
| Current usage (cycle May 19 – Jun 19) | **$19.83** |
| Railway's estimated full-cycle bill | **$62.64** |
| Compute hard limit | **$20.00** (you're at $18.99 — services will stop) |
| Plan | Hobby ($5 base, $5 usage included) |

So your real run-rate is roughly **$60/month**, not $1,200. The $1,200 figure may have been a worst-case projection, a different scenario, or a misread. Everything below cuts the real ~$60 bill **and** protects you from a blow-up if you scale or remove the cap — so it's worth doing either way. (If $1,200 came from a specific Railway Agent message, send it to me and I'll reconcile it.)

The more urgent issue: your **$20 compute hard limit is about to be hit**, which will stop all 6 services. The fixes below bring usage back under control so you don't have to just keep raising the cap.

## Where the money actually goes

Bill breakdown for the current cycle:

| Resource | Usage | Cost | % of bill |
|---|---|---|---|
| Memory | 36,804 GB-min | $8.52 | **43%** |
| Egress | 153.6 GB | $7.68 | **39%** |
| CPU | 5,972 vCPU-min | $2.76 | 14% |
| Agent | — | $0.84 | 4% |
| Volume | 6,839 GB-min | $0.02 | <1% |

CPU is cheap and low (peaks ~0.2 vCPU). **Memory and egress are the whole story.** Both trace back to a small number of code-level causes.

## Root causes (with the evidence)

**1. Market pulse runs every 30s, 24/7/365 — and runs twice.**
`scheduler.py` schedules `market-pulse-every-30sec` on Celery Beat, **and** `app/main.py` runs an *independent* `_market_pulse_loop()` doing the same fetch every 30s inside the API process. So you're fetching + writing Redis twice a minute, all day, every day — including nights and weekends when markets are closed (~80% of the week). This keeps the API, worker, beat, Redis, and Postgres permanently "hot," which is the biggest driver of the always-on **memory** bill.

**2. No response compression anywhere.**
There is no `GZipMiddleware` in `main.py`. Every strategy endpoint returns raw, uncompressed JSON (S&P 500-sized tables across 13 strategies). The frontend opens an SSE stream (`/events`) and re-fetches endpoints on each `strategy_update`. Uncompressed JSON is the bulk of your **153 GB egress**.

**3. 13 strategies recompute every 15 minutes.**
`scheduler.py` runs ~13 strategy tasks every 15 min, 4 AM–8 PM, weekdays — each loading full S&P 500 OHLCV from the DB into pandas. That's the **memory sawtooth** on `celery-worker` (climbing to ~1 GB then restarting). Several of these (golden cross, stage 2, VCP, earnings quality) are daily/swing signals that don't meaningfully change every 15 minutes.

**4. Six always-on services.**
`celery-beat` is a whole service that exists only to tick the schedule. On a per-minute memory model, each always-on service costs money even when idle.

## Recommendations (ordered by impact ÷ effort)

| # | Change | Effort | Est. saving | Cuts |
|---|---|---|---|---|
| 1 | Add GZip compression to the API | 2 lines | ~$5–6/cycle (≈70–85% of egress) | Egress |
| 2 | Remove the duplicate market-pulse loop | delete ~15 lines | worker/API load + Polygon calls | Memory/CPU |
| 3 | Gate market-pulse to market hours | ~5 lines | ~50–70% of pulse churn | Memory/CPU/egress |
| 4 | Fold `celery-beat` into the worker (`worker -B`) | config | one fewer always-on service | Memory |
| 5 | Slow down slow-signal strategies (15m → 30–60m) | edit schedule | shrinks the memory sawtooth | Memory/CPU |
| 6 | Frontend: refetch only the changed strategy | frontend | further egress cut | Egress |
| 7 | Enable app-sleep on the UI service | Railway toggle | idle-time savings | Memory |

Combined, items 1–5 realistically take the current ~$60/mo run-rate down toward **$20–30/mo** and, more importantly, pull compute back under the $20 cap.

### Quick win #1 — GZip (do this first)

In `aignitequant/app/main.py`:

```python
from fastapi.middleware.gzip import GZipMiddleware
# after app = FastAPI(...)
app.add_middleware(GZipMiddleware, minimum_size=500)
```

One change, no behavior difference, typically 70–85% less egress on JSON. This alone is the single biggest dollar saving.

### Quick win #2 — kill the duplicate market-pulse

You have two loops doing the same job. Keep one. The cleanest is to **remove the `_market_pulse_loop` from `main.py`** and let Celery Beat own it (or vice-versa) — but don't run both. This halves market-pulse Polygon calls and Redis writes immediately.

### Quick win #3 — don't poll a closed market

The `market-pulse-every-30sec` entry uses `timedelta(seconds=30)` with no hour/day guard, so it runs at 1:30 AM Saturday too. Gate it to extended trading hours/weekdays (e.g. a `crontab` window of `hour='4-20', day_of_week='1-5'`, or an in-task check that returns early when the market is closed). That removes the majority of off-hours runs that currently keep everything awake.

### Structural win — one fewer service

Run Beat embedded in the worker instead of as its own service:

```
celery -A aignitequant.tasks.celery_app worker -B --loglevel=info
```

Then delete the `celery-beat` service in Railway. (Embedded beat is fine for a single-worker setup like this one with `--pool=solo`.) That's a permanent always-on memory line item gone.

### Tuning win — strategy cadence

In `scheduler.py`, drop the daily/swing strategies from `*/15` to `*/30` or `*/60`:
`golden-cross`, `stage2`, `vcp-scanner`, `earnings-quality` (it already has a dedicated 6 AM run), and `follow-the-money`. Keep the fast/intraday ones at 15 min. This directly shrinks the worker memory peaks.

## What to do right now

1. Ship GZip (win #1) and remove the duplicate pulse loop (win #2) — biggest, safest wins.
2. Gate market-pulse to market hours (win #3).
3. Once usage drops below the cap, leave the $20 hard limit in place as a safety net rather than raising it.
4. Then consolidate beat into the worker and retune strategy cadence.

If you'd like, I can implement items 1–5 as a single commit on a branch so you can review the diff before deploying.
