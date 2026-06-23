# THE egress fix — SSE `/events` keepalive hot loop

## This is the real driver (bigger than the ETag fix)

After deploying the ETag/304 fix (PR #39), egress was still climbing — Railway's
Network Flow Logs showed **~9 MB egressing to a single internet connection every
5 seconds, continuously** (~1.8 MB/s per connection, ~150 GB/day), with **no
matching GET in the access logs**. That signature is a long-lived SSE `/events`
connection streaming far too much data.

**Root cause:** in `aignitequant/app/api/routes.py`, `_sse_event_generator()` did:

```python
message = await asyncio.wait_for(
    pubsub.get_message(ignore_subscribe_messages=True),  # default timeout=0.0 → NON-BLOCKING
    timeout=25.0,
)
```

`redis.asyncio`'s `get_message()` defaults to `timeout=0.0`, which is
**non-blocking** — it returns `None` immediately when no message is queued. So
the 25 s timeout on `asyncio.wait_for()` was never reached. The loop fell
straight into the keepalive branch and streamed `": ping\n\n"` **as fast as the
client would accept it** — thousands of pings/second, ~1.8 MB/s per connected
browser tab, 24/7 (the loop doesn't care whether the market is open). Every open
dashboard tab held one of these, which is the continuous egress climb.

## The fix

Pass the timeout to `get_message()` so it actually suspends the coroutine until a
message arrives or 25 s elapses — at most **one ping per 25 s** of idleness:

```python
message = await pubsub.get_message(
    ignore_subscribe_messages=True, timeout=25.0
)
```

(The redundant `asyncio.wait_for` wrapper and its `TimeoutError` handler are
removed.) Verified with a mock pubsub: the old loop emits ~23,000 pings/s; the
fixed loop emits ~0.04 pings/s — a 5-6 orders-of-magnitude drop on the stream.
No behavior change for clients: they still get a keepalive every 25 s and real
`strategy_update` events instantly.

## How the three fixes relate

1. **ETag/304** (PR #39, already deployed) — correctly cuts the *polling* egress
   (the 304s are visible and working in the HTTP logs). Keep it.
2. **Market-hours SSE publish gate** (PR #39) — fewer redundant refetch triggers.
   Keep it.
3. **This SSE ping-loop fix** — eliminates the dominant ~150 GB/day flood. **This
   is the one that actually craters the bill.**

## Deploy

```bash
del .git\index.lock                   # if the stale lock is still there
git checkout -b sse-ping-fix origin/main
git apply sse-ping-fix.patch
git add aignitequant/app/api/routes.py
git commit -m "Fix SSE /events keepalive hot loop: block in get_message(timeout=25) instead of streaming pings nonstop"
git push -u origin sse-ping-fix
```

Open a PR to `main`; Railway redeploys on merge.

## Verify after deploy

- Open the dashboard, then Railway → `stock_analizer` → **Network Flow Logs**: the
  recurring multi-MB egress flows to "Internet" should be gone — left only with
  small periodic flows (real data + 304s).
- The egress line on **Metrics → Public Network Traffic** should go flat.
- Within a day, the Estimated Bill should fall back toward the ~$15/mo target
  (egress was the entire overage).
