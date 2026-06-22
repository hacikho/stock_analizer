"""
HTTP middleware for the AigniteQuant API.

ETagMiddleware
--------------
The dashboard polls the read-only endpoints (``/market-pulse`` and the
``*_db`` strategy tables) far more often than the underlying data actually
changes -- the frontend re-fetches on every SSE ``strategy_update`` event,
and several browser tabs may be open at once. Without conditional requests,
every one of those polls re-downloads the full (gzipped) response body, which
is the bulk of the project's Railway **egress** bill.

This middleware adds an ``ETag`` to every cacheable ``GET``/``HEAD`` response
and honours the browser's ``If-None-Match`` header: when the body hasn't
changed since the client last saw it, the server replies ``304 Not Modified``
with an empty body. The browser then serves its cached copy. Egress for an
unchanged poll drops from "full payload" to a few bytes of headers.

It also sets ``Cache-Control: no-cache`` on tagged responses. Despite the name,
``no-cache`` means "store it, but always revalidate before reuse" -- exactly
what a live dashboard wants: the browser keeps the body and, on the next poll,
sends ``If-None-Match`` so the server can answer ``304`` when nothing changed.
(Without an explicit directive, browsers fall back to heuristic freshness and
may skip the conditional request entirely, defeating the optimisation.)

It is a pure-ASGI middleware (not BaseHTTPMiddleware) so it can:
  * stream the SSE ``/events`` response straight through, never buffering it, and
  * sit *inside* GZipMiddleware so the ETag is computed over the stable,
    uncompressed body (compression output can vary, the source bytes don't).

Install order in ``main.py`` (add ETag BEFORE GZip so the response stack is
CORS -> GZip -> ETag -> app)::

    app.add_middleware(ETagMiddleware)
    app.add_middleware(GZipMiddleware, minimum_size=500)
    app.add_middleware(CORSMiddleware, ...)
"""

import hashlib


class ETagMiddleware:
    """Add ETag + Cache-Control + 304 support to GET/HEAD responses."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        # Only conditional-cache safe, idempotent reads. Everything else
        # (POST, etc.) passes through untouched.
        if scope["type"] != "http" or scope.get("method") not in ("GET", "HEAD"):
            await self.app(scope, receive, send)
            return

        # The validator the client already holds, if any.
        if_none_match = None
        for name, value in scope.get("headers", []):
            if name == b"if-none-match":
                if_none_match = value.decode("latin-1")
                break

        state = {"start": None, "streaming": False, "chunks": []}

        async def send_wrapper(message):
            mtype = message["type"]

            if mtype == "http.response.start":
                headers = message.get("headers", [])
                content_type = b""
                for k, v in headers:
                    if k == b"content-type":
                        content_type = v
                        break
                # Never buffer a streaming response (SSE / event-stream).
                if content_type.startswith(b"text/event-stream"):
                    state["streaming"] = True
                    await send(message)
                else:
                    state["start"] = message
                return

            if state["streaming"]:
                await send(message)
                return

            if mtype == "http.response.body":
                state["chunks"].append(message.get("body", b""))
                if message.get("more_body", False):
                    return

                start = state["start"]
                body = b"".join(state["chunks"])
                status = start["status"]
                # Drop headers we are going to (re)set ourselves.
                headers = [
                    (k, v)
                    for (k, v) in start.get("headers", [])
                    if k not in (b"etag", b"cache-control")
                ]

                # Only attach an ETag to successful, non-empty bodies.
                if status == 200 and body:
                    etag = '"%s"' % hashlib.md5(body).hexdigest()
                    headers.append((b"etag", etag.encode("latin-1")))
                    # "no-cache" == store but always revalidate -> the browser
                    # sends If-None-Match on every poll and we answer 304 when
                    # unchanged. This is what makes the 304 path reliable.
                    headers.append((b"cache-control", b"no-cache"))

                    if if_none_match is not None and _etag_matches(if_none_match, etag):
                        not_modified = [
                            (k, v)
                            for (k, v) in headers
                            if k not in (b"content-length", b"content-encoding")
                        ]
                        await send(
                            {
                                "type": "http.response.start",
                                "status": 304,
                                "headers": not_modified,
                            }
                        )
                        await send({"type": "http.response.body", "body": b""})
                        return

                    await send(
                        {
                            "type": "http.response.start",
                            "status": status,
                            "headers": headers,
                        }
                    )
                    await send({"type": "http.response.body", "body": body})
                    return

                # Anything else (non-200, empty body): replay untouched.
                await send(start)
                await send({"type": "http.response.body", "body": body})

        await self.app(scope, receive, send_wrapper)


def _etag_matches(if_none_match: str, etag: str) -> bool:
    """
    True if the client's If-None-Match validator covers our ETag.

    Handles the ``*`` wildcard, comma-separated lists, and the weak-validator
    ``W/`` prefix (weak comparison is correct for cache validation).
    """
    candidates = [c.strip() for c in if_none_match.split(",")]
    if "*" in candidates:
        return True
    bare = etag.lstrip("W/").strip()
    for c in candidates:
        if c.lstrip("W/").strip() == bare:
            return True
    return False
