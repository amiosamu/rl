"""
In-Memory Rate Limiter for an LLM API Gateway
=============================================

The HTTP server is already implemented. Do not modify this file.
Implement the RateLimiter class in rate_limiter.py.

The active policy is selected with the STRATEGY environment variable:
- policy_a
- policy_b
"""

import os
from aiohttp import web
from rate_limiter import RateLimiter

STRATEGY = os.environ.get("STRATEGY", "policy_a")
if STRATEGY not in {"policy_a", "policy_b"}:
    raise ValueError(f"Unsupported STRATEGY: {STRATEGY}")

limiter = RateLimiter(strategy=STRATEGY)

LIMIT = 5
WINDOW_MS = 10_000
CAPACITY = 5
REFILL_RATE = 0.5


async def handle_completion(request: web.Request) -> web.Response:
    client_id = request.headers.get("X-Client-ID", "anonymous")
    endpoint = request.path

    allowed, remaining, reset_after_ms = limiter.check(
        client_id=client_id,
        endpoint=endpoint,
        limit=LIMIT,
        window_ms=WINDOW_MS,
        capacity=CAPACITY,
        refill_rate=REFILL_RATE,
    )

    headers = {
        "X-RateLimit-Remaining": str(remaining),
        "X-RateLimit-Reset-Ms": str(reset_after_ms),
        "X-RateLimit-Policy": STRATEGY,
    }

    if not allowed:
        return web.Response(status=429, text="Rate limit exceeded", headers=headers)

    return web.json_response(
        {
            "reply": f"Hello from {endpoint}",
            "client": client_id,
            "policy": STRATEGY,
        },
        headers=headers,
    )


async def handle_stats(_: web.Request) -> web.Response:
    return web.json_response(limiter.stats())


app = web.Application()
app.router.add_post("/v1/chat", handle_completion)
app.router.add_post("/v1/complete", handle_completion)
app.router.add_get("/stats", handle_stats)

if __name__ == "__main__":
    print(f"Rate limiter server listening on http://localhost:8080 (policy={STRATEGY})")
    web.run_app(app, port=8080)
