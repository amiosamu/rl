# Reflection

## Q1: `threading.Lock()` vs `asyncio.Lock()`

`rate_limiter.py` guards all shared state with a single `threading.Lock()`:

```python
class RateLimiter:
    def __init__(self, strategy: str = "policy_a"):
        ...
        self._lock = threading.Lock()  # protects self._state across OS threads

    def check(self, ...) -> Decision:
        with self._lock:
            ...
            return result
```

This is the right choice given the stated constraint — "calls may arrive from several server threads at the same time." `threading.Lock` is an OS-level, blocking primitive: it works no matter which OS thread calls `check()`, sync or async caller alike.

**Why not `asyncio.Lock()` here:**

- `asyncio.Lock` only coordinates *coroutines scheduled on the same event loop*. It has no concept of OS threads — if `check()` were ever called from a different thread than the one running the event loop (e.g. via `loop.run_in_executor(...)`, a thread pool, or a separate sync worker), an `asyncio.Lock` would give **zero** protection against a real data race, because threads never go through the event loop's cooperative scheduler at all.
- `asyncio.Lock` also requires `await` to acquire (`async with lock:`), which would force `check()` itself to become `async` for no real benefit — the work inside is pure CPU/in-memory logic, not I/O.

**The tradeoff `threading.Lock` carries:**

If this server only ever runs single-process, single-thread asyncio (no thread pool, no multiprocessing), the lock never actually contends — `aiohttp`'s event loop runs one coroutine at a time, and `check()` performs no `await` inside the `with self._lock:` block, so there's no window for interleaving. The lock is "free" insurance in that deployment. But it's also a latent footgun: if anyone ever adds an `await` *inside* that block, `threading.Lock.acquire()` would block the entire OS thread running the event loop — stalling *every* coroutine on it, not just the one holding the lock. `asyncio.Lock` wouldn't have this failure mode, since yielding at an `await` is exactly what it's built to coordinate around.

**Bottom line:** pick the lock that matches the actual concurrency model, not the framework. This server is specified to receive calls from multiple threads, so `threading.Lock` is correct — but it comes with an implicit rule ("never `await` while holding it") that isn't enforced by the type system, only by discipline.

## Q2: Evolving to a multi-instance, horizontally scaled deployment

The current design keeps all state local to one process:

```python
self._state: Dict[Key, object] = {}   # lives only in this process's memory
self._lock = threading.Lock()          # only coordinates threads of THIS process
```

That's fine for one instance, but breaks the moment the gateway scales horizontally. If a load balancer spreads `client_id="alice"`'s requests across 3 replicas, each replica enforces `limit` independently — Alice's *effective* limit becomes `limit × 3`, not `limit`. The `threading.Lock` also stops meaning anything at that point, since it only ever coordinated threads inside one process and has no reach across machines.

**The fix: move `self._state` out of the process into a shared store (Redis).** Memcached is usually a poor fit here — it lacks atomic scripting (Lua `EVAL`) and the data structures (sorted sets, hashes) this needs; Redis is the standard choice for distributed rate limiting.

- **Policy A (sliding window log)** maps onto a Redis **sorted set** per `(client_id, endpoint)`, timestamp as both member and score:

  ```text
  ZREMRANGEBYSCORE key -inf (now - window_ms)   # evict expired entries
  ZCARD key                                      # count what's left
  # if count < limit: ZADD key now now           # accept
  ```

  These steps must run atomically — exactly the job `self._lock` does today. Across the network that means a Lua script via `EVAL` (one round trip, atomic server-side), not separate round trips, which would reopen the same check-then-act race the in-process lock prevents.

- **Policy B (token bucket)** maps onto a Redis hash holding `tokens` + `last_update` per key. The same GET → compute-refill → compare → conditionally-decrement-and-SET sequence has to be one atomic Lua script server-side — otherwise two instances could both read "2.5 tokens available" and both accept, double-spending the same tokens.

**Other things that change, beyond swapping the dict for Redis:**

- *TTLs*: the in-memory `dict` never evicts idle keys today — a client that sends one request and vanishes leaves its entry in `self._state` forever (a real, if slow, memory leak that exists even in the current single-instance version). Moving to Redis is a natural point to fix this with `EXPIRE` on each key (e.g. `window_ms`, or a few multiples of `capacity / refill_rate`).
- *Latency*: every `check()` becomes a network round trip instead of an in-memory lookup, directly on the request hot path. Mitigate by colocating Redis near the app tier, or by accepting slightly approximate limiting via local caching + periodic reconciliation if strict precision isn't worth the latency.
- *`stats()`*: `self._total_allowed` / `self._total_rejected` are process-local ints; across instances they'd need to become Redis `INCR`s (atomic, shared) or be pushed to a metrics system (e.g. Prometheus) and aggregated there instead of read from one instance's memory.
- *Failure mode*: Redis becomes a new dependency on the critical path. If it's unreachable, does the gateway fail-open (let requests through, prioritizing availability) or fail-closed (reject everything, prioritizing the rate-limit guarantee)? That's a product decision that needs to be made explicitly, not whatever the client library happens to do on a connection error.

---

# Policies Explained

## Policy A — Sliding Window (log)

We keep the actual timestamp of every accepted request for a key, in a `deque`. On each check, we drop everything older than `now - window_ms`, then count what's left: if it's under `limit`, the request is accepted and its timestamp is appended; otherwise it's rejected.

```python
now = self.now_ms()
key = (client_id, endpoint)
log = self._state.setdefault(key, deque())    # per-key timestamp history
cutoff = now - window_ms

while log and log[0] <= cutoff:                # evict everything outside the window
    log.popleft()

if len(log) < limit:
    log.append(now)                            # accepted request joins the window
    reset_after_ms = log[0] + window_ms - now   # oldest surviving entry decides the reset
    return (True, limit - len(log), reset_after_ms)

reset_after_ms = log[0] + window_ms - now       # still full — oldest entry is what frees a slot
return (False, 0, reset_after_ms)
```

Because the window is defined by real timestamps rather than a fixed calendar interval, it "slides" continuously — there's no boundary effect where two bursts straddling the edge of adjacent fixed intervals combine to exceed `limit` within less than `window_ms`. The cost is memory: up to `limit` timestamps retained per key, instead of a single counter.

## Policy B — Token Bucket

Each key gets a bucket that starts full with `capacity` tokens. Every accepted request spends 1 token. Tokens regenerate continuously — based on exact elapsed time, not discrete ticks — at `refill_rate` per second, capped at `capacity`.

```python
now = self.now_ms()
key = (client_id, endpoint)
tokens, last_update = self._state.get(key, (capacity, now))        # fresh bucket starts full

elapsed_ms = now - last_update
tokens = min(capacity, tokens + (elapsed_ms / 1000) * refill_rate)  # continuous refill, capped

if tokens >= 1:
    tokens -= 1                       # spend one token
    self._state[key] = (tokens, now)
    return (True, int(tokens), 0)     # remaining = whole tokens left; never rounded before this

self._state[key] = (tokens, now)      # persist the partial refill even on rejection
if refill_rate > 0:
    deficit = 1 - tokens
    reset_after_ms = int((deficit / refill_rate) * 1000)  # time until 1 full token accrues
else:
    reset_after_ms = 0                 # refill_rate == 0: capacity will never recover
return (False, 0, reset_after_ms)
```

This allows **bursts**: an idle client can spend its entire `capacity` instantly, then has to wait for tokens to trickle back at `refill_rate`. `capacity` and `refill_rate` are independent knobs — burst allowance vs. steady-state rate — which is why token bucket is the more common choice for APIs that want to tolerate occasional spikes while still bounding the long-run average.
