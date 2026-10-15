
# Q1: In-memory rate limiter for an LLM API gateway

## What you should know

You will need a working knowledge of:

- Rate limiting concepts
- Sliding window
- Token bucket
- Shared state under concurrency
- Basic locking

---

## Scenario

You are adding rate limiting to an LLM API gateway. Several clients share endpoints such as `/v1/chat` and `/v1/complete`, so the gateway needs to stop one client from taking all of the available capacity.

The project includes the HTTP server. Implement the `RateLimiter` class in `rate_limiter.py`. The server calls it before accepting a request. Calls may arrive from several server threads at the same time, so access to shared limiter state must be safe. You do not need to create threads or implement a multi-threaded server.

---

## Goal

Build an in-memory limiter with two policies. Configuration decides which policy is active at runtime.

Each check receives three pieces of information:

- `client_id`
- `endpoint`
- policy parameters

It returns the decision and two pieces of client-facing metadata:

- `allowed: bool`
- `remaining: int`
- `reset_after_ms: int`

---

## Return contract

`RateLimiter.check(...)` must return a tuple:

```python
(allowed, remaining, reset_after_ms)
```

### allowed

Set `allowed` to `True` when the request can proceed. A rejected request must not consume capacity or appear in the history of accepted requests.

### remaining

Calculate `remaining` after making the current decision.

- If the request is accepted, `remaining` is the number of additional requests that can be accepted immediately for the same `(client_id, endpoint)` before the limiter would reject another request.
- If the request is rejected, `remaining` must be `0`.

Only capacity available right now counts. Do not include capacity that will appear later at `reset_after_ms`.

### reset_after_ms

`reset_after_ms` is a non-negative integer measured in milliseconds. If a calculation produces a fractional millisecond, truncate it toward `0`.

Its meaning changes slightly between the two policies:

- For Policy A, it is the time until the oldest currently counted accepted request is no longer counted.
- For Policy B, it is `0` when the current request is accepted, and otherwise the time until one full token becomes available.

---

## Policies

### General

- State must be isolated per `(client_id, endpoint)`.
- `remaining` is measured after the current decision.
- Rejected requests must return `remaining = 0`.
- All timer values must be non-negative integers.
- `check(...)` takes no time argument. The time of a request is the moment `check(...)` is called, so read the clock inside the limiter. In the examples below, values such as `0 ms` and `3000 ms` are the time elapsed since the first request for that `(client_id, endpoint)`. They are not arguments.

### Policy A

For each `(client_id, endpoint)`, it allows at most `limit` requests in the latest `window_ms` milliseconds.

- Only accepted requests inside the most recent `window_ms` should count.
- Expired requests must be removed before evaluating the current request.
- If the current request is accepted, it becomes part of the counted window.
- `remaining` is `limit - number_of_counted_requests` after the current decision.
- `reset_after_ms` is the time until the oldest currently counted accepted request is no longer counted.

An accepted request can therefore have a positive `reset_after_ms`.

#### Examples: exact limit and rejection

Parameters:

```text
limit = 2
window_ms = 10000
```

For the same `(client_id, endpoint)`:

```text
0 ms     -> accepted
3000 ms  -> accepted
3500 ms  -> rejected
```

Expected return values:

```text
0 ms     -> (True,  1, ~10000)
3000 ms  -> (True,  0, ~7000)
3500 ms  -> (False, 0, ~6500)
```

Why:

- At `0 ms`, the first accepted request is now the oldest counted request. It expires at `10000 ms`, so `reset_after_ms` is ~`10000`.
- At `3000 ms`, the second request is accepted. There are no remaining slots, and the oldest counted request still expires at `10000 ms`, so `reset_after_ms` is ~`7000`.
- At `3500 ms`, the window is full, so the request is rejected. `remaining` is `0`, and the oldest counted request expires in ~`6500 ms`.

#### Examples: recovery after expiry

Parameters:

```text
limit = 2
window_ms = 120
```

For the same `(client_id, endpoint)`:

```text
0 ms    -> accepted
0 ms    -> accepted
140 ms  -> accepted
```

At `140 ms`, neither earlier request is still in the window. The new request is accepted and is now the only timestamp being counted.

Expected return value at `140 ms`:

```text
(True, 1, ~120)
```


### Policy B

Each `(client_id, endpoint)` gets its own bucket, which starts with `capacity` tokens. Every accepted request spends one token. Tokens are added back continuously at `refill_rate` tokens per second, but the bucket can never hold more than `capacity`.

- The budget must refill continuously over elapsed time.
- The internal budget may be fractional. Do not round it before deciding whether to accept or reject a request.
- The budget must never exceed `capacity`.
- Each accepted request consumes exactly 1 unit.
- If the request is accepted, `remaining` is the number of whole tokens left after consuming 1 token.
- If the request is accepted, `reset_after_ms` must be `0`.
- If the request is rejected and `refill_rate > 0`, `reset_after_ms` is the truncated integer time until 1 full token becomes available.
- If the request is rejected and `refill_rate == 0`, return any non-negative integer for `reset_after_ms`, since capacity will not recover.

> Configuration chooses the active policy at runtime.

#### Examples: initial burst

Parameters:

```text
capacity    = 3
refill_rate = 0.5
```

A new bucket starts full, so for the same `(client_id, endpoint)`:

```text
request 1 -> (True,  2, 0)
request 2 -> (True,  1, 0)
request 3 -> (True,  0, 0)
request 4 -> (False, 0, ~2000)
```

The first three requests empty the bucket. At `0.5` tokens per second, the fourth request has to wait about `2` seconds for a complete token.


#### Examples: partial refill

Parameters:

```text
capacity    = 3
refill_rate = 0.5
```

Suppose the bucket is empty. After `1200 ms`:

```text
0.5 × 1.2 = 0.6  =>  budget = 0.6
```

Since the budget is below 1 unit, the request is rejected. To reach 1 full token:

```text
1 - 0.6 = 0.4 units
0.4 ÷ 0.5 = 0.8 s = 800 ms
```

Expected return:

```text
(False, 0, ~800)
```

---

## Implementation checklist

The implementation must:

- implement Policy A
- implement Policy B
- track state independently for each `(client_id, endpoint)` pair
- return `(allowed, remaining, reset_after_ms)` correctly
- preserve compatibility with the existing `server.py`
- keep shared state correct under concurrent requests from the provided server
- maintain correct stats in `stats()`


---

## Files

The starter project contains:

- `server.py`: completed server, provided for reference
- `rate_limiter.py`: your implementation file
- `test_client.py`: smoke test against the running server
- `README.md`: this file
- `requirements.txt`: local dependencies
- `REFLECTION.essay`: your thoughts on locking and the multi-instance evolution path

Make all changes in `rate_limiter.py` and `REFLECTION.essay`. You may replace its starter implementation as long as `RateLimiter.check(...)`, `stats()`, and the return contract expected by `server.py` stay intact.


---

## Estimated completion time

45 minutes

---

## Running locally

Install dependencies:

```bash
python3 -m pip install -r requirements.txt
```

Run the server:

```bash
# Policy A
STRATEGY=policy_a python3 server.py

# Policy B
STRATEGY=policy_b python3 server.py
```

In another terminal, run the smoke test:

```bash
python3 test_client.py
```
