"""
test_client.py — candidate-visible smoke test

This script is a quick behavioral check for burst limits, isolation, and
recovery after waiting. It is not the primary concurrency grader.

Run server.py first, then run this file in another terminal.
"""

import asyncio
import sys

import httpx

BASE_URL = "http://localhost:8080"
LIMIT = 5
WINDOW_MS = 10_000
CAPACITY = 5
REFILL_RATE = 0.5


async def burst(client: httpx.AsyncClient, endpoint: str, client_id: str, count: int):
    headers = {"X-Client-ID": client_id}
    tasks = [client.post(f"{BASE_URL}{endpoint}", headers=headers) for _ in range(count)]
    return await asyncio.gather(*tasks)


async def expect_counts(
    client: httpx.AsyncClient,
    endpoint: str,
    client_id: str,
    count: int,
    expected_ok: int,
    expected_blocked: int,
    label: str,
):
    responses = await burst(client, endpoint, client_id, count)
    codes = [r.status_code for r in responses]
    got_ok = codes.count(200)
    got_blocked = codes.count(429)

    assert got_ok == expected_ok, f"{label}: expected {expected_ok} allowed, got {got_ok}"
    assert got_blocked == expected_blocked, f"{label}: expected {expected_blocked} rejected, got {got_blocked}"

    return responses


async def get_stats(client: httpx.AsyncClient):
    response = await client.get(f"{BASE_URL}/stats")
    response.raise_for_status()
    return response.json()


async def main():
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            stats = await get_stats(client)
            assert stats["total_allowed"] == 0 and stats["total_rejected"] == 0, (
                "Server stats are not fresh. Restart server before running test_client.py"
            )

            strategy = stats["strategy"]
            assert strategy in {"policy_a", "policy_b"}, f"Unknown strategy: {strategy}"

            await expect_counts(client, "/v1/chat", "alice", LIMIT + 1, LIMIT, 1, "same-key burst")
            await expect_counts(client, "/v1/chat", "bob", 1, 1, 0, "client isolation")
            await expect_counts(client, "/v1/complete", "alice", 1, 1, 0, "endpoint isolation")

            if strategy == "policy_a":
                await asyncio.sleep(WINDOW_MS / 1000 + 0.1)
                response = await client.post(f"{BASE_URL}/v1/chat", headers={"X-Client-ID": "alice"})
                assert response.status_code == 200, "policy_a recovery failed"
                assert int(response.headers["X-RateLimit-Remaining"]) == LIMIT - 1, "policy_a remaining mismatch"
                final_stats = await get_stats(client)
                assert final_stats == {
                    "strategy": "policy_a",
                    "total_allowed": LIMIT + 3,
                    "total_rejected": 1,
                    "active_keys": 3,
                }, f"unexpected final stats: {final_stats}"
            else:
                await asyncio.sleep((2 / REFILL_RATE) + 0.1)
                await expect_counts(client, "/v1/chat", "alice", 3, 2, 1, "policy_b refill")
                final_stats = await get_stats(client)
                assert final_stats == {
                    "strategy": "policy_b",
                    "total_allowed": LIMIT + 4,
                    "total_rejected": 2,
                    "active_keys": 3,
                }, f"unexpected final stats: {final_stats}"

            print("ALL TESTS PASSED")

    except AssertionError as e:
        print(f"TEST FAILED: {e}")
        sys.exit(1)
    except httpx.HTTPError as e:
        print(f"HTTP ERROR: {e}")
        print("Make sure server.py is running first.")
        sys.exit(1)
    except Exception as e:
        print(f"FATAL ERROR: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
