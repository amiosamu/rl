import time
import threading
from collections import deque
from typing import Dict, Tuple, TypedDict

Key = Tuple[str, str]
Decision = Tuple[bool, int, int]


class StatsDict(TypedDict):
    strategy: str
    total_allowed: int
    total_rejected: int
    active_keys: int


class RateLimiter:
    def __init__(self, strategy: str = "policy_a"):
        if strategy not in ("policy_a", "policy_b"):
            raise ValueError(f"Unknown strategy: {strategy}")

        self.strategy = strategy
        self._lock = threading.Lock()  # protects self._state across OS threads

        # Generic per-key state store. You may choose any internal representation
        # that preserves the required behavior and return contract.
        self._state: Dict[Key, object] = {}

        self._total_allowed = 0
        self._total_rejected = 0

    def now_ms(self) -> int:
        return int(time.monotonic() * 1000)

    def check(
        self,
        client_id: str,
        endpoint: str,
        limit: int,
        window_ms: int,
        capacity: int,
        refill_rate: float,
    ) -> Decision:
        """
        Run the configured policy and return (allowed, remaining, reset_after_ms).

        Policy A uses: limit, window_ms
        Policy B uses: capacity, refill_rate

        remaining:
            Remaining capacity after the current decision.

        reset_after_ms:
            Milliseconds until capacity becomes available again.
        """
        with self._lock:
            if self.strategy == "policy_a":
                result = self.policy_a_limit(client_id, endpoint, limit, window_ms)
            else:
                result = self.policy_b_limit(client_id, endpoint, capacity, refill_rate)

            if result[0]:
                self._total_allowed += 1
            else:
                self._total_rejected += 1

            return result

    def policy_a_limit(
        self,
        client_id: str,
        endpoint: str,
        limit: int,
        window_ms: int,
    ) -> Decision:
        """
        Policy A:
        For each (client_id, endpoint), allow at most `limit` requests during
        the most recent `window_ms` milliseconds.
        """
        now = self.now_ms()
        key = (client_id, endpoint)
        log = self._state.setdefault(key, deque())  # per-key timestamp history
        cutoff = now - window_ms

        while log and log[0] <= cutoff:  # evict everything outside the window
            log.popleft()

        if len(log) < limit:
            log.append(now)  # accepted request joins the window
            reset_after_ms = log[0] + window_ms - now  # oldest surviving entry decides the reset
            return (True, limit - len(log), reset_after_ms)

        # still full — oldest entry is what frees a slot
        reset_after_ms = log[0] + window_ms - now
        return (False, 0, reset_after_ms)

    def policy_b_limit(
        self,
        client_id: str,
        endpoint: str,
        capacity: int,
        refill_rate: float,
    ) -> Decision:
        """
        Policy B:
        For each (client_id, endpoint), maintain a request budget with maximum
        size `capacity`. The budget increases over time at `refill_rate` units
        per second, up to `capacity`. Each accepted request consumes 1 unit.
        """
        now = self.now_ms()
        key = (client_id, endpoint)
        tokens, last_update = self._state.get(key, (capacity, now))  # fresh bucket starts full
        elapsed_ms = now - last_update
        tokens = min(capacity, tokens + (elapsed_ms / 1000) * refill_rate)  # continuous refill, capped

        if tokens >= 1:
            tokens -= 1  # spend one token
            self._state[key] = (tokens, now)
            return (True, int(tokens), 0)  # remaining = whole tokens left; never rounded before this

        self._state[key] = (tokens, now)  # persist the partial refill even on rejection
        if refill_rate > 0:
            deficit = 1 - tokens
            reset_after_ms = int((deficit / refill_rate) * 1000)  # time until 1 full token accrues
        else:
            reset_after_ms = 0  # refill_rate == 0: capacity will never recover
        return (False, 0, reset_after_ms)

    def stats(self) -> StatsDict:
        """
        Return aggregate limiter statistics.

        active_keys counts tracked (client_id, endpoint) entries for the active
        policy state.
        """
        with self._lock:
            return {
                "strategy": self.strategy,
                "total_allowed": self._total_allowed,
                "total_rejected": self._total_rejected,
                "active_keys": len(self._state),
            }
