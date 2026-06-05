"""
resource.py — Protected resource with fencing-token validation.

Represents a shared entity (billing ledger, inventory record) that must never
accept concurrent writes. The ProtectedCounter cooperates with the lock protocol
by validating fence tokens on every write.

Why token validation here and not just in the lock client?
  The lock client's lost_lock flag is a best-effort signal: it detects that a
  renewal failed, but there is an unavoidable race between the last heartbeat
  and a write that is already in flight. The resource is the final authority.
  A stale worker that somehow bypasses or delays its own lost_lock check will
  have a token that is numerically less than the resource's last-seen token, and
  the resource silently rejects it — no corruption occurs.
"""

import threading
from typing import Optional


class ProtectedCounter:
    """Simulates a shared billing ledger as an incrementing counter per entity.

    Invariant: the value for an entity equals the number of accepted increments.
    No double-increment is possible: each accepted write advances the last-seen
    token, causing all in-flight writes from prior lock holders to be rejected.
    """

    def __init__(self):
        self._values: dict[str, int] = {}
        self._last_token: dict[str, int] = {}  # highest fence token accepted per entity
        self._log: list[dict] = []
        self._mu = threading.Lock()

    def increment(self, entity_key: str, fence_token: int, worker_id: str) -> bool:
        """Increment the counter for entity_key if fence_token is current.

        Returns True on acceptance, False on rejection.

        Rejection means this worker's lock had already expired and another
        worker has since written with a higher token. The write is a no-op.
        """
        with self._mu:
            last = self._last_token.get(entity_key, 0)
            if fence_token < last:
                self._log.append({
                    "worker": worker_id,
                    "entity": entity_key,
                    "token": fence_token,
                    "status": "REJECTED_STALE",
                    "last_seen_token": last,
                })
                return False

            # Accept write
            self._last_token[entity_key] = fence_token
            self._values[entity_key] = self._values.get(entity_key, 0) + 1
            self._log.append({
                "worker": worker_id,
                "entity": entity_key,
                "token": fence_token,
                "status": "ACCEPTED",
                "new_value": self._values[entity_key],
            })
            return True

    def get(self, entity_key: str) -> int:
        with self._mu:
            return self._values.get(entity_key, 0)

    def write_log(self) -> list[dict]:
        with self._mu:
            return list(self._log)

    def accepted_count(self, entity_key: str) -> int:
        with self._mu:
            return sum(
                1 for e in self._log
                if e["entity"] == entity_key and e["status"] == "ACCEPTED"
            )

    def rejected_count(self, entity_key: str) -> int:
        with self._mu:
            return sum(
                1 for e in self._log
                if e["entity"] == entity_key and e["status"] == "REJECTED_STALE"
            )
