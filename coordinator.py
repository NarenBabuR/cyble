"""
coordinator.py — Lock/coordination component for worker fleets.

Assumption: single-node Redis-like coordination service with atomic SET NX PX
semantics. This module provides an in-memory stand-in (LockManager) and the
worker-facing client (LockClient).

Two-layer defence against the stale-lock-holder problem:
  1. Fencing tokens — every acquisition returns a strictly increasing integer.
     The protected resource validates this token and rejects writes from holders
     whose token has been superseded. This is the backstop that catches races
     heartbeat cannot prevent.
  2. Heartbeat renewal — a daemon thread renews the TTL every ttl/3 seconds
     while the worker is alive. If renewal fails (lock expired and was re-issued),
     the client sets lost_lock=True. The worker checks this flag before writing
     and aborts instead of corrupting the resource.
"""

import itertools
import threading
import time
from dataclasses import dataclass, field
from typing import Optional


# ---------------------------------------------------------------------------
# LockManager — simulates the coordination service (runs "inside Redis")
# ---------------------------------------------------------------------------

_global_fence_counter = itertools.count(1)


@dataclass
class _LockEntry:
    owner: str
    fence_token: int
    expires_at: float  # time.monotonic()


class LockManager:
    """Thread-safe in-memory coordination service.

    Models a single Redis node with SET NX PX / DEL semantics. All state
    mutations happen under a single mutex, matching Redis's single-threaded
    command execution model.
    """

    def __init__(self):
        self._locks: dict[str, _LockEntry] = {}
        self._mu = threading.Lock()
        # Renewal log for observability in tests
        self.renewal_log: list[dict] = []

    def acquire(self, key: str, owner: str, ttl_seconds: float) -> Optional[int]:
        """Atomic SET key owner NX PX ttl_ms.

        Returns the fence token on success, None if the lock is already held.
        """
        now = time.monotonic()
        with self._mu:
            entry = self._locks.get(key)
            if entry is not None and entry.expires_at > now:
                return None  # lock is live; reject
            # Expired or absent — issue new lock with next fence token
            token = next(_global_fence_counter)
            self._locks[key] = _LockEntry(
                owner=owner,
                fence_token=token,
                expires_at=now + ttl_seconds,
            )
            return token

    def renew(self, key: str, owner: str, fence_token: int, ttl_seconds: float) -> bool:
        """Extend TTL only if (key, fence_token) still matches what we issued.

        Returns True on success. False means the lock expired and was re-issued
        to another worker — the caller has lost the lock and must not write.
        """
        now = time.monotonic()
        with self._mu:
            entry = self._locks.get(key)
            if entry is None or entry.fence_token != fence_token:
                self.renewal_log.append({"result": "LOST", "key": key, "token": fence_token, "owner": owner})
                return False
            entry.expires_at = now + ttl_seconds
            self.renewal_log.append({"result": "OK", "key": key, "token": fence_token, "owner": owner})
            return True

    def release(self, key: str, owner: str, fence_token: int) -> bool:
        """DEL the key only if we still own it (same fence token).

        Returns True if released, False if we had already lost the lock (e.g.
        the TTL expired and another worker acquired it before we released).
        Critically: never releases another worker's lock.
        """
        with self._mu:
            entry = self._locks.get(key)
            if entry is None or entry.fence_token != fence_token:
                return False  # already gone or taken over — safe no-op
            del self._locks[key]
            return True

    def current_token(self, key: str) -> Optional[int]:
        """Return the fence token currently in force for key (test introspection)."""
        with self._mu:
            entry = self._locks.get(key)
            return entry.fence_token if entry else None


# ---------------------------------------------------------------------------
# LockClient — worker-facing API
# ---------------------------------------------------------------------------

class LockAcquireTimeout(Exception):
    """Raised when a worker cannot acquire the lock within the deadline."""


class LockClient:
    """Context manager that acquires, heartbeats, and releases a distributed lock.

    Usage:
        client = LockClient(manager, "order-123", "worker-7", ttl_seconds=5.0)
        with client as fence_token:
            if client.lost_lock:
                return  # abort — heartbeat confirmed we lost the lock
            resource.write(entity_key, fence_token, worker_id)

    The fence_token yielded by __enter__ must be passed to every resource write.
    The resource validates the token independently; this gives a second layer of
    protection even if the worker fails to check lost_lock in time.
    """

    def __init__(
        self,
        manager: LockManager,
        entity_key: str,
        worker_id: str,
        ttl_seconds: float = 5.0,
        acquire_timeout: float = 30.0,
        retry_interval: float = 0.05,
    ):
        self._mgr = manager
        self._key = entity_key
        self._worker_id = worker_id
        self._ttl = ttl_seconds
        self._acquire_timeout = acquire_timeout
        self._retry_interval = retry_interval

        self._fence_token: Optional[int] = None
        self._lost_lock = False
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._stop_heartbeat = threading.Event()

    @property
    def lost_lock(self) -> bool:
        """True if the heartbeat confirmed the lock was lost mid-job."""
        return self._lost_lock

    @property
    def fence_token(self) -> Optional[int]:
        return self._fence_token

    def __enter__(self) -> int:
        deadline = time.monotonic() + self._acquire_timeout
        while True:
            token = self._mgr.acquire(self._key, self._worker_id, self._ttl)
            if token is not None:
                self._fence_token = token
                self._lost_lock = False
                self._stop_heartbeat.clear()
                self._heartbeat_thread = threading.Thread(
                    target=self._heartbeat_loop, daemon=True
                )
                self._heartbeat_thread.start()
                return token
            if time.monotonic() >= deadline:
                raise LockAcquireTimeout(
                    f"{self._worker_id} timed out acquiring lock on {self._key!r}"
                )
            time.sleep(self._retry_interval)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._stop_heartbeat.set()
        if self._heartbeat_thread:
            self._heartbeat_thread.join(timeout=self._ttl / 3 + 1)
        if self._fence_token is not None:
            self._mgr.release(self._key, self._worker_id, self._fence_token)
        self._fence_token = None

    def _heartbeat_loop(self):
        interval = self._ttl / 3
        while not self._stop_heartbeat.wait(timeout=interval):
            if self._fence_token is None:
                break
            ok = self._mgr.renew(self._key, self._worker_id, self._fence_token, self._ttl)
            if not ok:
                self._lost_lock = True
                break  # stop heartbeating — nothing to renew
