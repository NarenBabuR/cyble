"""
simulation.py — Test harness for the coordinator.

Four scenarios, each demonstrating a different aspect of the operating conditions
described in the assignment brief. Run with: python3 simulation.py

Scenarios:
  1. Normal concurrency  — 20 workers, one increment each, no stalls.
  2. Stalled worker      — worker pauses past its TTL; fencing token blocks its write.
  3. Heartbeat survival  — long job (2.5s) keeps lock alive via heartbeat renewal.
  4. Dead worker         — heartbeat stops; TTL expires; second worker takes over.
"""

import threading
import time
from coordinator import LockManager, LockClient
from resource import ProtectedCounter


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _header(title: str):
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")

def _pass(msg: str):
    print(f"  [PASS] {msg}")

def _fail(msg: str):
    print(f"  [FAIL] {msg}")
    raise AssertionError(msg)

def _info(msg: str):
    print(f"         {msg}")


# ─────────────────────────────────────────────────────────────────────────────
# Scenario 1: Normal concurrency — 20 workers, no stalls
# ─────────────────────────────────────────────────────────────────────────────

def scenario_normal_concurrency():
    _header("Scenario 1: Normal concurrency (20 workers, no stalls)")

    ENTITY = "order-123"
    N = 20
    mgr = LockManager()
    counter = ProtectedCounter()

    # Track simultaneous lock holders — must never exceed 1
    active_holders: set[str] = set()
    holder_lock = threading.Lock()
    max_concurrent = [0]

    def worker(wid: str):
        client = LockClient(mgr, ENTITY, wid, ttl_seconds=2.0, acquire_timeout=60.0)
        with client as token:
            with holder_lock:
                active_holders.add(wid)
                if len(active_holders) > max_concurrent[0]:
                    max_concurrent[0] = len(active_holders)
            time.sleep(0.01)  # simulate brief work
            if not client.lost_lock:
                counter.increment(ENTITY, token, wid)
            with holder_lock:
                active_holders.discard(wid)

    threads = [threading.Thread(target=worker, args=(f"w{i}",)) for i in range(N)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    accepted = counter.accepted_count(ENTITY)
    value = counter.get(ENTITY)

    _info(f"Workers: {N}, Accepted writes: {accepted}, Counter value: {value}")
    _info(f"Max simultaneous lock holders observed: {max_concurrent[0]}")

    if max_concurrent[0] > 1:
        _fail(f"Mutual exclusion violated — {max_concurrent[0]} workers held the lock at once")
    if value != accepted:
        _fail(f"Counter value ({value}) != accepted writes ({accepted}) — double-write detected")
    if accepted != N:
        _fail(f"Expected {N} accepted writes, got {accepted}")

    _pass("All 20 writes accepted, counter correct, mutual exclusion maintained")


# ─────────────────────────────────────────────────────────────────────────────
# Scenario 2: Stalled worker — fencing token rejects the late write
# ─────────────────────────────────────────────────────────────────────────────

def scenario_stalled_worker():
    _header("Scenario 2: Stalled worker (TTL expires mid-pause; fencing rejects write)")
    _info("Models a GC/CPU-steal pause: the entire process freezes, so the")
    _info("heartbeat also stops. We bypass LockClient to replicate this directly.")

    # In a real GC pause, ALL threads freeze simultaneously — including the
    # heartbeat thread. Python's threading.sleep() does NOT model this: only the
    # sleeping thread pauses while daemons run freely. We therefore simulate the
    # pause at the LockManager level, acquiring the lock without a heartbeat and
    # then sleeping past the TTL. This is exactly what the original naive design
    # (acquire → work → release, no heartbeat) experiences during a long pause.

    ENTITY = "billing-456"
    TTL = 0.3
    mgr = LockManager()
    counter = ProtectedCounter()

    stall_done = threading.Event()

    def stalling_worker():
        # Acquire directly — no heartbeat, replicating the naive original design
        stall_token = mgr.acquire(ENTITY, "staller", TTL)
        assert stall_token is not None
        _info(f"Staller acquired lock (token={stall_token}), now stalling for {TTL * 4}s …")
        time.sleep(TTL * 4)  # TTL expires; lock is now available to others
        _info(f"Staller woke up, attempting write with stale token={stall_token} …")
        result = counter.increment(ENTITY, stall_token, "staller")
        _info(f"Staller write result: {'ACCEPTED' if result else 'REJECTED_STALE'}")
        # No release — worker doesn't know it lost the lock (it thinks it still has it)
        stall_done.set()

    def legitimate_worker():
        time.sleep(TTL * 2)  # wait for staller's TTL to expire
        client = LockClient(mgr, ENTITY, "legit", ttl_seconds=2.0, acquire_timeout=10.0)
        with client as token:
            _info(f"Legit worker acquired lock (token={token})")
            time.sleep(0.02)
            result = counter.increment(ENTITY, token, "legit")
            _info(f"Legit worker write result: {'ACCEPTED' if result else 'REJECTED'}")

    t_stall = threading.Thread(target=stalling_worker)
    t_legit = threading.Thread(target=legitimate_worker)
    t_stall.start()
    t_legit.start()
    t_stall.join(timeout=10)
    t_legit.join(timeout=10)

    log = counter.write_log()
    staler_entries = [e for e in log if e["worker"] == "staller"]
    legit_entries  = [e for e in log if e["worker"] == "legit"]

    _info(f"Write log: {log}")

    if not any(e["status"] == "REJECTED_STALE" for e in staler_entries):
        _fail("Staller's write was NOT rejected — fencing token failed to protect resource")

    if not any(e["status"] == "ACCEPTED" for e in legit_entries):
        _fail("Legitimate worker's write was NOT accepted")

    if counter.get(ENTITY) != 1:
        _fail(f"Counter should be 1 (only legit write), got {counter.get(ENTITY)}")

    _pass("Stalled worker's write rejected; legitimate write accepted; counter=1")


# ─────────────────────────────────────────────────────────────────────────────
# Scenario 3: Heartbeat keeps a long job alive
# ─────────────────────────────────────────────────────────────────────────────

def scenario_heartbeat_survival():
    _header("Scenario 3: Heartbeat survival (long job, TTL=0.5s, job duration=1.5s)")

    ENTITY = "inventory-789"
    TTL = 0.5
    JOB_DURATION = 1.5  # 3× TTL — would expire 3 times without heartbeat
    mgr = LockManager()
    counter = ProtectedCounter()

    client = LockClient(mgr, ENTITY, "long-runner", ttl_seconds=TTL, acquire_timeout=10.0)
    with client as token:
        _info(f"Long-runner acquired lock (token={token}), working for {JOB_DURATION}s …")
        time.sleep(JOB_DURATION)
        lost = client.lost_lock
        _info(f"lost_lock after {JOB_DURATION}s: {lost}")
        result = counter.increment(ENTITY, token, "long-runner")
        _info(f"Write result: {'ACCEPTED' if result else 'REJECTED'}")

    renewals = [r for r in mgr.renewal_log if r["key"] == ENTITY]
    ok_renewals = [r for r in renewals if r["result"] == "OK"]
    _info(f"Successful heartbeat renewals: {len(ok_renewals)}")

    if client.lost_lock:
        _fail("Heartbeat failed to keep lock alive — lost_lock=True after a normal job")
    if not counter.get(ENTITY) == 1:
        _fail("Write was rejected despite heartbeat keeping lock alive")
    if len(ok_renewals) < 2:
        _fail(f"Expected ≥2 successful renewals, got {len(ok_renewals)}")

    _pass(f"Lock survived {JOB_DURATION}s job via {len(ok_renewals)} heartbeat renewals")


# ─────────────────────────────────────────────────────────────────────────────
# Scenario 4: Dead worker — heartbeat stops, TTL expires, second worker takes over
# ─────────────────────────────────────────────────────────────────────────────

def scenario_dead_worker():
    _header("Scenario 4: Dead worker (heartbeat killed; second worker reclaims lock)")

    ENTITY = "shipment-321"
    TTL = 0.5
    mgr = LockManager()
    counter = ProtectedCounter()

    dead_token: list[int] = []
    second_acquired = threading.Event()

    def dead_worker():
        client = LockClient(mgr, ENTITY, "dead", ttl_seconds=TTL, acquire_timeout=10.0)
        with client as token:
            dead_token.append(token)
            _info(f"Dead worker acquired lock (token={token})")
            # Simulate death by killing the heartbeat early, then blocking forever
            client._stop_heartbeat.set()  # stops heartbeat thread
            _info("Dead worker's heartbeat stopped (simulating process death)")
            second_acquired.wait(timeout=10)  # block without renewing — TTL will expire

    def rescuer():
        time.sleep(TTL * 2.5)  # wait for dead worker's TTL to expire
        client = LockClient(mgr, ENTITY, "rescuer", ttl_seconds=2.0, acquire_timeout=10.0)
        with client as token:
            _info(f"Rescuer acquired lock (token={token}) after dead worker's TTL expired")
            result = counter.increment(ENTITY, token, "rescuer")
            _info(f"Rescuer write: {'ACCEPTED' if result else 'REJECTED'}")
        second_acquired.set()

    t_dead   = threading.Thread(target=dead_worker)
    t_rescue = threading.Thread(target=rescuer)
    t_dead.start()
    t_rescue.start()
    t_dead.join(timeout=10)
    t_rescue.join(timeout=10)

    if not dead_token:
        _fail("Dead worker never acquired the lock")

    rescuer_entries = [e for e in counter.write_log() if e["worker"] == "rescuer"]
    if not any(e["status"] == "ACCEPTED" for e in rescuer_entries):
        _fail("Rescuer's write was not accepted after dead worker's TTL expired")

    if counter.get(ENTITY) != 1:
        _fail(f"Counter should be 1 (only rescuer wrote), got {counter.get(ENTITY)}")

    dead_writes = [e for e in counter.write_log() if e["worker"] == "dead"]
    if any(e["status"] == "ACCEPTED" for e in dead_writes):
        _fail("Dead worker somehow wrote — should not have happened")

    _pass("Dead worker's lock expired; rescuer reclaimed it and wrote successfully")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("  The Coordinator — Simulation / Test Harness")
    print("=" * 60)

    try:
        scenario_normal_concurrency()
        scenario_stalled_worker()
        scenario_heartbeat_survival()
        scenario_dead_worker()
        print(f"\n{'=' * 60}")
        print("  ALL SCENARIOS PASSED")
        print(f"{'=' * 60}\n")
    except AssertionError as e:
        print(f"\n{'=' * 60}")
        print(f"  SCENARIO FAILED: {e}")
        print(f"{'=' * 60}\n")
        raise SystemExit(1)
