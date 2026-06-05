# The Coordinator

A distributed locking system that prevents multiple workers from corrupting shared data when they try to write at the same time.

## Requirements

- Python 3.10 or higher
- No external libraries needed — uses Python standard library only

## How to run

```bash
python3 simulation.py
```

That's the only command you need. It runs all 4 test scenarios and prints PASS or FAIL for each.

## Expected output

```
============================================================
  The Coordinator — Simulation / Test Harness
============================================================

────────────────────────────────────────────────────────────
  Scenario 1: Normal concurrency (20 workers, no stalls)
────────────────────────────────────────────────────────────
  [PASS] All 20 writes accepted, counter correct, mutual exclusion maintained

────────────────────────────────────────────────────────────
  Scenario 2: Stalled worker (TTL expires mid-pause; fencing rejects write)
────────────────────────────────────────────────────────────
  [PASS] Stalled worker's write rejected; legitimate write accepted; counter=1

────────────────────────────────────────────────────────────
  Scenario 3: Heartbeat survival (long job, TTL=0.5s, job duration=1.5s)
────────────────────────────────────────────────────────────
  [PASS] Lock survived 1.5s job via 8 heartbeat renewals

────────────────────────────────────────────────────────────
  Scenario 4: Dead worker (heartbeat killed; second worker reclaims lock)
────────────────────────────────────────────────────────────
  [PASS] Dead worker's lock expired; rescuer reclaimed it and wrote successfully

============================================================
  ALL SCENARIOS PASSED
============================================================
```

## Files

| File | What it is | Should you run it? |
|---|---|---|
| `simulation.py` | Test harness — runs all 4 scenarios | **Yes, run this one** |
| `coordinator.py` | The lock system (LockManager + LockClient) | No — imported by simulation.py |
| `resource.py` | The protected shared resource (ProtectedCounter) | No — imported by simulation.py |
| `DESIGN_NOTE.md` | Design decisions, guarantees, and known limitations | Read this |
| `EXPLAINER.md` | Line-by-line explanation of every file, with diagrams | Read this if new to the topic |

## What the 4 scenarios test

| Scenario | What it proves |
|---|---|
| 1. Normal concurrency | 20 workers contend for the same lock — only 1 holds it at a time |
| 2. Stalled worker | A frozen worker's write is rejected by the fencing token after it loses its lock |
| 3. Heartbeat survival | A long job (3× the TTL) completes because the heartbeat keeps renewing the lock |
| 4. Dead worker | After a worker's heartbeat stops, the lock expires and a second worker takes over |

## How it works in one paragraph

Every worker that wants to touch a shared resource must first acquire a lock from the `LockManager`. The lock comes with a ticket number (fence token) and a countdown timer (TTL). While the worker does its job, a background thread (heartbeat) resets the timer every few seconds to prove the worker is still alive. If the worker dies, the heartbeat stops and the timer eventually hits zero — freeing the lock for the next worker. When the worker writes to the resource, it passes its ticket number. The resource rejects any write whose ticket number is lower than the last one it accepted — so even if a frozen worker wakes up late and tries to write, its old ticket is rejected and no corruption occurs.
