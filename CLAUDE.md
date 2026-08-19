# Rub — working notes for agents

Engine 13: the observability engine. `Rub.over(store)` attaches a tail subscriber (the meter)
and `sample()`/`tick()` fuse it with the store gauge into `Vitals`. `RubTest` is the oracle:
what Rub meters equals what the tail carried; its gauge equals the store's own size.

## Build & test
- Composite build including `../SmokeHouse` (transitively SuperBeefSort + CSRBT).
- The tail is a real thread. Every counter read in a test is fenced behind
  `awaitObserved(n, ms)` — keep it that way; never assert a counter without first awaiting it.

## Git is host-side
Same as the siblings: agent sandboxes cannot write `.git`. Run all git commands from a host
terminal (PowerShell). Stale `.git/index.lock` fix: `Remove-Item .git\index.lock -Force`.

## Invariants (do not break)
- **Rub is an observer, never a shortcut.** It touches only the store's public surface
  (`tail`, `size`, `segmentStats`, `garbageBytes`, `tailSequence`). If observability needs a
  new reading, name the seam upstream on SmokeHouse — do not reach inside.
- **It never closes the store it watches.** `close()` detaches the tail subscriber only.
- **The meter never lies low.** Read the store gauge before the counters in `sample()`, so a
  mutation racing the sample shows up as an extra observed event, never a missing one.
- **Gaps are reported, not hidden.** A tail gap means the counters undercount; `Vitals.gapFree()`
  and `gapsObserved()` surface it. Do not swallow `onGap()`.
- Caller-cadenced: no clock, no scheduler. `tick()` advances history at the caller's rhythm.
