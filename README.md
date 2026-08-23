# Rub

[![CI](https://github.com/RicheyWorks/Rub/actions/workflows/ci.yml/badge.svg)](https://github.com/RicheyWorks/Rub/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Java 17](https://img.shields.io/badge/Java-17-orange.svg)](https://adoptium.net/)

> **New here — or not a coder?** Start with the [plain-English guide to the whole ecosystem →](https://github.com/RicheyWorks/WholeHog/blob/main/ECOSYSTEM.md): what all of this is, what you'd actually use it for, and how to get it running even if you've never written a line of code.


Engine thirteen of the ecosystem: **the observability engine** — the rub worked into the
surface of every other engine. WholeHog stood a bare watcher on the store's tail and counted
events to prove its four subscribers converge; Rub is that watcher **promoted to an organ**.

```java
try (SmokeHouse<Long, String> store = SmokeHouse.open(dir, opts);
     Rub<Long, String> rub = Rub.over(store)) {
    // ... the store churns ...
    Vitals v = rub.sample();
    System.out.println(v.line());
    // keys=1042 seq=8801 segs=6 live=41kB garbage=12kB (22.6%) puts=7203 dels=1598 (18.2% del) gaps=0
}
```

## Two clocks, one readout

An observability organ needs both a **gauge** and a **meter**, and most tools give you one.

- **The meter** rides the tail. A background subscriber counts every committed mutation as a
  put or a delete the moment it lands, and records a **gap** the instant the ring outruns it.
  A gap means the counters undercount — Rub reports the hole in `Vitals`, it does not paper
  over it.
- **The gauge** is `sample()`: the store's current size, segment count, and live-vs-garbage
  bytes, read straight off its public surface.

`Vitals` fuses the two. Derived indicators — garbage ratio, delete ratio — are computed on
read, never stored, so they cannot drift from their inputs.

## What Rub deliberately is not

A metrics backend, a clock, or a thread of its own. It owns no scheduler: `tick()` is
caller-cadenced like every control loop in the ring, so the sample history advances at your
rhythm, not a wall clock's. It observes the store it is handed and **never closes it**; closing
Rub only detaches the tail subscriber. Loopback-only, deterministic up to the tail thread's
bounded lag — and `awaitObserved(n, ms)` is the fence for that lag, so a consumer that just
drove *n* writes can wait for the meter to catch up before reading it.

## The house rule it keeps

Rub touches only the store's **public** surface — `tail`, `size`, `segmentStats`,
`garbageBytes`, `tailSequence`. The engine that watches everything is the last one that should
be allowed to reach inside anything; it earns its readings the same way every consumer does.

## The ecosystem

Engines 1–6: [CSRBT](https://github.com/RicheyWorks/CSRBT) (index) · [SuperBeefSort](https://github.com/RicheyWorks/SuperBeefSort) (intake) · [SmokeHouse](https://github.com/RicheyWorks/SmokeHouse) (store) · [Carver](https://github.com/RicheyWorks/Carver) (read planner) · [Renderer](https://github.com/RicheyWorks/Renderer) (materialized views) · [Brine](https://github.com/RicheyWorks/Brine) (adaptive cache).
Engines 7–11: [PitBoss](https://github.com/RicheyWorks/PitBoss) (fleet conductor) · [DryAge](https://github.com/RicheyWorks/DryAge) (time travel) · [Twine](https://github.com/RicheyWorks/Twine) (atomic batches) · [SmokeSignal](https://github.com/RicheyWorks/SmokeSignal) (the wire) · [Jerky](https://github.com/RicheyWorks/Jerky) (cold archives).
Engine 12: [WholeHog](https://github.com/RicheyWorks/WholeHog) (the integration organism).
Engines 13–14: **Rub** (this repo, observability) · [Sizzle](https://github.com/RicheyWorks/Sizzle) (chaos).

## Build

**Never set up a project like this before?** You don't need to know Java or Gradle. Open [Claude](https://claude.ai) or ChatGPT and paste:

> *“Walk me through installing Java 17 and running `RicheyWorks/Rub` from GitHub, one step at a time. I'm on Windows (or Mac) and I've never done this — keep it simple.”*

It will take you the rest of the way. The full newcomer guide lives in [ECOSYSTEM.md](https://github.com/RicheyWorks/WholeHog/blob/main/ECOSYSTEM.md).


```bash
# Requires SmokeHouse (and its siblings) cloned alongside — composite build.
./gradlew build     # the observer against the oracle
```

Java 17+, Gradle 9.5.1 (bundled wrapper). MIT license.
