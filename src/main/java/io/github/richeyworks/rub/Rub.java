package io.github.richeyworks.rub;

import io.github.richeyworks.smokehouse.SmokeHouse;
import io.github.richeyworks.smokehouse.TailEvent;
import io.github.richeyworks.smokehouse.TailListener;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Rub — engine thirteen of the ecosystem: the observability engine, the spice rub worked into
 * the surface of every other engine. WholeHog stood a bare watcher on the tail and counted
 * events to prove the four-subscriber convergence; Rub is that watcher <b>promoted to an
 * organ</b> — a tail-driven meter fused with a store gauge, producing {@link Vitals} that any
 * consumer (an exhibit, a dashboard, a test) reads without reaching inside a single engine.
 *
 * <p><b>Two clocks, one readout.</b> The tail feed is a <em>meter</em>: a background subscriber
 * counts every committed mutation as a put or a delete, and records a gap the instant the ring
 * outruns it (a gap means the counters undercount — Rub reports the hole, it does not paper over
 * it). {@link #sample()} adds the <em>gauge</em>: the store's current size, segment count, and
 * live-vs-garbage bytes, read off its public surface. {@link Vitals} carries both.</p>
 *
 * <p><b>What Rub deliberately is not:</b> a metrics backend, a clock, or a thread of its own. It
 * owns no scheduler — {@link #tick()} is caller-cadenced like every control loop in the ring, so
 * the sample history advances at the caller's rhythm, not a wall clock's. It observes the store
 * it is handed and never closes it; closing Rub only detaches the tail subscriber. Loopback-only,
 * deterministic up to the tail thread's bounded lag ({@link #awaitObserved} is the fence for
 * that lag). One store, one Rub.</p>
 *
 * @param <K> the store's key type
 * @param <V> the store's value type
 */
public final class Rub<K, V> implements Closeable {

    /** Default depth of the retained {@link #history()} ring — old samples fall off the back. */
    public static final int DEFAULT_HISTORY = 1_024;

    private final SmokeHouse<K, V> store;
    private final AutoCloseable subscription;
    private final int historyDepth;

    private final AtomicLong puts = new AtomicLong();
    private final AtomicLong deletes = new AtomicLong();
    private final AtomicLong gaps = new AtomicLong();

    private final Object historyLock = new Object();
    private final Deque<Vitals> history = new ArrayDeque<>();
    private volatile boolean closed;

    private Rub(SmokeHouse<K, V> store, int historyDepth) {
        this.store = store;
        this.historyDepth = historyDepth;
        // Subscribe to the whole tail from NOW: Rub meters every key's mutations flowing past
        // after it attaches (tailSequence() is the next sequence, so there is no history replay),
        // never claiming to have seen events it was not present for. The tail delivers off the
        // store lock, so a slow observer never stalls the writer — it just risks a gap, counted.
        this.subscription = store.tail(store.tailSequence(), new TailListener<K, V>() {
            @Override
            public void onEvent(TailEvent<K, V> event) {
                if (event.deleted()) {
                    deletes.incrementAndGet();
                } else {
                    puts.incrementAndGet();
                }
            }

            @Override
            public void onGap() {
                gaps.incrementAndGet();
            }
        });
    }

    /**
     * Attach a Rub to {@code store} with the default history depth. Metering begins now; the
     * store belongs to the caller (Rub never closes it).
     */
    public static <K, V> Rub<K, V> over(SmokeHouse<K, V> store) {
        return over(store, DEFAULT_HISTORY);
    }

    /**
     * Attach a Rub retaining the last {@code historyDepth} samples in {@link #history()}. A
     * depth of zero keeps no history — {@link #tick()} still returns each sample, it just is
     * not retained.
     */
    public static <K, V> Rub<K, V> over(SmokeHouse<K, V> store, int historyDepth) {
        Objects.requireNonNull(store, "store");
        if (historyDepth < 0) {
            throw new IllegalArgumentException("historyDepth must be >= 0: " + historyDepth);
        }
        return new Rub<>(store, historyDepth);
    }

    /**
     * Read the store's current gauge and fuse it with the accumulated meter into one
     * {@link Vitals}. Does not retain the sample — use {@link #tick()} for that. Reads the
     * store's public surface only.
     */
    public Vitals sample() throws IOException {
        long liveSegBytes = 0;
        int segments = 0;
        for (SmokeHouse.SegmentStat stat : store.segmentStats()) {
            segments++;
            liveSegBytes += stat.bytes();
        }
        long garbage = store.garbageBytes();
        long live = Math.max(0, liveSegBytes - garbage);
        // Read the counters after the store gauge: a mutation that lands between the two reads
        // shows up as an extra observed event, never as a missing one — the meter never lies low.
        return new Vitals(
                store.tailSequence(),
                store.size(),
                segments,
                live,
                garbage,
                puts.get(),
                deletes.get(),
                gaps.get());
    }

    /**
     * Take a {@link #sample()} and retain it in the bounded {@link #history()} ring. The
     * observability equivalent of the other engines' {@code tick()} — caller-cadenced; call it
     * from your own loop at your own rhythm.
     */
    public Vitals tick() throws IOException {
        Vitals sample = sample();
        if (historyDepth > 0) {
            synchronized (historyLock) {
                history.addLast(sample);
                while (history.size() > historyDepth) {
                    history.removeFirst();
                }
            }
        }
        return sample;
    }

    /** The retained samples, oldest first — a copy, safe to iterate while metering continues. */
    public List<Vitals> history() {
        synchronized (historyLock) {
            return List.copyOf(history);
        }
    }

    /** Puts seen on the tail since attach. */
    public long putsObserved() {
        return puts.get();
    }

    /** Deletes (tombstones) seen on the tail since attach. */
    public long deletesObserved() {
        return deletes.get();
    }

    /** Every committed mutation seen on the tail since attach — puts and deletes. */
    public long mutationsObserved() {
        return puts.get() + deletes.get();
    }

    /** Tail gaps — times the observer fell behind and older events were dropped. */
    public long gapsObserved() {
        return gaps.get();
    }

    /**
     * Block until Rub has metered at least {@code minMutations} committed mutations, or
     * {@code timeoutMillis} elapses. The tail is asynchronous, so a consumer that just drove a
     * known number of writes fences the observer's lag here before reading its counters. Returns
     * true if the target was reached in time.
     */
    public boolean awaitObserved(long minMutations, long timeoutMillis) {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (mutationsObserved() < minMutations) {
            if (System.currentTimeMillis() >= deadline) {
                return false;
            }
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return true;
    }

    /**
     * Detach the tail subscriber. The observed store stays open — it is the caller's.
     * Idempotent: a second close is a no-op, so Rub is safe in try-with-resources stacks
     * that tear down more than once.
     */
    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        try {
            subscription.close();
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("detaching Rub's tail subscriber", e);
        }
    }
}
