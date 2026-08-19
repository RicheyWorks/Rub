package io.github.richeyworks.rub;

/**
 * One point-in-time readout of a store, taken by {@link Rub}: the instantaneous shape of the
 * log (live keys, segments, live vs. garbage bytes, tail position) fused with the tail-driven
 * counters {@link Rub} has accumulated since it attached (puts, deletes, gaps observed).
 *
 * <p>Instantaneous fields answer "what does the store look like right now"; the {@code *Observed}
 * counters answer "what has flowed past since Rub started watching" — the two together are the
 * difference between a gauge and a meter, and an observability organ needs both. Derived
 * indicators ({@link #garbageRatio}, {@link #deleteRatio}) are computed, never stored, so they
 * cannot drift from their inputs.</p>
 */
public record Vitals(
        long tailSequence,
        int liveKeys,
        int segments,
        long liveBytes,
        long garbageBytes,
        long putsObserved,
        long deletesObserved,
        long gapsObserved) {

    /** Live + garbage: the on-disk weight of the log's records. */
    public long totalBytes() {
        return liveBytes + garbageBytes;
    }

    /** Fraction of on-disk record bytes that are garbage (0 when the log is empty). */
    public double garbageRatio() {
        long total = totalBytes();
        return total == 0 ? 0.0 : (double) garbageBytes / total;
    }

    /** Every mutation Rub has seen on the tail since it attached — puts and deletes. */
    public long mutationsObserved() {
        return putsObserved + deletesObserved;
    }

    /** Fraction of observed mutations that were deletes (0 when none seen). */
    public double deleteRatio() {
        long mutations = mutationsObserved();
        return mutations == 0 ? 0.0 : (double) deletesObserved / mutations;
    }

    /**
     * True when Rub's tail feed has stayed gap-free — every mutation since attach accounted
     * for. A gap means the observer fell behind the ring and its counters undercount; an
     * honest observability organ reports the hole rather than hiding it.
     */
    public boolean gapFree() {
        return gapsObserved == 0;
    }

    /** A one-line human readout, the shape every engine prints its vitals in. */
    public String line() {
        return String.format(
                "keys=%d seq=%d segs=%d live=%dB garbage=%dB (%.1f%%) puts=%d dels=%d (%.1f%% del) gaps=%d",
                liveKeys, tailSequence, segments, liveBytes, garbageBytes, garbageRatio() * 100,
                putsObserved, deletesObserved, deleteRatio() * 100, gapsObserved);
    }
}
