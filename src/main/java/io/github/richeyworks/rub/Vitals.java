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

    /**
     * The change between an earlier reading and this one — a <b>pulse</b> (2026-08-19): rates
     * without a clock, in the house tradition. Everything is op-relative ({@code opsElapsed}
     * is the tail-sequence advance), so two pulses are comparable across machines and runs
     * where wall-clock rates would not be. {@code earlier} must genuinely be earlier (its
     * tail sequence at most this one's); the meters are monotonic, so negative deltas mean
     * the samples were swapped — refused loudly rather than reported as nonsense.
     */
    public Pulse since(Vitals earlier) {
        java.util.Objects.requireNonNull(earlier, "earlier");
        if (earlier.tailSequence > tailSequence) {
            throw new IllegalArgumentException("samples swapped: 'earlier' is at sequence "
                    + earlier.tailSequence + ", this sample at " + tailSequence);
        }
        return new Pulse(
                tailSequence - earlier.tailSequence,
                putsObserved - earlier.putsObserved,
                deletesObserved - earlier.deletesObserved,
                gapsObserved - earlier.gapsObserved,
                liveKeys - earlier.liveKeys,
                garbageBytes - earlier.garbageBytes);
    }

    /**
     * The change between two {@link Vitals} readings: committed ops elapsed, mutations metered,
     * gaps suffered, and how the live set and the garbage moved — the derivative the gauge and
     * meter together make possible. Key/byte deltas can be negative (deletes shrink the live
     * set; compaction reclaims garbage); the observation counters cannot.
     */
    public record Pulse(
            long opsElapsed,
            long putsObserved,
            long deletesObserved,
            long gapsObserved,
            long liveKeysDelta,
            long garbageBytesDelta) {

        /** Mutations metered across this pulse. */
        public long mutationsObserved() {
            return putsObserved + deletesObserved;
        }

        /** A one-line readout, {@link java.util.Locale#ROOT}-pinned like every house line. */
        public String line() {
            return String.format(java.util.Locale.ROOT,
                    "ops=%+d puts=%d dels=%d gaps=%d keys=%+d garbage=%+dB",
                    opsElapsed, putsObserved, deletesObserved, gapsObserved,
                    liveKeysDelta, garbageBytesDelta);
        }
    }

    /**
     * A one-line human readout, the shape every engine prints its vitals in. Formatted with
     * {@link java.util.Locale#ROOT} so the line is byte-identical on every machine — a vitals
     * line that changes shape with the default locale would break any consumer that greps it.
     */
    public String line() {
        return String.format(java.util.Locale.ROOT,
                "keys=%d seq=%d segs=%d live=%dB garbage=%dB (%.1f%%) puts=%d dels=%d (%.1f%% del) gaps=%d",
                liveKeys, tailSequence, segments, liveBytes, garbageBytes, garbageRatio() * 100,
                putsObserved, deletesObserved, deleteRatio() * 100, gapsObserved);
    }
}
