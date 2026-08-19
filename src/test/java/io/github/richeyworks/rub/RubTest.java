package io.github.richeyworks.rub;

import io.github.richeyworks.smokehouse.SmokeHouse;
import io.github.richeyworks.smokehouse.SmokeHouseOptions;
import io.github.richeyworks.superbeefsort.external.SpillSerializer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Random;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The observer against the oracle: what Rub meters off the tail must equal what the tail
 * actually carried, its gauge must equal the store's own size, and its derived indicators must
 * track ground truth (garbage appears when records are overwritten, and disappears when the log
 * is compacted). Seeded; the tail is a real thread, so every counter read is fenced behind a
 * bounded {@link Rub#awaitObserved}.
 */
class RubTest {

    private static final long AWAIT = 10_000;

    private static SmokeHouseOptions<Long, String> opts() {
        return SmokeHouseOptions.of(SpillSerializer.forLongs(), SpillSerializer.forStrings())
                .segmentBytes(2048)                       // small: force many segments + garbage
                .indexTier(SmokeHouseOptions.IndexTier.STATIC);
    }

    private static long churn(SmokeHouse<Long, String> store, TreeMap<Long, String> oracle,
                              Random rnd, int ops) throws IOException {
        for (int i = 0; i < ops; i++) {
            long key = rnd.nextInt(120);
            if (rnd.nextInt(6) == 0) {
                store.delete(key);
                oracle.remove(key);
            } else {
                String v = "v" + key + ":" + i;
                store.put(key, v);
                oracle.put(key, v);
            }
        }
        return store.tailSequence();
    }

    @Test
    void metersEveryCommittedMutationAndItsGaugeEqualsSize(@TempDir Path dir) throws IOException {
        Random rnd = new Random(42);
        TreeMap<Long, String> oracle = new TreeMap<>();
        try (SmokeHouse<Long, String> store = SmokeHouse.open(dir, opts());
             Rub<Long, String> rub = Rub.over(store)) {
            long before = store.tailSequence();            // 0 on a fresh store
            long after = churn(store, oracle, rnd, 800);
            long committed = after - before;

            assertTrue(rub.awaitObserved(committed, AWAIT),
                    "the tail feed must catch up to every committed mutation");

            Vitals v = rub.sample();
            assertEquals(committed, v.mutationsObserved(),
                    "Rub meters exactly the mutations the tail carried");
            assertEquals(store.size(), v.liveKeys(), "the gauge equals the store's own size");
            assertEquals(oracle.size(), v.liveKeys(), "and the store equals the oracle");
            assertTrue(v.gapFree(), "a bounded churn must not overrun the tail ring");
            assertTrue(v.putsObserved() > 0 && v.deletesObserved() > 0,
                    "the churn exercised both puts and deletes");
        }
    }

    @Test
    void putsAndDeletesAreClassifiedExactly(@TempDir Path dir) throws IOException {
        try (SmokeHouse<Long, String> store = SmokeHouse.open(dir, opts());
             Rub<Long, String> rub = Rub.over(store)) {
            int puts = 40, deletes = 15;
            for (long k = 0; k < puts; k++) {              // every put is a new key → all commit
                store.put(k, "x" + k);
            }
            for (long k = 0; k < deletes; k++) {           // every delete hits a present key → commits
                store.delete(k);
            }
            assertTrue(rub.awaitObserved(puts + deletes, AWAIT));

            assertEquals(puts, rub.putsObserved(), "puts classified exactly");
            assertEquals(deletes, rub.deletesObserved(), "deletes classified exactly");
            assertEquals((long) puts + deletes, store.tailSequence(),
                    "and together they account for the whole tail");
        }
    }

    @Test
    void historyIsBoundedAndTickRetainsMostRecent(@TempDir Path dir) throws IOException {
        try (SmokeHouse<Long, String> store = SmokeHouse.open(dir, opts());
             Rub<Long, String> rub = Rub.over(store, 4)) {
            Vitals last = null;
            for (int i = 0; i < 10; i++) {
                store.put((long) i, "v" + i);
                last = rub.tick();
            }
            assertEquals(4, rub.history().size(), "history is bounded to its depth");
            assertEquals(last, rub.history().get(rub.history().size() - 1),
                    "the newest tick is retained at the back");
        }
    }

    @Test
    void garbageAppearsWithOverwritesAndVanishesOnCompaction(@TempDir Path dir) throws IOException {
        try (SmokeHouse<Long, String> store = SmokeHouse.open(dir, opts());
             Rub<Long, String> rub = Rub.over(store)) {
            for (int round = 0; round < 200; round++) {    // overwrite a small key set repeatedly
                store.put((long) (round % 20), "value-" + round);
            }
            assertTrue(rub.awaitObserved(200, AWAIT));

            Vitals dirty = rub.sample();
            assertEquals(20, dirty.liveKeys(), "only 20 keys are live after all the overwrites");
            assertTrue(dirty.garbageBytes() > 0, "overwrites leave garbage behind");
            assertTrue(dirty.garbageRatio() > 0.0 && dirty.garbageRatio() < 1.0,
                    "garbage ratio is a real fraction: " + dirty.garbageRatio());

            store.compact();
            Vitals clean = rub.sample();
            assertEquals(20, clean.liveKeys(), "compaction preserves the live set");
            assertTrue(clean.garbageBytes() < dirty.garbageBytes(),
                    "compaction reclaims garbage the gauge was reporting");
            assertFalse(rub.mutationsObserved() < 200, "the meter kept its count across compaction");
        }
    }
}
