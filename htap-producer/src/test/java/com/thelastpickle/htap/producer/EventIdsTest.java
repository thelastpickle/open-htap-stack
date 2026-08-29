package com.thelastpickle.htap.producer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.thelastpickle.htap.common.TimeUuids;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The identifiers the fleet mints, which the events table is keyed on.
 *
 * <p>{@code demo.events} is keyed on {@code ((event_bucket, shard), event_id)} and the sink derives
 * {@code event_time} from the identifier, so two events sharing one are not a duplicate row but a
 * lost one. That makes distinctness the property this whole file exists for.
 */
class EventIdsTest {

    /** A fleet's worth: the largest the dashboard may ask for, and a batch at the demo's rate. */
    private static final int FLEET = 2000;

    private static final int THREADS = 8;

    /**
     * Sixteen thousand identifiers minted at once are all distinct.
     *
     * <p>Eight threads of a fleet each, every one of them stamped at the same instant, which is the
     * case the driver's own {@code Uuids.startOf} fails: it fixes the clock sequence and the node,
     * so every mint within one millisecond is identical. Here the two are drawn per call, which
     * leaves 62 bits of difference between two events of the same microsecond.
     */
    @Test
    @Timeout(30)
    void everyIdentifierIsDistinctAcrossThreads() throws InterruptedException {
        Instant sameInstant = Instant.parse("2026-08-27T15:55:33.000500Z");
        ConcurrentLinkedQueue<UUID> minted = new ConcurrentLinkedQueue<>();
        CountDownLatch ready = new CountDownLatch(THREADS);
        CountDownLatch go = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(THREADS);

        for (int thread = 0; thread < THREADS; thread++) {
            Thread.ofPlatform().start(() -> {
                ready.countDown();
                try {
                    go.await();
                    for (int i = 0; i < FLEET; i++) {
                        minted.add(TimeUuids.timeUuid(sameInstant));
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        assertTrue(ready.await(10, TimeUnit.SECONDS));
        go.countDown();
        assertTrue(done.await(20, TimeUnit.SECONDS));

        assertEquals(THREADS * FLEET, minted.size());
        assertEquals(
                THREADS * FLEET,
                new HashSet<>(minted).size(),
                "two of " + THREADS * FLEET + " identifiers were the same");
    }

    /**
     * Identifiers minted at rising instants read back in that order.
     *
     * <p>Which is what the sink relies on: it takes {@code event_time} from the identifier, and the
     * clustering order within a partition is the identifier's own. Distinctness alone would leave an
     * asset's history unordered.
     */
    @Test
    void identifiersFromRisingInstantsAreOrdered() {
        double at = 1787846133.0;
        List<UUID> ids = new ArrayList<>(FLEET);
        for (int i = 0; i < FLEET; i++) {
            // 500 microseconds apart, which is a 50 ms batch at 2,000 events a second.
            ids.add(TimeUuids.timeUuid(Fleet.instantOf(at + i * 0.0005)));
        }

        Set<Instant> stamps = new HashSet<>();
        for (int i = 0; i < ids.size(); i++) {
            Instant read = TimeUuids.instantOf(ids.get(i));
            stamps.add(read);
            if (i > 0) {
                assertTrue(
                        read.isAfter(TimeUuids.instantOf(ids.get(i - 1))),
                        "identifier " + i + " reads back before the one before it");
            }
        }
        assertEquals(FLEET, stamps.size(), "two events a batch apart shared a stamp");
        assertEquals(FLEET, new HashSet<>(ids).size());
    }

    /**
     * Two events of the same microsecond are still distinct, and their order is then undefined.
     *
     * <p>Recorded rather than asserted away: above about two million events a second the stamp step
     * falls below a microsecond, and what the demo needs there is that no row is lost. The sink
     * stores both, and which of the two a reader sees first is not a claim the demo makes.
     */
    @Test
    void twoEventsOfOneMicrosecondAreDistinctButUnordered() {
        Instant same = Instant.parse("2026-08-27T15:55:33.000500Z");

        UUID first = TimeUuids.timeUuid(same);
        UUID second = TimeUuids.timeUuid(same);

        assertTrue(!first.equals(second));
        assertEquals(TimeUuids.instantOf(first), TimeUuids.instantOf(second));
    }
}
