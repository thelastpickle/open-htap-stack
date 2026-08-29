package com.thelastpickle.htap.backend.vector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.thelastpickle.htap.backend.api.dto.LiveEmbeddingStatus;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.Test;

/**
 * The loop that keeps the index following the sink's writes.
 *
 * <p>Driven one pass at a time rather than through its thread: what is worth testing is which
 * snippets a pass chooses and what it records afterwards, and a test that started the loop would
 * be asserting against a sleep.
 */
class LiveEmbedderTest {

    private static final int NO_CAP = 1000;

    private final FakeEmbeddings table = new FakeEmbeddings();
    private final Indexer indexer = new Indexer(new LocalEmbedder(), table);
    private final AtomicLong nanos = new AtomicLong();

    @Test
    void aFirstPassEmbedsEveryAssetCarryingProse() {
        fleet("d0", "the drone climbed");
        fleet("d1", "over the airport perimeter");
        fleet("d2", "");
        LiveEmbedder live = embedder(NO_CAP);

        live.pass();

        assertEquals(Set.of("d0", "d1"), table.stored.keySet());
        assertEquals(2, live.status().embedded());
        assertEquals(2, live.status().lastEmbedded());
        assertEquals(2, live.status().tracked());
        assertEquals(1, live.status().passes());
    }

    @Test
    void aSecondPassEmbedsNothingWhenNoSnippetChanged() {
        fleet("d0", "the drone climbed");
        LiveEmbedder live = embedder(NO_CAP);
        live.pass();
        table.stored.clear();

        live.pass();

        assertTrue(table.stored.isEmpty());
        assertEquals(1, live.status().embedded());
        assertEquals(0, live.status().lastEmbedded());
        assertEquals(2, live.status().passes());
    }

    @Test
    void aSnippetTheSinkRewroteIsEmbeddedAgain() {
        fleet("d0", "the drone climbed");
        fleet("d1", "over the airport perimeter");
        LiveEmbedder live = embedder(NO_CAP);
        live.pass();
        table.stored.clear();
        fleet("d1", "then held at two hundred metres");

        live.pass();

        assertEquals(Set.of("d1"), table.stored.keySet());
        assertEquals(3, live.status().embedded());
    }

    /**
     * The point of priming: a restarted backend re-embedding the whole fleet would spend a network
     * round trip per asset to arrive at the vectors already in the table.
     */
    @Test
    void whatIsAlreadyIndexedIsNotEmbeddedAgainAfterARestart() {
        fleet("d0", "the drone climbed");
        fleet("d1", "over the airport perimeter");
        table.alreadyIndexed("d0");
        LiveEmbedder live = embedder(NO_CAP);

        live.pass();

        assertEquals(Set.of("d1"), table.stored.keySet());
        assertEquals(2, live.status().tracked());
    }

    @Test
    void theIndexIsReadOnceAndNotOnEveryPass() {
        fleet("d0", "the drone climbed");
        LiveEmbedder live = embedder(NO_CAP);

        live.pass();
        live.pass();

        assertEquals(1, table.indexedReads.get());
    }

    @Test
    void aPassDefersWhatItCannotTakeAndTheNextOneTakesIt() {
        for (int i = 0; i < 5; i++) {
            fleet("d" + i, "asset " + i + " over the airport perimeter");
        }
        LiveEmbedder live = embedder(2);

        live.pass();

        assertEquals(2, table.stored.size());
        assertEquals(3, live.status().pending());

        live.pass();

        assertEquals(4, table.stored.size());
        assertEquals(1, live.status().pending());

        live.pass();

        assertEquals(5, table.stored.size());
        assertEquals(0, live.status().pending());
    }

    /** Recorded only after the write, so a write that failed is work the next pass still has. */
    @Test
    void aFailedWriteIsTriedAgainOnTheNextPass() {
        fleet("d0", "the drone climbed");
        fleet("d1", "over the airport perimeter");
        table.refuse.add("d1");
        LiveEmbedder live = embedder(NO_CAP);

        live.pass();

        assertEquals(Set.of("d0"), table.stored.keySet());
        assertEquals(1, live.status().failed());
        assertEquals(1, live.status().tracked());

        table.refuse.clear();
        live.pass();

        assertEquals(Set.of("d0", "d1"), table.stored.keySet());
        assertEquals(1, live.status().failed());
        assertEquals(2, live.status().embedded());
    }

    @Test
    void thereIsNoAgeBeforeAPassAndAnAgeAfterOne() {
        fleet("d0", "the drone climbed");
        LiveEmbedder live = embedder(NO_CAP);

        assertNull(live.status().behindS());

        live.pass();
        nanos.addAndGet(2_500_000_000L);

        assertEquals(2.5, live.status().behindS());
    }

    /** The pass is timed from its own clock readings, so the figure is the pass and not the poll. */
    @Test
    void aPassReportsHowLongItTook() {
        fleet("d0", "the drone climbed");
        LiveEmbedder live = new LiveEmbedder(
                table, indexer, Vectors.live(false, 5.0, NO_CAP), advancingByOneMillisecond());

        live.pass();

        assertEquals(1.0, live.status().lastPassMs());
    }

    @Test
    void aTickDoesNothingWhileTheToggleIsOff() {
        fleet("d0", "the drone climbed");
        LiveEmbedder live = embedder(NO_CAP);

        live.tick();

        assertTrue(table.stored.isEmpty());
        assertEquals(0, live.status().passes());
    }

    @Test
    void aTickPassesOnceTheToggleIsOn() {
        fleet("d0", "the drone climbed");
        LiveEmbedder live = embedder(NO_CAP);

        live.enable(true);
        live.tick();

        assertEquals(Set.of("d0"), table.stored.keySet());
        assertTrue(live.status().enabled());
    }

    /**
     * Reported rather than fatal: Cassandra restarting under the loop is an expected state here,
     * and the loop has to resume on its own rather than needing the toggle cycled.
     */
    @Test
    void aPassThatFailsIsReportedAndTheNextOneClearsIt() {
        fleet("d0", "the drone climbed");
        table.fleetReadFails = new IllegalStateException("Cassandra not connected");
        LiveEmbedder live = embedder(NO_CAP);
        live.enable(true);

        live.tick();

        assertEquals("Cassandra not connected", live.status().error());
        assertEquals(0, live.status().passes());

        table.fleetReadFails = null;
        live.tick();

        assertNull(live.status().error());
        assertEquals(Set.of("d0"), table.stored.keySet());
    }

    @Test
    void turningTheLoopOffClearsTheErrorAndKeepsTheCounts() {
        fleet("d0", "the drone climbed");
        LiveEmbedder live = embedder(NO_CAP);
        live.enable(true);
        live.tick();
        table.fleetReadFails = new IllegalStateException("Cassandra not connected");
        live.tick();
        assertNotNull(live.status().error());

        LiveEmbeddingStatus off = live.enable(false);

        assertNull(off.error());
        assertEquals(1, off.embedded());
        assertEquals(1, off.tracked());
        assertFalse(off.enabled());
    }

    @Test
    void theStatusNamesTheEmbedderAndTheIntervalItRunsAt() {
        LiveEmbeddingStatus status = embedder(NO_CAP).status();

        assertEquals("local", status.embedder());
        assertEquals(5.0, status.intervalS());
        assertFalse(status.enabled());
    }

    private void fleet(String entityId, String text) {
        table.fleet.put(entityId, text);
    }

    private LiveEmbedder embedder(int maxPerCycle) {
        return new LiveEmbedder(table, indexer, Vectors.live(false, 5.0, maxPerCycle), nanos::get);
    }

    /**
     * A clock that moves one millisecond per reading.
     *
     * <p>A pass reads it at its start and again when it records how long it took, so one
     * millisecond per reading is what the pass should report.
     */
    private static LongSupplier advancingByOneMillisecond() {
        AtomicLong reading = new AtomicLong();
        return () -> reading.getAndAdd(1_000_000L);
    }
}
