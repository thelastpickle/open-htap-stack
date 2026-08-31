package com.thelastpickle.htap.producer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.junit.jupiter.api.Test;

/** How many events a turn sends, and that a second of turns adds up to the rate asked for. */
class BatchPacerTest {

    /** The loop's own cadence, so the figures here are the ones the container runs on. */
    private static final Duration PERIOD = Duration.ofMillis(50);

    /** A rate that divides the period gives the same whole batch every turn. */
    @Test
    void aRateThatDividesGivesAWholeBatch() {
        BatchPacer pacer = new BatchPacer(PERIOD);

        assertEquals(100, pacer.next(2000));
        assertEquals(100, pacer.next(2000));
        assertEquals(20, pacer.next(400));
    }

    /**
     * The demo's default rate, which is a quarter of an event a turn.
     *
     * <p>This is the case the flooring got wrong: one event on every turn is 20 a second, four
     * times what was asked for. Three empty turns and then one event is the shape of the fix, and
     * the assertion is on the shape rather than on the total alone, because a pacer that sent four
     * events on the fourth turn would also total four.
     */
    @Test
    void aRateBelowOneEventATurnSpendsTurnsWaiting() {
        BatchPacer pacer = new BatchPacer(PERIOD);

        assertEquals(0, pacer.next(5));
        assertEquals(0, pacer.next(5));
        assertEquals(0, pacer.next(5));
        assertEquals(1, pacer.next(5));
        assertEquals(0, pacer.next(5));
    }

    /** A second of turns sends the rate, whether or not the rate divides the period. */
    @Test
    void aSecondOfTurnsSendsTheRate() {
        for (int rate : new int[] {1, 5, 7, 50, 333, 2000, 5000, 999_983}) {
            BatchPacer pacer = new BatchPacer(PERIOD);
            int sent = 0;
            for (int turn = 0; turn < 20; turn++) {
                sent += pacer.next(rate);
            }
            assertEquals(rate, sent, "a second at " + rate + " a second");
        }
    }

    /**
     * A rate change takes effect at once, and carries at most one event of the rate before it.
     *
     * <p>The Settings page changes the rate, and it may change it by three orders of magnitude; a
     * pacer that banked the old rate's remainder would send a burst at the new one. What is carried
     * is below one event, so a second at the new rate is that rate to within a single event.
     */
    @Test
    void aRateChangeCarriesAtMostOneEventOfTheOldRate() {
        BatchPacer pacer = new BatchPacer(PERIOD);
        pacer.next(5000);
        pacer.next(4999);

        int sent = 0;
        for (int turn = 0; turn < 20; turn++) {
            sent += pacer.next(5);
        }

        assertTrue(sent == 5 || sent == 6, "a second at 5 after 5,000 sent " + sent);
    }

    /** A rate of nothing sends nothing, however many turns it is asked. */
    @Test
    void aRateOfNothingSendsNothing() {
        BatchPacer pacer = new BatchPacer(PERIOD);

        for (int turn = 0; turn < 100; turn++) {
            assertEquals(0, pacer.next(0));
        }
    }

    /**
     * The five-millisecond floor is a period too, and it divides a second exactly 200 ways.
     *
     * <p>BATCH_PERIOD_MS is configurable, so the accounting must not assume the 50 ms default.
     */
    @Test
    void theShortestCadenceStillSendsTheRate() {
        BatchPacer pacer = new BatchPacer(Duration.ofMillis(5));
        int sent = 0;
        for (int turn = 0; turn < 200; turn++) {
            int batch = pacer.next(5);
            assertTrue(batch <= 1, "a burst at the shortest cadence: " + batch);
            sent += batch;
        }

        assertEquals(5, sent);
    }
}
