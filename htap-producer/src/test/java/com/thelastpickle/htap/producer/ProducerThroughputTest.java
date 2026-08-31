package com.thelastpickle.htap.producer;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Locale;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * What one event costs to build, which is the part of the rate this process owns.
 *
 * <p>Everything up to {@code producer.send}: advancing the model, minting the identifier and writing
 * the JSON. What happens after it is the broker's, and no figure here is a claim about that.
 *
 * <p>The assertion is a floor two orders of magnitude below what a laptop does, because this runs on
 * whatever machine the suite runs on and a tight bound would be a flaky test rather than a
 * measurement. The figure itself is printed and recorded in the commit message.
 */
class ProducerThroughputTest {

    /** Enough to leave the interpreter behind: the first few thousand events are all warm-up. */
    private static final int EVENTS = 200_000;

    private static final int FLEET = 2000;

    /**
     * The floor, in events a second.
     *
     * <p>The rate measured against is 2,000 a second, which is what the docs' figures were taken at
     * and what the Settings page is turned up to for a run; the shipped default is 5.  This loop
     * does far more than 2,000, and 20,000 is the rate below which the producer would be the reason
     * the demo could not reach its own figure, which is what this assertion is for.
     */
    private static final double FLOOR_PER_SECOND = 20_000.0;

    @Test
    @Timeout(120)
    void buildingAnEventCostsFarLessThanTheDemoAsksFor() {
        double at = 1787846133.0;
        Fleet fleet = new Fleet(new FleetState(FleetConfig.of(FLEET), at), FLEET);

        // A pass that is not measured, so the figure is of compiled code rather than of the
        // interpreter and the first class loads.
        run(fleet, at, 20_000);

        long startedAt = System.nanoTime();
        int built = run(fleet, at + 60.0, EVENTS);
        double seconds = (System.nanoTime() - startedAt) / 1e9;
        double perSecond = built / seconds;

        System.out.println(String.format(
                Locale.ROOT,
                "[measured] %d events built in %.3f s: %.0f events/s, %.2f us each",
                built,
                seconds,
                perSecond,
                seconds * 1e6 / built));
        assertTrue(
                perSecond > FLOOR_PER_SECOND,
                String.format(Locale.ROOT, "only %.0f events a second, floor is %.0f",
                        perSecond, FLOOR_PER_SECOND));
    }

    /** One batch after another, as the send loop drives it, and nothing kept. */
    private static int run(Fleet fleet, double from, int events) {
        int built = 0;
        double at = from;
        // 100 events a batch, which is 2,000 a second over the 50 ms cadence the image ships with;
        // the pacer is not in the way here, because this measures what one event costs to build.
        while (built < events) {
            int[] ids = fleet.next(100, FLEET);
            built += fleet.batch(ids, at, 0.05, TextSource.NONE, 5.0, 30.0, 0.05).length;
            at += 0.05;
        }
        return built;
    }
}
