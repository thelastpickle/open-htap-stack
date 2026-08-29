package com.thelastpickle.htap.backend.read;

import com.thelastpickle.htap.backend.support.Round;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.function.LongSupplier;

/**
 * Events a second, from the change in the sink's own counter between two observations.
 *
 * <p>Dividing the counter by elapsed bucket time would understate the rate whenever the stack
 * was started mid-bucket, and would keep understating it for up to half an hour. Differencing
 * consecutive observations measures what is arriving now, which is what a live dashboard
 * claims to show.
 */
@ApplicationScoped
public class IngestionRate {

    /**
     * Shortest gap between samples worth differencing. Below it the counter's own write
     * latency dominates the arithmetic.
     */
    static final long MIN_INTERVAL_NANOS = 2_000_000_000L;

    private record Sample(long atNanos, long totalEvents) {}

    private final LongSupplier nanoClock;
    private Sample previous;
    private double lastRate;

    IngestionRate() {
        this(System::nanoTime);
    }

    IngestionRate(LongSupplier nanoClock) {
        this.nanoClock = nanoClock;
    }

    /**
     * The rate implied by this observation and the last one.
     *
     * <p>0.0 until there are two observations to compare, and the previous figure when called
     * again too soon to difference meaningfully. Called too soon it keeps the older baseline
     * rather than replacing it, so a dashboard polling faster than the interval still gets a
     * fresh figure every two seconds instead of none.
     */
    public synchronized double observe(long totalEvents) {
        long now = nanoClock.getAsLong();
        Sample last = previous;
        if (last == null) {
            previous = new Sample(now, totalEvents);
            return 0.0;
        }
        long elapsedNanos = now - last.atNanos();
        if (elapsedNanos < MIN_INTERVAL_NANOS) {
            return lastRate;
        }
        previous = new Sample(now, totalEvents);
        long delta = totalEvents - last.totalEvents();
        // The counters were truncated, by cleanup-data.sh or by a wipe; start again here
        // rather than reporting a large negative rate.
        lastRate = delta < 0 ? 0.0 : Round.tenth(delta / (elapsedNanos / 1e9));
        return lastRate;
    }
}
