package com.thelastpickle.htap.backend.query;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

/**
 * One partition read over and over on a thread of its own, keeping the latencies.
 *
 * <p>Started before a path runs and closed when it has, so the readings cover exactly the window
 * that path was working in. Every path here reads the same single node, so a path that scans the
 * whole history is expected to be felt.
 *
 * <p>A read that raises is counted rather than allowed to end the sampling: a point read that never
 * came back is what the comparison is looking for.
 */
public final class OltpProbe implements AutoCloseable {

    /**
     * How often the partition is read.
     *
     * <p>Four reads a second is far below what the dashboard's own polling already costs, so the
     * probe does not itself become the noisy neighbour it is measuring.
     */
    static final Duration INTERVAL = Duration.ofMillis(250);

    /** How long {@link #close} waits for the sampling thread before giving up on it. */
    private static final Duration JOIN_TIMEOUT = Duration.ofSeconds(5);

    private final Runnable read;
    private final Duration interval;
    private final LongSupplier nanoClock;
    private final CountDownLatch stop = new CountDownLatch(1);
    private final List<Double> latencies = Collections.synchronizedList(new ArrayList<>());
    private final AtomicInteger failures = new AtomicInteger();
    private final Thread thread;

    /** Sample {@code read} until closed. */
    public static OltpProbe start(Runnable read) {
        return start(read, INTERVAL, System::nanoTime);
    }

    static OltpProbe start(Runnable read, Duration interval, LongSupplier nanoClock) {
        return new OltpProbe(read, interval, nanoClock);
    }

    private OltpProbe(Runnable read, Duration interval, LongSupplier nanoClock) {
        this.read = read;
        this.interval = interval;
        this.nanoClock = nanoClock;
        this.thread = Thread.ofPlatform().daemon().name("oltp-probe").start(this::loop);
    }

    /** The readings so far, which a caller may ask for before closing. */
    public OltpImpact impact() {
        List<Double> taken;
        synchronized (latencies) {
            taken = List.copyOf(latencies);
        }
        return OltpImpact.of(taken, failures.get());
    }

    @Override
    public void close() {
        stop.countDown();
        try {
            thread.join(JOIN_TIMEOUT);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void loop() {
        while (stop.getCount() > 0) {
            long began = nanoClock.getAsLong();
            try {
                read.run();
                latencies.add((nanoClock.getAsLong() - began) / 1_000_000.0);
            } catch (RuntimeException e) {
                failures.incrementAndGet();
            }
            pause();
        }
    }

    /** Wait out the interval, and return early when the probe is closed mid-wait. */
    private void pause() {
        try {
            stop.await(interval.toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            stop.countDown();
        }
    }
}
