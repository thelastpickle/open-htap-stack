package com.thelastpickle.htap.backend.engine;

import java.time.Duration;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;
import org.jboss.logging.Logger;

/**
 * The lazy connect an access path shares: connect once, retry no oftener than the interval,
 * and report a failure rather than raising it.
 *
 * <p>The throttle is what stops an outage becoming a connection storm. The dashboard polls
 * every few seconds and several endpoints attempt a connect on the way in, so without it a
 * down engine would be dialled once per request per browser tab.
 *
 * <p>A forced connect ignores both the connected state and the throttle, and raises. That is
 * the operator's own re-sync button, where a failure is the answer rather than something to
 * hide.
 */
public final class ConnectionGate {

    private static final Logger LOG = Logger.getLogger(ConnectionGate.class);

    /** What a path does to build its connection; anything it throws is a failed attempt. */
    @FunctionalInterface
    public interface Attempt {
        void run() throws Exception;
    }

    private final String path;
    private final long retryIntervalNanos;
    private final LongSupplier nanoClock;
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * Whether an attempt has been made at all, which the elapsed comparison cannot tell on
     * its own: {@code System.nanoTime()} has no defined origin, so the first reading may be
     * any value including a negative one, and there is no "never" to initialise a timestamp
     * to. Python's {@code time.monotonic()} starts near zero on the platforms this ran on,
     * which is why the original could use {@code 0.0} and this cannot.
     */
    private boolean attempted;

    private long lastAttemptNanos;
    private volatile boolean connected;

    public ConnectionGate(String path, Duration retryInterval) {
        this(path, retryInterval, System::nanoTime);
    }

    ConnectionGate(String path, Duration retryInterval, LongSupplier nanoClock) {
        this.path = path;
        this.retryIntervalNanos = retryInterval.toNanos();
        this.nanoClock = nanoClock;
    }

    public boolean connected() {
        return connected;
    }

    /** Marks the path disconnected, for a caller that found its connection dead in use. */
    public void invalidate() {
        connected = false;
    }

    /**
     * Run {@code attempt} unless the path is already connected or was tried too recently.
     *
     * @throws EngineUnavailable when {@code force} and the attempt threw
     */
    public void connect(boolean force, Attempt attempt) {
        lock.lock();
        try {
            if (connected && !force) {
                return;
            }
            long now = nanoClock.getAsLong();
            if (!force && attempted && now - lastAttemptNanos < retryIntervalNanos) {
                return;
            }
            attempted = true;
            lastAttemptNanos = now;
            try {
                attempt.run();
                connected = true;
                LOG.infof("%s connected", path);
            } catch (Exception e) {
                connected = false;
                LOG.warnf("%s connection failed: %s", path, e);
                if (force) {
                    throw new EngineUnavailable(path + " connection failed: " + e, e);
                }
            }
        } finally {
            lock.unlock();
        }
    }
}
