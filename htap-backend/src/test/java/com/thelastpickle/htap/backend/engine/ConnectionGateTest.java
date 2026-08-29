package com.thelastpickle.htap.backend.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class ConnectionGateTest {

    private static final Duration RETRY = Duration.ofSeconds(10);

    private final AtomicLong clock = new AtomicLong();
    private final AtomicInteger attempts = new AtomicInteger();

    private ConnectionGate gate() {
        return new ConnectionGate("test", RETRY, clock::get);
    }

    @Test
    void aSuccessfulAttemptConnectsAndIsNotRepeated() {
        ConnectionGate gate = gate();
        gate.connect(false, attempts::incrementAndGet);
        gate.connect(false, attempts::incrementAndGet);

        assertTrue(gate.connected());
        assertEquals(1, attempts.get());
    }

    @Test
    void aFailedAttemptIsReportedRatherThanRaised() {
        ConnectionGate gate = gate();
        gate.connect(false, () -> {
            throw new IllegalStateException("no route");
        });

        assertFalse(gate.connected());
    }

    @Test
    void aSecondAttemptWaitsForTheRetryInterval() {
        ConnectionGate gate = gate();
        gate.connect(false, failing());
        clock.addAndGet(RETRY.toNanos() - 1);
        gate.connect(false, failing());

        assertEquals(1, attempts.get());

        clock.addAndGet(1);
        gate.connect(false, failing());

        assertEquals(2, attempts.get());
    }

    /**
     * The throttle has to know that no attempt has been made, which elapsed time cannot tell
     * it: {@code System.nanoTime()} may read 0 or any other value at the first call, so a
     * zero-initialised timestamp would look like an attempt made just now and suppress the
     * first one.
     */
    @Test
    void theFirstAttemptIsMadeWhenTheClockReadsZero() {
        ConnectionGate gate = new ConnectionGate("test", RETRY, () -> 0L);
        gate.connect(false, attempts::incrementAndGet);

        assertTrue(gate.connected());
        assertEquals(1, attempts.get());
    }

    @Test
    void aForcedAttemptIgnoresTheThrottleAndTheConnectedState() {
        ConnectionGate gate = gate();
        gate.connect(false, attempts::incrementAndGet);
        gate.connect(true, attempts::incrementAndGet);

        assertEquals(2, attempts.get());
    }

    @Test
    void aForcedFailureRaises() {
        ConnectionGate gate = gate();
        EngineUnavailable raised = assertThrows(
                EngineUnavailable.class,
                () -> gate.connect(true, () -> {
                    throw new IllegalStateException("no route");
                }));

        assertTrue(raised.getMessage().contains("no route"));
        assertFalse(gate.connected());
    }

    @Test
    void anInvalidatedGateConnectsAgain() {
        ConnectionGate gate = gate();
        gate.connect(false, attempts::incrementAndGet);
        gate.invalidate();
        clock.addAndGet(RETRY.toNanos());
        gate.connect(false, attempts::incrementAndGet);

        assertTrue(gate.connected());
        assertEquals(2, attempts.get());
    }

    /**
     * A top-up neither connects nor disconnects: the cqlite reader registers a table at a time and
     * reads the ones it has, so a failure looking for the rest must leave those alone.
     */
    @Test
    void aTopUpLeavesTheConnectedStateAsItWas() {
        ConnectionGate gate = gate();
        gate.topUp(attempts::incrementAndGet);

        assertFalse(gate.connected());
        assertEquals(1, attempts.get());

        clock.addAndGet(RETRY.toNanos());
        gate.connect(false, attempts::incrementAndGet);
        clock.addAndGet(RETRY.toNanos());
        gate.topUp(failing());

        assertTrue(gate.connected());
        assertEquals(3, attempts.get());
    }

    @Test
    void aTopUpSharesTheThrottleWithConnect() {
        ConnectionGate gate = gate();
        gate.connect(false, failing());
        gate.topUp(attempts::incrementAndGet);

        assertEquals(1, attempts.get());

        clock.addAndGet(RETRY.toNanos());
        gate.topUp(attempts::incrementAndGet);

        assertEquals(2, attempts.get());
    }

    private ConnectionGate.Attempt failing() {
        return () -> {
            attempts.incrementAndGet();
            throw new IllegalStateException("no route");
        };
    }
}
