package com.thelastpickle.htap.backend.sql;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/** One batch at a time, refused rather than queued. */
class ConsoleGateTest {

    @Test
    void aSecondBatchIsRefusedWhileTheFirstHoldsTheGate() {
        ConsoleGate gate = new ConsoleGate();
        assertTrue(gate.tryEnter());
        assertFalse(gate.tryEnter());
        gate.leave();
        assertTrue(gate.tryEnter());
        gate.leave();
    }

    /**
     * The refusal is what another thread sees, not a wait.
     *
     * <p>A single connection means a queued caller would sit on the client's lock for the length of
     * the batch ahead of it, and a reset sends twenty-five statements.
     */
    @Test
    void anotherThreadIsRefusedRatherThanBlocked() throws InterruptedException {
        ConsoleGate gate = new ConsoleGate();
        assertTrue(gate.tryEnter());
        AtomicBoolean entered = new AtomicBoolean(true);
        CountDownLatch answered = new CountDownLatch(1);
        Thread other = new Thread(() -> {
            entered.set(gate.tryEnter());
            answered.countDown();
        });
        other.start();
        assertTrue(answered.await(5, TimeUnit.SECONDS), "the second caller blocked");
        assertFalse(entered.get());
        other.join();
        gate.leave();
    }
}
