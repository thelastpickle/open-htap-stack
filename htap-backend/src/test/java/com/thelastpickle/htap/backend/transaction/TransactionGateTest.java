package com.thelastpickle.htap.backend.transaction;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** One run at a time, and a second caller refused rather than made to wait. */
class TransactionGateTest {

    @Test
    void aSecondCallerIsRefusedWhileTheFirstHoldsIt() {
        TransactionGate gate = new TransactionGate();

        assertTrue(gate.tryEnter());
        assertFalse(gate.tryEnter());
    }

    @Test
    void leavingLetsTheNextCallerIn() {
        TransactionGate gate = new TransactionGate();
        gate.tryEnter();

        gate.leave();

        assertTrue(gate.tryEnter());
    }

    /**
     * The refusal has to hold across threads, since that is the only place it matters: a queued
     * caller would be timed while the run ahead of it finished, which is the figure the gate exists
     * to protect.
     */
    @Test
    void theRefusalIsImmediateOnAnotherThread() throws InterruptedException {
        TransactionGate gate = new TransactionGate();
        gate.tryEnter();
        CountDownLatch asked = new CountDownLatch(1);
        boolean[] entered = new boolean[1];

        Thread other = Thread.ofPlatform().start(() -> {
            entered[0] = gate.tryEnter();
            asked.countDown();
        });

        assertTrue(asked.await(5, TimeUnit.SECONDS), "the second caller queued rather than refusing");
        other.join();
        assertFalse(entered[0]);
    }
}
