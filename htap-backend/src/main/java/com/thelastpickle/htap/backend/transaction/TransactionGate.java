package com.thelastpickle.htap.backend.transaction;

import jakarta.enterprise.context.ApplicationScoped;
import java.util.concurrent.Semaphore;

/**
 * One transaction demonstration at a time, across both halves of the page.
 *
 * <p>Two overlapping runs would each be timed while the other ran, which is the reason the
 * comparison page holds a gate of its own. Shared between the session sequence and the clearance
 * semaphore because both write, and a clearance run would move the slots a session run's probe was
 * reading past.
 *
 * <p>A second caller is refused rather than queued: one that waited its turn would be timed while
 * the run ahead of it finished.
 */
@ApplicationScoped
public class TransactionGate {

    private final Semaphore turn = new Semaphore(1);

    /** Takes the gate, or answers false. Every caller must pair a true with {@link #leave}. */
    public boolean tryEnter() {
        return turn.tryAcquire();
    }

    public void leave() {
        turn.release();
    }
}
