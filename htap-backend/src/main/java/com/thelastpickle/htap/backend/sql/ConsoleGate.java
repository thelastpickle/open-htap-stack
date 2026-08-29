package com.thelastpickle.htap.backend.sql;

import jakarta.enterprise.context.ApplicationScoped;
import java.util.concurrent.Semaphore;

/**
 * One console batch at a time, refused rather than queued.
 *
 * <p>The client holds a single connection, so a second caller would wait on its lock anyway;
 * refusing says so instead. And the unit that must not interleave is the batch and not the
 * statement: a reset sends twenty-four statements, and another caller's {@code INSERT} landing
 * between the drops and the seed would leave a state neither caller asked for.
 *
 * <p>Not the transaction gate, although it does the same thing. Sharing one would have a console
 * statement refuse an Accord demonstration and the other way round, and the two touch no common
 * table: cassandra-sql keeps its rows in keyspaces of its own.
 */
@ApplicationScoped
public class ConsoleGate {

    private final Semaphore turn = new Semaphore(1);

    /** Takes the gate, or answers false. Every caller must pair a true with {@link #leave}. */
    public boolean tryEnter() {
        return turn.tryAcquire();
    }

    public void leave() {
        turn.release();
    }
}
