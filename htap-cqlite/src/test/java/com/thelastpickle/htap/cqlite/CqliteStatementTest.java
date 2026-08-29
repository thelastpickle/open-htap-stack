package com.thelastpickle.htap.cqlite;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.junit.jupiter.api.Test;

/**
 * What a failed drain becomes, without the library.
 *
 * <p>The rest of the class needs a running statement and is covered by {@link
 * CqliteNativeTest}, which reports itself skipped where the library cannot load. This
 * mapping needs neither, and it is the one a viewer sees.
 */
class CqliteStatementTest {

    @Test
    void anOrdinaryFailureKeepsItsMessage() {
        CqliteException failure = CqliteStatement.drainFailure(
                "DataFusion error: External error: cqlite: no SSTable files", false);
        assertFalse(failure.cancelled());
        assertEquals("no SSTable files", failure.getMessage());
        assertEquals(CqliteStatus.ERROR, failure.status());
    }

    @Test
    void thisStatementsOwnCancelIsReportedAsOne() {
        CqliteException failure = CqliteStatement.drainFailure("the merge stopped", true);
        assertTrue(failure.cancelled());
        assertEquals(CqliteStatement.CANCELLED_MESSAGE, failure.getMessage());
    }

    /** A cancel some other holder of the statement made, which this side did not request. */
    @Test
    void aMessageSayingCancelledIsReportedAsOne() {
        CqliteException failure =
                CqliteStatement.drainFailure("External error: cqlite: scan cancelled", false);
        assertTrue(failure.cancelled());
        assertEquals(CqliteStatement.CANCELLED_MESSAGE, failure.getMessage());
    }

    /**
     * The price of reading the message: a genuine failure whose text says "cancelled" is
     * reported as a cancellation, and there is nothing at the boundary to tell them apart.
     */
    @Test
    void aFailureThatMerelyMentionsCancellingIsTakenForOne() {
        CqliteException failure = CqliteStatement.drainFailure(
                "No field named cancelled_at. Valid fields are id, at.", false);
        assertTrue(failure.cancelled());
    }

    /**
     * The refusal a sink meets if it closes the statement it is draining. Reaching it through
     * {@code close()} needs a live statement, which only the native test has.
     */
    @Test
    void aCloseFromInsideTheDrainIsRefusedAndSaysWhatToCallInstead() {
        assertDoesNotThrow(() -> CqliteStatement.refuseCloseInsideDrain(0));

        IllegalStateException refused = assertThrows(
                IllegalStateException.class, () -> CqliteStatement.refuseCloseInsideDrain(1));
        assertTrue(
                refused.getMessage().contains("cancel()"),
                "the refusal names the call that does work, and said: " + refused.getMessage());
    }

    @Test
    void aDrainWithNoMessageStillReports() {
        CqliteException failure = CqliteStatement.drainFailure(null, false);
        assertFalse(failure.cancelled());
        assertEquals("", failure.getMessage());
    }

    /**
     * What {@link CqliteStatement#cancel()} rests on, pinned as an assumption rather than
     * as this class's own behaviour: {@code tryLock} takes the read lock while a writer is
     * queued, where {@code lock} waits behind it. A JDK that made a queued writer block
     * {@code tryLock} too would turn a cancel during a long drain back into a wait for the
     * drain it is stopping, and nothing else here would say so.
     */
    @Test
    void aReadLockBargesPastAQueuedWriterOnlyWhenItTries() throws InterruptedException {
        ReentrantReadWriteLock lifetime = new ReentrantReadWriteLock();
        lifetime.readLock().lock();
        Thread close = new Thread(
                () -> {
                    lifetime.writeLock().lock();
                    lifetime.writeLock().unlock();
                },
                "close");
        close.start();
        while (!lifetime.hasQueuedThread(close)) {
            Thread.onSpinWait();
        }

        assertTrue(lifetime.readLock().tryLock(), "tryLock, which is what cancel() takes");
        lifetime.readLock().unlock();

        Thread waiting = new Thread(() -> lifetime.readLock().lock(), "cancel-with-lock");
        waiting.start();
        waiting.join(100);
        assertTrue(waiting.isAlive(), "lock() queued behind the writer, as cancel() must not");

        lifetime.readLock().unlock();
        close.join(2_000);
        waiting.join(2_000);
        assertFalse(close.isAlive(), "the writer never acquired");
    }

    /**
     * What {@link CqliteStatement#close()}'s refusal rests on: the hold count is this
     * thread's alone, so it tells a sink closing mid-drain from an ordinary close on
     * another thread while a drain runs, which must wait rather than be refused.
     */
    @Test
    void theReadHoldCountIsPerThread() throws InterruptedException {
        ReentrantReadWriteLock lifetime = new ReentrantReadWriteLock();
        lifetime.readLock().lock();
        try {
            assertEquals(1, lifetime.getReadHoldCount(), "the drain's own thread");
            AtomicInteger elsewhere = new AtomicInteger(-1);
            Thread other = new Thread(() -> elsewhere.set(lifetime.getReadHoldCount()), "close");
            other.start();
            other.join(2_000);
            assertEquals(0, elsewhere.get(), "a thread that is not draining");
        } finally {
            lifetime.readLock().unlock();
        }
        assertEquals(0, lifetime.getReadHoldCount(), "after the drain");
    }

    /** The other half of that contract: closing holds the write lock, so there is no cancel. */
    @Test
    void aTryLockFailsWhileTheWriteLockIsHeld() throws InterruptedException {
        ReentrantReadWriteLock lifetime = new ReentrantReadWriteLock();
        lifetime.writeLock().lock();
        try {
            AtomicBoolean took = new AtomicBoolean(true);
            Thread cancel = new Thread(() -> took.set(lifetime.readLock().tryLock()), "cancel");
            cancel.start();
            cancel.join(2_000);
            assertFalse(took.get(), "tryLock while a close holds the write lock");
        } finally {
            lifetime.writeLock().unlock();
        }
    }
}
