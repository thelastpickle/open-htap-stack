package com.thelastpickle.htap.cqlite;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;

/**
 * That Arrow's own leak check is live in this fork, which is what makes the statement's
 * child allocator worth having.
 *
 * <p>What the property buys is measured rather than assumed, and it is the message and not
 * the check. With {@code arrow.memory.debug.allocator} unset, closing an allocator holding
 * a buffer still refuses, with "Memory was leaked by query. Memory leaked: (64)". With it
 * set, the same close reports "Allocator[leak-check] closed with outstanding buffers
 * allocated (1)" and the allocation history beneath it, and Arrow's outstanding-child
 * check, which {@link CqliteSession} relies on to report a failed statement start, appears
 * only then: that branch of {@code BaseAllocator.close()} sits behind {@code if (DEBUG)}.
 * So this test asserts the debug message, since the other arrives either way.
 */
class AllocatorLeakCheckTest {

    @Test
    void aBufferNobodyGaveBackIsReportedWhenItsAllocatorCloses() {
        RootAllocator root = new RootAllocator();
        BufferAllocator child = root.newChildAllocator("leak-check", 0, root.getLimit());
        // 40 bytes is the C Data Interface's struct, and Arrow rounds a request to the next
        // power of two, so the buffer holding it is 64.
        ArrowBuf leaked = child.buffer(40);
        assertEquals(64L, leaked.capacity());

        IllegalStateException reported = assertThrows(IllegalStateException.class, child::close);
        assertTrue(
                reported.getMessage().contains("outstanding buffers"),
                "Arrow's check reported: " + reported.getMessage());

        // Nothing is tidied up, and it cannot be: the failed close marked the child closed
        // while leaving it registered on the root, so giving the buffer back afterwards is
        // refused with "Attempting operation on allocator when allocator is closed" and
        // closing the root reports the child rather than anything this test is about.  The
        // 40 bytes stay with the fork, which is what a leak is.
    }
}
