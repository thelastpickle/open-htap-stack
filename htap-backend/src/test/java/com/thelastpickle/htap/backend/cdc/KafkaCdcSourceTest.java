package com.thelastpickle.htap.backend.cdc;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/**
 * Where the tail starts reading each partition.
 *
 * <p>The arithmetic alone, which is the part that can be wrong without a broker saying so: whether the
 * assign and the seek take is settled by running the tail against the stack.
 */
class KafkaCdcSourceTest {

    /** One bufferful is shared between the partitions, so twelve of them read a sixteenth each. */
    @Test
    void oneBufferfulIsSharedBetweenThePartitions() {
        assertEquals(16, KafkaCdcSource.backfillEach(200, 12));
        assertEquals(200, KafkaCdcSource.backfillEach(200, 1));
    }

    /** At least one record per partition, however many there are: a seek to the end shows nothing. */
    @Test
    void everyPartitionIsReadBackAtLeastOneRecord() {
        assertEquals(1, KafkaCdcSource.backfillEach(200, 400));
        assertEquals(1, KafkaCdcSource.backfillEach(0, 0));
    }

    /** A partition longer than the backfill is read from a bufferful back. */
    @Test
    void aLongPartitionIsReadFromNearItsEnd() {
        assertEquals(984, KafkaCdcSource.startOffset(0, 1000, 16));
    }

    /** A partition shorter than the backfill is read from its beginning, not from before it. */
    @Test
    void aShortPartitionIsReadFromItsBeginning() {
        assertEquals(0, KafkaCdcSource.startOffset(0, 10, 16));
        // A partition whose head has been deleted by retention begins above zero.
        assertEquals(500, KafkaCdcSource.startOffset(500, 510, 16));
    }
}
