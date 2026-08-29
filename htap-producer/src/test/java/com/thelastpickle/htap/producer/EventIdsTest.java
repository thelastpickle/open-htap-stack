package com.thelastpickle.htap.producer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.thelastpickle.htap.common.TimeUuids;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The identifiers the fleet mints, stamped from the producer's own clock arithmetic.
 *
 * <p>{@code demo.events} is keyed on {@code ((event_bucket, shard), event_id)} and the sink derives
 * {@code event_time} from the identifier, so what matters here is that a batch's worth of stamps
 * taken through {@link Fleet#instantOf} rise.  Distinctness itself belongs to {@code TimeUuids} and
 * is asserted beside it, in {@code htap-common}, including the eight-thread case; {@code FleetTest}
 * covers this module's own use of it.
 */
class EventIdsTest {

    /** A fleet's worth: the largest the dashboard may ask for, and a batch at the demo's rate. */
    private static final int FLEET = 2000;

    /**
     * Identifiers minted at rising instants read back in that order.
     *
     * <p>Which is what the sink relies on: it takes {@code event_time} from the identifier, and the
     * clustering order within a partition is the identifier's own. Distinctness alone would leave an
     * asset's history unordered.
     */
    @Test
    void identifiersFromRisingInstantsAreOrdered() {
        double at = 1787846133.0;
        List<UUID> ids = new ArrayList<>(FLEET);
        for (int i = 0; i < FLEET; i++) {
            // 500 microseconds apart, which is a 50 ms batch at 2,000 events a second.
            ids.add(TimeUuids.timeUuid(Fleet.instantOf(at + i * 0.0005)));
        }

        Set<Instant> stamps = new HashSet<>();
        for (int i = 0; i < ids.size(); i++) {
            Instant read = TimeUuids.instantOf(ids.get(i));
            stamps.add(read);
            if (i > 0) {
                assertTrue(
                        read.isAfter(TimeUuids.instantOf(ids.get(i - 1))),
                        "identifier " + i + " reads back before the one before it");
            }
        }
        assertEquals(FLEET, stamps.size(), "two events a batch apart shared a stamp");
        assertEquals(FLEET, new HashSet<>(ids).size());
    }

}
