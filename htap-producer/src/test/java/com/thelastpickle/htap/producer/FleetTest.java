package com.thelastpickle.htap.producer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.thelastpickle.htap.common.EventPartitions;
import com.thelastpickle.htap.common.TimeUuids;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/** The rotation through the fleet, the keys the broker partitions by, and the stamps. */
class FleetTest {

    private static final double NOON = 1787846133.0;

    private static final Pattern EVENT_ID =
            Pattern.compile("\"event_id\":\"([0-9a-f-]{36})\"");

    private final Fleet fleet = new Fleet(new FleetState(FleetConfig.of(200), NOON), 200);

    /** The identifier is the Python's, six digits and zero-padded, and the key is its bytes. */
    @Test
    void theKeyIsTheAssetsOwnIdentifier() {
        assertEquals("asset-000000", fleet.entityId(0));
        assertEquals("asset-000199", fleet.entityId(199));

        Fleet.Event[] batch = batch(new int[] {7});
        assertEquals("asset-000007", new String(batch[0].key(), StandardCharsets.UTF_8));
    }

    /**
     * The cursor advances past a batch rather than restarting it.
     *
     * <p>Which is what gives every asset an even cadence: a fleet of 200 at a batch of 100 reports
     * on the first hundred, then the second hundred, then wraps.
     */
    @Test
    void theRotationCoversTheWholeFleet() {
        assertEquals(0, fleet.cursor());
        int[] first = fleet.next(100, 200);
        int[] second = fleet.next(100, 200);
        int[] third = fleet.next(100, 200);

        assertEquals(0, first[0]);
        assertEquals(99, first[99]);
        assertEquals(100, second[0]);
        assertEquals(0, third[0], "the rotation wrapped");
        assertEquals(100, fleet.cursor());
    }

    /** A batch larger than the live fleet wraps within itself rather than indexing past it. */
    @Test
    void aBatchLargerThanTheFleetWraps() {
        int[] ids = fleet.next(250, 100);

        assertEquals(0, ids[100], "index 100 of a 100-asset fleet is asset 0 again");
        assertTrue(java.util.Arrays.stream(ids).allMatch(id -> id >= 0 && id < 100));
        assertEquals(50, fleet.cursor());
    }

    /**
     * The events of one batch are stamped across the window the batch represents.
     *
     * <p>Not one instant for the batch: the id is a timeuuid and the sink derives
     * {@code event_time} from it, so identical stamps would collapse an asset's history into a
     * point and leave the order within the batch undefined.
     */
    @Test
    void aBatchIsStampedAcrossItsWindow() {
        List<UUID> ids = eventIds(batch(fleet.next(100, 200)));

        List<Instant> stamps = ids.stream().map(TimeUuids::instantOf).toList();
        assertEquals(100, new HashSet<>(stamps).size(), "two events shared a stamp");
        for (int k = 1; k < stamps.size(); k++) {
            assertTrue(stamps.get(k).isAfter(stamps.get(k - 1)), "the stamps are out of order");
        }
        // 50 ms over 100 events is 500 microseconds apart, which is what the resolution has to
        // carry: rounding to the millisecond would give twenty events the same stamp.
        assertEquals(
                500_000L,
                java.time.Duration.between(stamps.get(0), stamps.get(1)).toNanos(),
                "the stamp step is not half a millisecond");
        assertEquals(
                49_500_000L,
                java.time.Duration.between(stamps.get(0), stamps.get(99)).toNanos(),
                "the batch does not span its own window");
    }

    /** Every identifier a batch mints is distinct, which is what the events table's key needs. */
    @Test
    void everyEventIdIsDistinct() {
        Set<UUID> ids = new HashSet<>(eventIds(batch(fleet.next(200, 200))));

        assertEquals(200, ids.size());
    }

    /**
     * The identifiers spread across the shards, which is what keeps a window's partitions even.
     *
     * <p>The sink derives the shard from the id through {@link EventPartitions#shard}, so this is
     * the producer's side of that arrangement: an id source drawing one node for the whole process
     * would put every row of a window in one partition, which is the failure this rules out.
     *
     * <p>Fourteen of sixteen rather than all sixteen, because 200 draws over 16 shards leaves a
     * shard empty about once in twenty-five thousand runs and a flaky assertion is worse than a
     * loose one; one shard is what a broken id source gives.
     */
    @Test
    void theIdentifiersSpreadAcrossTheShards() {
        Set<Integer> shards = new HashSet<>();
        for (UUID id : eventIds(batch(fleet.next(200, 200)))) {
            shards.add(EventPartitions.shard(id, 16));
        }

        assertTrue(shards.size() >= 14, "the ids reached only " + shards.size() + " shards: " + shards);
    }

    /** Microsecond resolution, because the stamp step needs it at the demo's rate. */
    @Test
    void anInstantKeepsItsMicroseconds() {
        assertEquals(
                Instant.parse("2026-08-27T15:55:33.000500Z"), Fleet.instantOf(NOON + 0.0005));
        assertEquals(Instant.parse("2026-08-27T15:55:33Z"), Fleet.instantOf(NOON));
    }

    private Fleet.Event[] batch(int[] ids) {
        return fleet.batch(ids, NOON, 0.05, TextSource.NONE, 5.0, 30.0, 0.05);
    }

    private static List<UUID> eventIds(Fleet.Event[] events) {
        List<UUID> ids = new ArrayList<>(events.length);
        for (Fleet.Event event : events) {
            Matcher matcher = EVENT_ID.matcher(new String(event.value(), StandardCharsets.UTF_8));
            assertTrue(matcher.find(), "no event_id in the event");
            ids.add(UUID.fromString(matcher.group(1)));
        }
        return ids;
    }
}
