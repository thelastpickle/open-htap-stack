package com.thelastpickle.htap.producer;

import com.thelastpickle.htap.common.TimeUuids;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Locale;
import java.util.UUID;

/**
 * The assets themselves: their identifiers, their keys, and the batch a turn of the loop sends.
 *
 * <p>Identifiers and Kafka keys are built once for the largest fleet the dashboard may ask for, so
 * neither a rate change nor a fleet change allocates a string in the send loop.
 */
final class Fleet {

    /** One event ready to send: the key the broker partitions by, and the value. */
    record Event(byte[] key, byte[] value) {}

    private final String[] entityIds;
    private final byte[][] entityKeys;
    private final FleetState state;
    private final EventJson json = new EventJson();

    /** The round-robin cursor, so every asset reports at an even cadence. */
    private int cursor;

    Fleet(FleetState state, int maxEntities) {
        this.state = state;
        this.entityIds = new String[maxEntities];
        this.entityKeys = new byte[maxEntities][];
        for (int i = 0; i < maxEntities; i++) {
            // Locale.ROOT for the reason FleetState gives beside the observer's name.
            entityIds[i] = String.format(Locale.ROOT, "asset-%06d", i);
            entityKeys[i] = entityIds[i].getBytes(StandardCharsets.UTF_8);
        }
    }

    String entityId(int index) {
        return entityIds[index];
    }

    int cursor() {
        return cursor;
    }

    /**
     * The next {@code count} assets in the rotation, wrapped at the live fleet size.
     *
     * <p>The cursor advances past the batch rather than restarting, so a fleet larger than one
     * batch still has every asset reporting: the Python's own round-robin, and what keeps a
     * 2,000-asset fleet at 2,000 events a second from reporting on the first hundred only.
     */
    int[] next(int count, int liveEntities) {
        int[] ids = new int[count];
        for (int k = 0; k < count; k++) {
            ids[k] = (cursor + k) % liveEntities;
        }
        cursor = (cursor + count) % liveEntities;
        return ids;
    }

    /**
     * One batch of events, stamped across the window the batch represents.
     *
     * <p>The stamps are spread rather than identical, and that is not cosmetic: the id is a
     * timeuuid and the sink derives {@code event_time} from it, so one instant for a whole batch
     * would collapse an asset's history into a single point and leave the order within the batch
     * undefined.
     */
    Event[] batch(
            int[] ids,
            double nowSeconds,
            double windowSeconds,
            TextSource text,
            double refreshMinS,
            double refreshMaxS,
            double outlierFraction) {
        Telemetry[] telemetry =
                state.step(ids, nowSeconds, text, refreshMinS, refreshMaxS, outlierFraction);
        double stampStep = windowSeconds / ids.length;
        Event[] events = new Event[ids.length];
        for (int k = 0; k < ids.length; k++) {
            int index = telemetry[k].index();
            UUID eventId = TimeUuids.timeUuid(instantOf(nowSeconds + k * stampStep));
            events[k] = new Event(
                    entityKeys[index],
                    json.bytes(
                            eventId,
                            entityIds[index],
                            state.observerId(index),
                            EventTypes.of(index),
                            telemetry[k]));
        }
        return events;
    }

    /**
     * An instant at microsecond resolution from a count of seconds.
     *
     * <p>Microseconds because that is what the stamp step needs: at 2,000 events a second the
     * events of a 50 ms batch are 500 microseconds apart, and rounding to the millisecond would
     * give twenty events the same stamp. The Python passed the same {@code double} to
     * {@code uuid_from_time}, so both carry the same loss of precision at this magnitude, and
     * {@link TimeUuids#timeUuid} truncates to the microsecond in either case.
     */
    static Instant instantOf(double epochSeconds) {
        long micros = Math.round(epochSeconds * 1_000_000.0);
        return Instant.ofEpochSecond(
                Math.floorDiv(micros, 1_000_000L), Math.floorMod(micros, 1_000_000L) * 1_000L);
    }
}
