package com.thelastpickle.htap.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.thelastpickle.htap.common.TimeUuids;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** What one record on the topic becomes, and what an unusable one becomes instead. */
class EventTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Instant NOW = Instant.parse("2026-08-29T12:00:00Z");

    /** The producer's own record, field for field. */
    @Test
    void everyFieldIsRead() throws Exception {
        UUID id = TimeUuids.timeUuid(Instant.parse("2026-08-29T11:59:58.500Z"));
        Event event = read("""
                {"event_id": "%s", "entity_id": "asset-000042",
                 "position": {"lat": 59.91, "lon": 10.75}, "z_m": 103.5,
                 "event_type": "telemetry", "observer_id": "observer-0003",
                 "temp_external_c": -13.25, "temp_internal_c": 2.5,
                 "text": "Urban planning is a technical and political process"}""".formatted(id));

        assertEquals(id, event.eventId());
        assertEquals("asset-000042", event.entityId());
        assertEquals(59.91, event.latitude());
        assertEquals(10.75, event.longitude());
        assertEquals(103.5, event.altitudeM());
        assertEquals("telemetry", event.eventType());
        assertEquals("observer-0003", event.observerId());
        assertEquals(-13.25, event.tempExternalC());
        assertEquals(2.5, event.tempInternalC());
        assertEquals("Urban planning is a technical and political process", event.textPayload());
    }

    /**
     * The time comes from the id and from nothing else.
     *
     * <p>The producer stamps a version-1 UUID at the reading's own time, so the id is the time; the
     * bucket the row is filed under is derived from it, and the backend derives the time back out of
     * the id, which means a second source for it could only disagree.
     */
    @Test
    void theTimeIsTheIdsOwn() throws Exception {
        Instant stamped = Instant.parse("2026-08-29T11:59:58.500Z");
        Event event = read("{\"event_id\": \"%s\"}".formatted(TimeUuids.timeUuid(stamped)));

        assertEquals(stamped, event.eventTime());
    }

    /** A record missing everything is zeros and empty strings rather than a failure. */
    @Test
    void anEmptyRecordIsZerosAndEmptyStrings() throws Exception {
        Event event = read("{}");

        assertEquals("", event.entityId());
        assertEquals(0.0, event.latitude());
        assertEquals(0.0, event.longitude());
        assertEquals(0.0, event.altitudeM());
        assertEquals("", event.eventType());
        assertEquals("", event.observerId());
        assertEquals(0.0, event.tempExternalC());
        assertEquals(0.0, event.tempInternalC());
        assertEquals("", event.textPayload());
    }

    /** A position that is absent is not a position of nulls: the two fields default separately. */
    @Test
    void anAbsentPositionIsTheOrigin() throws Exception {
        Event event = read("{\"position\": {\"lat\": 59.91}}");

        assertEquals(59.91, event.latitude());
        assertEquals(0.0, event.longitude());
    }

    /**
     * An id that cannot be used is replaced, and the replacement agrees with the time it is filed
     * under.
     *
     * <p>Both an unparseable id and one that is not version 1 reach the same branch, because neither
     * can give an event time.
     */
    @Test
    void anUnusableIdIsMintedAtTheGivenInstant() throws Exception {
        Event unparseable = read("{\"event_id\": \"not-a-uuid\"}");
        Event version4 = read("{\"event_id\": \"%s\"}".formatted(UUID.randomUUID()));

        for (Event event : new Event[] {unparseable, version4}) {
            assertEquals(NOW, event.eventTime());
            assertEquals(1, event.eventId().version());
            assertEquals(NOW, TimeUuids.instantOf(event.eventId()),
                    "the minted id must carry the time the row is filed under");
        }
        assertNotEquals(unparseable.eventId(), version4.eventId());
    }

    /** An absent id is the same case as an unusable one. */
    @Test
    void anAbsentIdIsMintedToo() throws Exception {
        assertEquals(1, read("{}").eventId().version());
    }

    private static Event read(String json) throws Exception {
        return Event.from(JSON.readTree(json), NOW);
    }
}
