package com.thelastpickle.htap.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.thelastpickle.htap.common.TimeUuids;
import java.time.Instant;
import java.util.UUID;

/**
 * One telemetry reading, as the producer wrote it onto the topic.
 *
 * <p>Every field has a default, because a sink that refused a malformed record would stop the whole
 * demo over one bad message: an absent number reads as zero and an absent string as empty, which is
 * what the Python did.
 *
 * @param eventTime the mutation's own time, taken from the id rather than from any field. The
 *     producer mints a version-1 UUID stamped at the reading's time, so the id is the time, and
 *     nothing downstream can disagree with the partition the row was filed under
 */
record Event(
        UUID eventId,
        Instant eventTime,
        String entityId,
        double latitude,
        double longitude,
        double altitudeM,
        String eventType,
        String observerId,
        double tempExternalC,
        double tempInternalC,
        String textPayload) {

    /**
     * Reads one record, minting an id where the record's own cannot be used.
     *
     * <p>The fallback covers both an unparseable {@code event_id} and one that is not version 1,
     * since neither can give an event time. It differs from the Python in one way worth stating:
     * there the fallback read the clock twice, once for {@code uuid1()} and once for
     * {@code now()}, so the id and the time it was filed under were microseconds apart; here they
     * are the same instant, which is what {@code demo.events} needs, since the bucket comes from
     * the time and the backend derives the time back out of the id.
     *
     * @param now the time to stamp a record whose own id is unusable
     */
    static Event from(JsonNode json, Instant now) {
        UUID eventId;
        Instant eventTime;
        try {
            eventId = UUID.fromString(json.path("event_id").asText());
            eventTime = TimeUuids.instantOf(eventId);
        } catch (RuntimeException unusable) {
            eventId = TimeUuids.timeUuid(now);
            eventTime = now;
        }
        JsonNode position = json.path("position");
        return new Event(
                eventId,
                eventTime,
                json.path("entity_id").asText(""),
                position.path("lat").asDouble(0.0),
                position.path("lon").asDouble(0.0),
                json.path("z_m").asDouble(0.0),
                json.path("event_type").asText(""),
                json.path("observer_id").asText(""),
                json.path("temp_external_c").asDouble(0.0),
                json.path("temp_internal_c").asDouble(0.0),
                json.path("text").asText(""));
    }
}
