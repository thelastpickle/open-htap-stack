package com.thelastpickle.htap.producer;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.UUID;

/**
 * One event as the sink reads it.
 *
 * <p>The field names are the contract between this process and the sink, and they are the Python's
 * exactly: {@code position} is a nested object where everything else is flat, and {@code z_m} and
 * {@code text} are spelled as they are rather than as the columns they end up in. Renaming any of
 * them means changing the sink in the same commit.
 *
 * <p>Written with a streaming generator over a reused buffer, so one event costs no map and no
 * intermediate string. What that buys is measured in {@code ProducerThroughputTest}.
 */
final class EventJson {

    private static final JsonFactory JSON = new JsonFactory();

    private final ByteArrayOutputStream buffer = new ByteArrayOutputStream(1024);

    /**
     * The event's bytes.
     *
     * <p>Not thread-safe, deliberately: the send loop is one thread, and a shared buffer across
     * threads would be a lock in the hottest place in this process.
     */
    byte[] bytes(
            UUID eventId,
            String entityId,
            String observerId,
            String eventType,
            Telemetry telemetry) {
        buffer.reset();
        try (JsonGenerator json = JSON.createGenerator(buffer)) {
            json.writeStartObject();
            json.writeStringField("event_id", eventId.toString());
            json.writeStringField("entity_id", entityId);
            json.writeStringField("observer_id", observerId);
            json.writeStringField("event_type", eventType);
            json.writeObjectFieldStart("position");
            json.writeNumberField("lat", telemetry.lat());
            json.writeNumberField("lon", telemetry.lon());
            json.writeEndObject();
            json.writeNumberField("z_m", telemetry.altitudeM());
            json.writeNumberField("temp_external_c", telemetry.tempExternalC());
            json.writeNumberField("temp_internal_c", telemetry.tempInternalC());
            json.writeStringField("text", telemetry.text());
            json.writeEndObject();
        } catch (IOException e) {
            // A generator over a byte array does not fail for an I/O reason, so reaching here is
            // a bug in this class rather than a condition a caller could handle.
            throw new UncheckedIOException(e);
        }
        return buffer.toByteArray();
    }
}
