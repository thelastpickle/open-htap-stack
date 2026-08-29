package com.thelastpickle.htap.producer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The wire format between this process and the sink.
 *
 * <p>The field names are the contract, and they are asserted literally rather than through a parser:
 * a test that read the JSON back with the same library would pass on a renamed field, and the sink
 * would then drop every record.
 */
class EventJsonTest {

    private static final UUID EVENT_ID = UUID.fromString("2c7b5b5a-a384-11f1-b1e8-0bd392adf78a");

    private final EventJson json = new EventJson();

    @Test
    void theFieldsAreTheOnesTheSinkReads() {
        String event = event(new Telemetry(7, 59.9139, 10.7522, 103.5, -13.2, 2.1, "holding"));

        assertEquals(
                "{\"event_id\":\"2c7b5b5a-a384-11f1-b1e8-0bd392adf78a\","
                        + "\"entity_id\":\"asset-000007\","
                        + "\"observer_id\":\"observer-0000\","
                        + "\"event_type\":\"telemetry_update\","
                        + "\"position\":{\"lat\":59.9139,\"lon\":10.7522},"
                        + "\"z_m\":103.5,"
                        + "\"temp_external_c\":-13.2,"
                        + "\"temp_internal_c\":2.1,"
                        + "\"text\":\"holding\"}",
                event);
    }

    /**
     * The position is nested where every other field is flat.
     *
     * <p>The sink reads {@code position.lat}, so flattening it here would leave the latitude null in
     * every row and the map empty.
     */
    @Test
    void thePositionIsNested() {
        assertTrue(event(telemetry("")).contains("\"position\":{\"lat\":"));
    }

    /**
     * A snippet carrying quotes, backslashes and newlines survives as one string.
     *
     * <p>This is what the corpus holds, and it is the reason the event is written by a generator
     * rather than assembled: a hand-written escape missing one case would produce a record the sink
     * cannot parse, which shows up as a gap in a rate chart rather than as an error.
     */
    @Test
    void theTextIsEscaped() {
        String event = event(telemetry("she said \"stop\"\nthen \\left"));

        assertTrue(event.endsWith("\"text\":\"she said \\\"stop\\\"\\nthen \\\\left\"}"), event);
    }

    /** Non-ASCII goes as UTF-8 rather than as an escape, which is what the corpus contains. */
    @Test
    void nonAsciiGoesAsUtf8() {
        byte[] bytes = json.bytes(
                EVENT_ID, "asset-000007", "observer-0000", "telemetry_update", telemetry("Grünerløkka"));

        assertTrue(new String(bytes, StandardCharsets.UTF_8).contains("Grünerløkka"));
    }

    /** One buffer, reused: a second event must not carry the first one's bytes. */
    @Test
    void theBufferIsClearedBetweenEvents() {
        json.bytes(EVENT_ID, "asset-000000", "observer-0000", "data_sync", telemetry("first"));
        String second = event(telemetry("second"));

        assertEquals(1, second.split("event_id", -1).length - 1, "the buffer held the last event");
        assertTrue(second.contains("second") && !second.contains("first"));
    }

    private String event(Telemetry telemetry) {
        return new String(
                json.bytes(
                        EVENT_ID, "asset-000007", "observer-0000", "telemetry_update", telemetry),
                StandardCharsets.UTF_8);
    }

    private static Telemetry telemetry(String text) {
        return new Telemetry(7, 59.9139, 10.7522, 103.5, -13.2, 2.1, text);
    }
}
