package com.thelastpickle.htap.backend.cdc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.thelastpickle.htap.backend.api.dto.CdcRecord;
import com.thelastpickle.htap.backend.config.CdcSettings;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

/** One published mutation, read back through the framing and the schema the publisher used. */
class CdcDecoderTest {

    /** The mutation's own write time, and the broker's append eight seconds later. */
    private static final long WROTE_AT_MICROS = 1787846133_000000L;
    private static final long APPENDED_AT_MS = 1787846141_000L;

    private final CdcSettings settings = CdcFixtures.settings();
    private final CdcDecoder decoder = new CdcDecoder(CdcFixtures.registry(settings));

    /** The envelope's own fields, and the row flattened out of {@code payload}. */
    @Test
    void aMutationCarriesItsColumnsAndWhatWroteThem() {
        CdcRecord record = decode(mutation());

        assertEquals("demo", record.keyspace());
        assertEquals("drone_latest_status", record.table());
        assertEquals("UPDATE", record.operation());
        assertEquals("demo:drone_latest_status:0007", record.key());
        assertEquals(Integer.valueOf(CdcFixtures.SCHEMA_ID), record.schemaId());
        assertNull(record.decodeError());
        assertFalse(record.partial());
        assertEquals(1787846133_000L, record.mutationAtMs());
        assertEquals("drone-0007", record.columns().get("entity_id"));
        assertEquals(12.5, record.columns().get("speed_mps"));
        assertEquals(true, record.columns().get("is_flying"));
    }

    /**
     * A column's logical type is applied, so the page shows a time and not a count of microseconds.
     *
     * <p>{@code event_time} is declared {@code timestamp-micros} and {@code event_id} {@code uuid};
     * the publisher writes each as the plain type beneath, and reading them back as the logical one is
     * what the decoder's data model is for.
     */
    @Test
    void aLogicalTypeIsReadAsWhatItDeclares() {
        Map<String, Object> columns = decode(mutation()).columns();

        assertEquals("2026-08-27T15:55:33+00:00", columns.get("event_time"));
        assertEquals("8f14e45f-ceea-467a-9f38-0e5e2a5b0000", columns.get("event_id"));
    }

    /** A column the mutation did not write is null, and {@code updateFields} says which it wrote. */
    @Test
    void theUpdatedColumnsAreNamedApartFromTheNullOnes() {
        CdcRecord record = decode(mutation());

        assertEquals(
                List.of("entity_id", "event_id", "event_time", "speed_mps", "is_flying", "text_payload"),
                record.updateFields());
        assertNull(record.columns().get("altitude_m"));
        assertEquals(19, record.columns().size(), "every column of the table is a field of the record");
    }

    /**
     * The age is the publisher's delay: Kafka's append time less the mutation's own write time.
     *
     * <p>Both figures ride on the record, so it stays true while this backend's consumer is behind.
     */
    @Test
    void theAgeIsTheDelayBetweenTheWriteAndTheAppend() {
        assertEquals(8000.0, decode(mutation()).ageMs().doubleValue());
    }

    /** A backfilled record has no age: its own would measure the backlog, not the publisher. */
    @Test
    void aBackfilledRecordReportsNoAge() {
        CdcRecord record = decoder.decode(
                7,
                CdcFixtures.arrival(1, 4200, APPENDED_AT_MS, CdcFixtures.framed(1, mutation())),
                true);

        assertNull(record.ageMs());
        assertTrue(record.backfill());
        assertEquals(7, record.seq());
    }

    /** A value that is not Confluent-framed is a record with a reason, not a dropped record. */
    @Test
    void anUnframedValueIsKeptWithItsReason() {
        CdcRecord record =
                decoder.decode(1, CdcFixtures.arrival(0, 1, APPENDED_AT_MS, new byte[] {9, 9}), false);

        assertEquals(
                "IllegalArgumentException: not Confluent-framed: 2 bytes beginning 09",
                record.decodeError());
        assertNull(record.schemaId());
        // The key needs no schema, so an unreadable record still says what it touched.
        assertEquals("demo", record.keyspace());
        assertEquals("drone_latest_status", record.table());
    }

    /** A schema id the registry does not hold is reported against the record, id included. */
    @Test
    void anUnknownSchemaIdIsReportedWithTheIdItNamed() {
        CdcRecord record = decode(mutation(), 9);

        assertEquals(Integer.valueOf(9), record.schemaId());
        assertTrue(record.decodeError().startsWith("IOException: the registry answered HTTP 404"),
                record.decodeError());
        assertEquals(Map.of(), record.columns());
    }

    /** An empty value says so rather than reporting a byte it did not read. */
    @Test
    void anEmptyValueNamesNoFirstByte() {
        CdcRecord record = decoder.decode(1, CdcFixtures.arrival(0, 1, 0, new byte[0]), false);

        assertEquals(
                "IllegalArgumentException: not Confluent-framed: 0 bytes beginning nothing",
                record.decodeError());
    }

    /** The four-byte id is big-endian, which one above 127 is what proves. */
    @Test
    void theSchemaIdIsBigEndian() {
        assertEquals(1, CdcDecoder.schemaId(new byte[] {0, 0, 0, 0, 1}));
        assertEquals(300, CdcDecoder.schemaId(new byte[] {0, 0, 0, 1, 44}));
    }

    private static GenericRecord mutation() {
        return CdcFixtures.mutation("UPDATE", WROTE_AT_MICROS, CdcFixtures.telemetry());
    }

    private CdcRecord decode(GenericRecord record) {
        return decode(record, CdcFixtures.SCHEMA_ID);
    }

    private CdcRecord decode(GenericRecord record, int schemaId) {
        return decoder.decode(
                1,
                CdcFixtures.arrival(1, 4200, APPENDED_AT_MS, CdcFixtures.framed(schemaId, record)),
                false);
    }
}
