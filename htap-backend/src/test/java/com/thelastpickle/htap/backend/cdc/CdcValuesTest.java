package com.thelastpickle.htap.backend.cdc;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.junit.jupiter.api.Test;

/** How a decoded Avro value is spelled for the page, which is not how the compare paths spell one. */
class CdcValuesTest {

    private static final Schema NESTED = new Schema.Parser().parse("""
            {"type": "record", "name": "Ttl", "fields": [
              {"name": "ttl", "type": "int"},
              {"name": "deletedAt", "type": "long"}]}""");

    /** Avro's own string type, which is not {@link String} and would otherwise reach JSON as one. */
    @Test
    void avrosStringBecomesAString() {
        assertEquals("holding at waypoint 4", CdcValues.jsonSafe(new Utf8("holding at waypoint 4")));
    }

    /**
     * Bytes go as base64 under a key that names the encoding.
     *
     * <p>Reversible, and obviously not text; the page renders the object as {@code … (base64)}. The
     * compare paths spell a blob {@code 0x…} instead, because they are checked against {@code cqlsh}.
     */
    @Test
    void bytesGoAsBase64UnderAKeyThatSaysSo() {
        assertEquals(Map.of("base64", "AQIDBA=="), CdcValues.jsonSafe(new byte[] {1, 2, 3, 4}));
    }

    /** Reading a buffer does not consume it, so a second reader sees the same bytes. */
    @Test
    void aBufferIsReadWithoutBeingConsumed() {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2, 3, 4});

        assertEquals(Map.of("base64", "AQIDBA=="), CdcValues.jsonSafe(buffer));
        assertEquals(4, buffer.remaining(), "the decoder's own buffer was drained");
    }

    /** The offset is Python's, whose decoder handed the route an aware UTC datetime. */
    @Test
    void anInstantCarriesTheOffsetThePageWasWrittenFor() {
        assertEquals(
                "2026-08-27T15:55:33+00:00", CdcValues.jsonSafe(Instant.ofEpochSecond(1787846133)));
        assertEquals("2026-08-27", CdcValues.jsonSafe(LocalDate.of(2026, 8, 27)));
    }

    /** A decimal goes as text: JSON has no exact decimal and the column it came from does. */
    @Test
    void aDecimalGoesAsText() {
        assertEquals("1.250", CdcValues.jsonSafe(new BigDecimal("1.250")));
        assertEquals(
                "8f14e45f-ceea-467a-9f38-0e5e2a5b0000",
                CdcValues.jsonSafe(UUID.fromString("8f14e45f-ceea-467a-9f38-0e5e2a5b0000")));
    }

    /** A number is left as a number, since the page formats it. */
    @Test
    void aNumberIsLeftAlone() {
        assertEquals(12.5, CdcValues.jsonSafe(12.5));
        assertEquals(7, CdcValues.jsonSafe(7));
        assertEquals(true, CdcValues.jsonSafe(Boolean.TRUE));
        assertEquals(null, CdcValues.jsonSafe(null));
    }

    /** A nested record becomes an object keyed by its field names, in declaration order. */
    @Test
    void aNestedRecordBecomesAnObject() {
        GenericRecord ttl = new GenericData.Record(NESTED);
        ttl.put("ttl", 60);
        ttl.put("deletedAt", 1787846193L);

        Map<String, Object> expected = new LinkedHashMap<>();
        expected.put("ttl", 60);
        expected.put("deletedAt", 1787846193L);
        assertEquals(expected, CdcValues.jsonSafe(ttl));
        assertEquals(List.of("ttl", "deletedAt"), List.copyOf(CdcValues.fields(ttl).keySet()));
    }

    /** An array becomes a list and a map's keys become strings, both element by element. */
    @Test
    void collectionsAreConvertedThroughout() {
        assertEquals(
                List.of("a", Map.of("base64", "AQ==")),
                CdcValues.jsonSafe(List.of(new Utf8("a"), new byte[] {1})));
        assertEquals(
                Map.of("speed_mps", "12.5"),
                CdcValues.jsonSafe(Map.of(new Utf8("speed_mps"), new Utf8("12.5"))));
    }

    /** An enum symbol, and anything else Avro invents, goes as the text it prints. */
    @Test
    void anythingElseGoesAsItsOwnText() {
        Schema operation = new Schema.Parser().parse(
                "{\"type\": \"enum\", \"name\": \"OperationType\", \"symbols\": [\"INSERT\"]}");

        assertEquals("INSERT", CdcValues.jsonSafe(new GenericData.EnumSymbol(operation, "INSERT")));
    }
}
