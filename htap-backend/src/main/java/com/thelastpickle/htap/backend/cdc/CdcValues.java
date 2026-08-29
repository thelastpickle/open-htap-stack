package com.thelastpickle.htap.backend.cdc;

import com.thelastpickle.htap.common.Timestamps;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

/**
 * One decoded Avro value, spelled so a browser can hold it.
 *
 * <p>Avro carries bytes, and Cassandra's {@code blob} and {@code inet} columns arrive as bytes here.
 * Base64 rather than a hex string or a lossy decode, so what the page shows is reversible and it is
 * obvious that it is not text; the Streaming page renders a {@code {base64: …}} object as such.
 * That differs from {@link com.thelastpickle.htap.backend.support.Cells}, which spells a blob
 * {@code 0x…} because the five paths are compared against {@code cqlsh}, and the difference is
 * deliberate: these two spellings answer to different readers.
 */
final class CdcValues {

    private CdcValues() {}

    /** The value as the Streaming page reads it. */
    static Object jsonSafe(Object value) {
        return switch (value) {
            case null -> null;
            case String text -> text;
            case Utf8 text -> text.toString();
            // Before Number, which it is one of: a decimal goes as text, because JSON has no exact
            // decimal and the value came from a column that does.
            case BigDecimal decimal -> decimal.toString();
            case Number number -> number;
            case Boolean flag -> flag;
            // The offset is Python's: fastavro converted a timestamp-millis to an aware UTC
            // datetime, and the page was written against that spelling.
            case Instant at -> Timestamps.isoOffset(at);
            case LocalDate date -> date.toString();
            case LocalTime time -> time.toString();
            case UUID uuid -> uuid.toString();
            case byte[] bytes -> base64(bytes);
            case ByteBuffer buffer -> base64(remaining(buffer));
            case GenericFixed fixed -> base64(fixed.bytes());
            case GenericRecord nested -> fields(nested);
            case Map<?, ?> map -> mapped(map);
            case Collection<?> items -> items.stream().map(CdcValues::jsonSafe).toList();
            default -> value.toString();
        };
    }

    /** A nested record's own fields, which is what the Python's decoder handed back as a dict. */
    static Map<String, Object> fields(GenericRecord record) {
        Map<String, Object> named = new LinkedHashMap<>();
        record.getSchema().getFields()
                .forEach(field -> named.put(field.name(), jsonSafe(record.get(field.pos()))));
        return named;
    }

    /** The names of a record's fields, in declaration order. */
    static List<String> names(GenericRecord record) {
        List<String> names = new ArrayList<>();
        record.getSchema().getFields().forEach(field -> names.add(field.name()));
        return names;
    }

    private static Map<String, Object> mapped(Map<?, ?> map) {
        Map<String, Object> named = new LinkedHashMap<>();
        map.forEach((key, value) -> named.put(String.valueOf(key), jsonSafe(value)));
        return named;
    }

    private static Map<String, Object> base64(byte[] bytes) {
        return Map.of("base64", Base64.getEncoder().encodeToString(bytes));
    }

    private static byte[] remaining(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        // A duplicate, so reading the bytes does not consume the buffer the decoder holds.
        buffer.duplicate().get(bytes);
        return bytes;
    }
}
