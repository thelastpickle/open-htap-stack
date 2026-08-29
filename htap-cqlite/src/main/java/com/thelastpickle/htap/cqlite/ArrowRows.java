package com.thelastpickle.htap.cqlite;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * One Arrow batch as rows a JSON encoder can take.
 *
 * <p>Converted by Arrow type rather than by {@code getObject} alone, because five of the
 * twelve declared column types return something a JSON encoder would either refuse or
 * spell differently from the other four access paths: a {@code Text} rather than a
 * string, a {@code byte[]} rather than a blob's hex, a day count rather than a date, and
 * a nanosecond or millisecond count rather than a time. The paths are compared row for
 * row, so a difference in spelling reads as a difference in the data.
 *
 * <p>Two spellings here are deliberately not the Python's, and the Java CQL path has to
 * take the same two for a comparison to stay honest. A blob becomes {@code 0x} hex, where
 * the Python passed the raw bytes on; and a {@code time} keeps its nanoseconds, where
 * Python's {@code datetime.time} truncated to microseconds. No table this demo registers
 * has a blob or a {@code time} column, so neither is reachable today; the note is here
 * because the first such column is where a comparison would report a difference that is
 * this file's doing rather than the data's.
 */
final class ArrowRows {

    /**
     * Seconds precision, with the fraction appended separately.
     *
     * <p>Not {@link LocalDateTime#toString()}, which gives three fractional digits for a
     * millisecond timestamp and none for a whole second. Python's {@code isoformat} gives
     * six digits or none, and the other paths' timestamps are that spelling.
     */
    private static final DateTimeFormatter TO_SECONDS =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

    private ArrowRows() {
    }

    /** The batch the root currently holds, one map per row with the columns in order. */
    static List<Map<String, Object>> of(VectorSchemaRoot root) {
        List<FieldVector> vectors = root.getFieldVectors();
        int rowCount = root.getRowCount();
        List<Map<String, Object>> rows = new ArrayList<>(rowCount);
        for (int row = 0; row < rowCount; row++) {
            Map<String, Object> values = LinkedHashMap.newLinkedHashMap(vectors.size());
            for (FieldVector vector : vectors) {
                values.put(vector.getName(), value(vector, row));
            }
            rows.add(values);
        }
        return rows;
    }

    static Object value(FieldVector vector, int row) {
        if (vector.isNull(row)) {
            return null;
        }
        return switch (vector.getMinorType()) {
            case VARCHAR -> ((VarCharVector) vector).getObject(row).toString();
            // Cassandra's own blob spelling, so a value can be pasted back into CQL; the
            // Python handed the bytes to the JSON encoder instead.
            case VARBINARY -> "0x" + HexFormat.of().formatHex(((VarBinaryVector) vector).get(row));
            // get() is a count of days since the epoch, not a date.
            case DATEDAY -> LocalDate.ofEpochDay(((DateDayVector) vector).get(row)).toString();
            case TIMESTAMPMILLI -> timestamp(((TimeStampMilliVector) vector).get(row));
            // Nanoseconds, where Python truncated to microseconds; Cassandra's `time` is
            // nanosecond-precision, so this is the more faithful of the two.
            case TIMENANO -> LocalTime.ofNanoOfDay(((TimeNanoVector) vector).get(row)).toString();
            // Boolean, the four integer widths and the two float widths each box to a
            // Java type already; a wider statement output is stringified rather than
            // risking a JSON encoder failure at the route.
            default -> plain(vector.getObject(row));
        };
    }

    private static Object plain(Object value) {
        return switch (value) {
            case null -> null;
            case Boolean b -> b;
            case Number n -> n;
            case String s -> s;
            default -> value.toString();
        };
    }

    private static String timestamp(long epochMillis) {
        LocalDateTime at = LocalDateTime.ofEpochSecond(
                Math.floorDiv(epochMillis, 1000L),
                (int) Math.floorMod(epochMillis, 1000L) * 1_000_000,
                ZoneOffset.UTC);
        String text = at.format(TO_SECONDS);
        int micros = at.getNano() / 1000;
        return micros == 0 ? text : text + "." + "%06d".formatted(micros);
    }
}
