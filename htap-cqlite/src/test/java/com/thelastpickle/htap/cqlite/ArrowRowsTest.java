package com.thelastpickle.htap.cqlite;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

/**
 * The conversion of one batch, over a root built here rather than over the library.
 *
 * <p>Nine columns, covering each type whose {@code getObject} is not already what the
 * other four access paths spell, and two rows so that a null is one row rather than an
 * absent column.
 */
class ArrowRowsTest {

    private static final Schema SCHEMA = new Schema(List.of(
            Field.nullable("text", new ArrowType.Utf8()),
            Field.nullable("flag", new ArrowType.Bool()),
            Field.nullable("small", new ArrowType.Int(32, true)),
            Field.nullable("big", new ArrowType.Int(64, true)),
            Field.nullable("ratio", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
            Field.nullable("blob", new ArrowType.Binary()),
            Field.nullable("day", new ArrowType.Date(DateUnit.DAY)),
            Field.nullable("at", new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)),
            Field.nullable("clock", new ArrowType.Time(TimeUnit.NANOSECOND, 64))));

    private static final LocalDate DAY = LocalDate.of(2026, 8, 28);
    private static final LocalDateTime AT = LocalDateTime.of(2026, 8, 28, 12, 34, 56, 789_000_000);
    private static final LocalTime CLOCK = LocalTime.of(1, 2, 3, 456_789_012);

    @Test
    void everyDeclaredTypeCrossesAsTheOtherPathsSpellIt() {
        withRoot(root -> {
            List<Map<String, Object>> rows = ArrowRows.of(root);
            assertEquals(2, rows.size());

            Map<String, Object> first = rows.getFirst();
            assertEquals(
                    List.of("text", "flag", "small", "big", "ratio", "blob", "day", "at", "clock"),
                    List.copyOf(first.keySet()),
                    "the projected column order");
            assertEquals("alpha", first.get("text"));
            assertEquals(Boolean.TRUE, first.get("flag"));
            assertEquals(7, first.get("small"));
            assertEquals(9_000_000_000L, first.get("big"));
            assertEquals(0.5d, first.get("ratio"));
            assertEquals("0x000fff", first.get("blob"));
            assertEquals("2026-08-28", first.get("day"));
            assertEquals("2026-08-28T12:34:56.789000", first.get("at"));
            assertEquals("01:02:03.456789012", first.get("clock"));
        });
    }

    @Test
    void aNullIsANullAndNotAZero() {
        withRoot(root -> {
            Map<String, Object> second = ArrowRows.of(root).get(1);
            assertEquals(SCHEMA.getFields().size(), second.size(), "every column is present");
            second.forEach((column, value) -> assertNull(value, column));
        });
    }

    /**
     * A whole second carries no fraction, which is the spelling the other paths use;
     * {@code LocalDateTime.toString()} would give three digits for the row above and none
     * here.
     */
    @Test
    void aWholeSecondTimestampCarriesNoFraction() {
        Schema schema = new Schema(
                List.of(Field.nullable("at", new ArrowType.Timestamp(TimeUnit.MILLISECOND, null))));
        try (RootAllocator allocator = new RootAllocator();
                VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            TimeStampMilliVector at = (TimeStampMilliVector) root.getVector("at");
            at.allocateNew(1);
            at.set(0, LocalDateTime.of(2026, 3, 28, 12, 34, 56).toInstant(ZoneOffset.UTC).toEpochMilli());
            root.setRowCount(1);
            assertEquals("2026-03-28T12:34:56", ArrowRows.of(root).getFirst().get("at"));
        }
    }

    /** Two rows: the first a value of every type, the second a null of every type. */
    private static void withRoot(Consumer<VectorSchemaRoot> assertions) {
        try (RootAllocator allocator = new RootAllocator();
                VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator)) {
            for (FieldVector vector : root.getFieldVectors()) {
                vector.allocateNew();
            }
            ((VarCharVector) root.getVector("text")).setSafe(0, "alpha".getBytes(UTF_8));
            ((BitVector) root.getVector("flag")).setSafe(0, 1);
            ((IntVector) root.getVector("small")).setSafe(0, 7);
            ((BigIntVector) root.getVector("big")).setSafe(0, 9_000_000_000L);
            ((Float8Vector) root.getVector("ratio")).setSafe(0, 0.5d);
            ((VarBinaryVector) root.getVector("blob"))
                    .setSafe(0, new byte[] {0x00, 0x0f, (byte) 0xff});
            ((DateDayVector) root.getVector("day")).setSafe(0, (int) DAY.toEpochDay());
            ((TimeStampMilliVector) root.getVector("at"))
                    .setSafe(0, AT.toInstant(ZoneOffset.UTC).toEpochMilli());
            ((TimeNanoVector) root.getVector("clock")).setSafe(0, CLOCK.toNanoOfDay());
            for (FieldVector vector : root.getFieldVectors()) {
                setNull(vector, 1);
            }
            root.setRowCount(2);
            assertions.accept(root);
        }
    }

    /** Arrow declares setNull on the two base classes rather than on the interface. */
    private static void setNull(FieldVector vector, int row) {
        switch (vector) {
            case BaseFixedWidthVector fixed -> fixed.setNull(row);
            case BaseVariableWidthVector variable -> variable.setNull(row);
            default -> throw new AssertionError("no setNull for " + vector.getClass());
        }
    }
}
