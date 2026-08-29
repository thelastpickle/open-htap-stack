package com.thelastpickle.htap.backend.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * What a transaction's {@code SELECT} projection becomes on the way to a page.
 *
 * <p>Over a proxy row rather than a node: the only questions worth asking here are how a projected
 * column is named and how its value is spelled, and both are decided in this class.
 */
class AccordTest {

    /**
     * A projected column is named {@code session_ok.session_id}, dot and all.
     *
     * <p>Read by position for exactly that reason. A dotted name is not an identifier the driver's own
     * rules spell that way, so asking a row for it by name finds nothing, and every guard the demo
     * reads back would come out null: each refusal would then report "the session is not open".
     */
    @Test
    void aDottedGuardNameSurvives() {
        UUID session = UUID.fromString("6bd0c9f4-1c9a-4a2e-8f5c-6d1f2a3b4c5d");

        Map<String, Object> projection = Accord.projection(
                row(List.of("session_ok.session_id", "already.seq"), Arrays.asList(session, null)));

        Map<String, Object> expected = new LinkedHashMap<>();
        expected.put("session_ok.session_id", session.toString());
        expected.put("already.seq", null);
        assertEquals(expected, projection);
    }

    /** The projection's own order, so a reader sees the guards in the order the statement lists them. */
    @Test
    void theOrderIsTheProjectionsOwn() {
        Map<String, Object> projection = Accord.projection(
                row(List.of("occ.remaining", "occ.capacity", "held.zone_id"),
                        Arrays.asList(1L, 2L, null)));

        assertEquals(
                List.of("occ.remaining", "occ.capacity", "held.zone_id"),
                List.copyOf(projection.keySet()));
    }

    /**
     * A count stays a number, because a refusal is decided by comparing it.
     *
     * <p>The clearance guard asks whether {@code occ.remaining} is greater than zero. Were the value
     * stringified here, that comparison would be against the text "0", and a full zone would report
     * itself as having room.
     */
    @Test
    void aCountArrivesAsANumber() {
        assertEquals(0L, Accord.projection(row(List.of("occ.remaining"), List.of(0L)))
                .get("occ.remaining"));
    }

    /** Every value takes the spelling the five paths share, so a timestamp reads as the Python's. */
    @Test
    void aTimestampTakesTheSharedSpelling() {
        assertEquals(
                "2026-08-29T12:34:56.789000",
                Accord.projection(
                                row(List.of("granted_at"),
                                        List.of(Instant.parse("2026-08-29T12:34:56.789Z"))))
                        .get("granted_at"));
    }

    private static Row row(List<String> names, List<?> values) {
        List<ColumnDefinition> definitions = new ArrayList<>();
        for (String name : names) {
            definitions.add((ColumnDefinition) Proxy.newProxyInstance(
                    AccordTest.class.getClassLoader(),
                    new Class<?>[] {ColumnDefinition.class},
                    (proxy, method, args) -> switch (method.getName()) {
                        case "getName" -> CqlIdentifier.fromInternal(name);
                        default -> throw new UnsupportedOperationException(method.getName());
                    }));
        }
        ColumnDefinitions columns = (ColumnDefinitions) Proxy.newProxyInstance(
                AccordTest.class.getClassLoader(),
                new Class<?>[] {ColumnDefinitions.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "size" -> definitions.size();
                    case "get" -> definitions.get((Integer) args[0]);
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        return (Row) Proxy.newProxyInstance(
                AccordTest.class.getClassLoader(),
                new Class<?>[] {Row.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getColumnDefinitions" -> columns;
                    case "getObject" -> values.get((Integer) args[0]);
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }
}
