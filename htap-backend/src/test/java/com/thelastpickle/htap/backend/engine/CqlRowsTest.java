package com.thelastpickle.htap.backend.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A CQL result set drained into the shape every path answers in.
 *
 * <p>Over proxies rather than a cluster: the three methods this reads are the whole of what it
 * needs from the driver, and the two questions worth asking are how a column is named and how a
 * value is spelled.
 */
class CqlRowsTest {

    @Test
    void theColumnsAndTheValuesArriveInOrder() {
        QueryRows rows = CqlRows.read(
                resultSet(
                        List.of("event_type", "events"),
                        List.of(List.of("takeoff", 4L), List.of("landing", 2L))));

        assertEquals(List.of("event_type", "events"), rows.columns());
        assertEquals(List.of(List.of("takeoff", 4L), List.of("landing", 2L)), rows.rows());
        assertEquals(2, rows.rowCount());
    }

    /**
     * The internal name, so a column CQL considers case-sensitive is not reported in the quotes
     * that spell it in a statement: the compare page lines the five paths up by this name, and only
     * this path would carry the quotes.
     */
    @Test
    void aCaseSensitiveColumnIsNamedWithoutTheQuotesCqlWouldNeed() {
        assertEquals(
                List.of("Event Type"),
                CqlRows.read(resultSet(List.of("Event Type"), List.of(List.of("takeoff"))))
                        .columns());
    }

    /** Every value passes through the shared spelling, so an Instant arrives as the Python's text. */
    @Test
    void everyValueIsSpelledAsTheOtherPathsSpellIt() {
        UUID id = UUID.fromString("6bd0c9f4-1c9a-4a2e-8f5c-6d1f2a3b4c5d");
        QueryRows rows = CqlRows.read(
                resultSet(
                        List.of("at", "id", "missing"),
                        List.of(Arrays.asList(
                                Instant.parse("2026-08-29T12:34:56.789Z"), id, null))));

        assertEquals(
                List.of(Arrays.asList("2026-08-29T12:34:56.789000", id.toString(), null)),
                rows.rows());
    }

    /** An answer of no rows still names its columns, which the Python's could not. */
    @Test
    void anEmptyAnswerStillNamesItsColumns() {
        QueryRows rows = CqlRows.read(resultSet(List.of("event_id"), List.of()));

        assertEquals(List.of("event_id"), rows.columns());
        assertEquals(List.of(), rows.rows());
        assertEquals(ReadFigures.NONE, rows.figures());
    }

    private static ResultSet resultSet(List<String> names, List<? extends List<?>> values) {
        List<ColumnDefinition> definitions = new ArrayList<>();
        for (String name : names) {
            definitions.add((ColumnDefinition) Proxy.newProxyInstance(
                    CqlRowsTest.class.getClassLoader(),
                    new Class<?>[] {ColumnDefinition.class},
                    (proxy, method, args) -> switch (method.getName()) {
                        case "getName" -> CqlIdentifier.fromInternal(name);
                        default -> throw new UnsupportedOperationException(method.getName());
                    }));
        }
        ColumnDefinitions columns = (ColumnDefinitions) Proxy.newProxyInstance(
                CqlRowsTest.class.getClassLoader(),
                new Class<?>[] {ColumnDefinitions.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "size" -> definitions.size();
                    case "iterator" -> definitions.iterator();
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        List<Row> rows = new ArrayList<>();
        for (List<?> value : values) {
            rows.add((Row) Proxy.newProxyInstance(
                    CqlRowsTest.class.getClassLoader(),
                    new Class<?>[] {Row.class},
                    (proxy, method, args) -> switch (method.getName()) {
                        case "getObject" -> value.get((Integer) args[0]);
                        default -> throw new UnsupportedOperationException(method.getName());
                    }));
        }
        return (ResultSet) Proxy.newProxyInstance(
                CqlRowsTest.class.getClassLoader(),
                new Class<?>[] {ResultSet.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getColumnDefinitions" -> columns;
                    case "iterator" -> rows.iterator();
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }
}
