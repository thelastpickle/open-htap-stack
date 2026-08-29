package com.thelastpickle.htap.backend.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * A result set drained into the shape every path answers in.
 *
 * <p>Over a proxy rather than a driver, because the three methods this reads are the whole of what
 * it needs from JDBC and a Presto or Thrift Server connection would add nothing to the question.
 */
class JdbcRowsTest {

    @Test
    void theColumnsAndTheValuesArriveInOrder() throws SQLException {
        QueryRows rows = JdbcRows.read(
                Jdbc.resultSet(
                        List.of("event_type", "events"),
                        List.of(List.of("takeoff", 4L), List.of("landing", 2L))),
                false);

        assertEquals(List.of("event_type", "events"), rows.columns());
        assertEquals(List.of(List.of("takeoff", 4L), List.of("landing", 2L)), rows.rows());
        assertEquals(2, rows.rowCount());
    }

    /**
     * HiveServer2 qualifies a projected column as {@code view.column} where Presto does not, and
     * the compare page lines the five paths up by column name.
     */
    @Test
    void theQualifierHiveServer2AddsIsDroppedWhenAsked() {
        assertEquals(
                List.of("event_type", "events"),
                read(List.of("bulk_events.event_type", "events"), true).columns());
        assertEquals(
                List.of("bulk_events.event_type", "events"),
                read(List.of("bulk_events.event_type", "events"), false).columns());
    }

    /** An answer of no rows still names its columns, which the Python's could not. */
    @Test
    void anEmptyAnswerStillNamesItsColumns() {
        QueryRows rows = read(List.of("event_id"), List.of(), false);

        assertEquals(List.of("event_id"), rows.columns());
        assertEquals(List.of(), rows.rows());
        assertEquals(ReadFigures.NONE, rows.figures());
    }

    /** Each value passes through the shared spelling, so a JDBC timestamp arrives as the CQL one. */
    @Test
    void everyValueIsSpelledAsTheOtherPathsSpellIt() {
        QueryRows rows = read(
                List.of("at", "id"),
                List.of(Arrays.asList(
                        java.sql.Timestamp.valueOf("2026-08-29 12:34:56.789"), null)),
                false);

        assertEquals(List.of(Arrays.asList("2026-08-29T12:34:56.789000", null)), rows.rows());
    }

    /** A failure reading the set is the caller's to report, so it is not swallowed here. */
    @Test
    void aFailureMidReadIsRaised() {
        ResultSet failing = (ResultSet) Proxy.newProxyInstance(
                JdbcRowsTest.class.getClassLoader(),
                new Class<?>[] {ResultSet.class},
                (proxy, method, args) -> {
                    throw new SQLException("connection reset");
                });

        assertEquals("connection reset", assertThrows(
                        SQLException.class, () -> JdbcRows.read(failing, false))
                .getMessage());
    }

    private static QueryRows read(List<String> labels, boolean stripQualifier) {
        return read(labels, List.of(List.of("takeoff", 4L)), stripQualifier);
    }

    private static QueryRows read(
            List<String> labels, List<? extends List<?>> values, boolean stripQualifier) {
        try {
            return JdbcRows.read(Jdbc.resultSet(labels, values), stripQualifier);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }
}
