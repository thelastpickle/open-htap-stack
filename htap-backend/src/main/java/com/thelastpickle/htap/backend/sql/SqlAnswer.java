package com.thelastpickle.htap.backend.sql;

import java.util.List;

/**
 * What cassandra-sql answered one statement with.
 *
 * <p>Every value is text, and that is the service's doing rather than this class's: the server
 * sends no type identifier worth reading, so a column holding an {@code INT} arrives as "75", and
 * as "75.0" after an {@code UPDATE} that added to it. Presenting these as numbers here would
 * invent a type the server did not send.
 *
 * @param rows empty for a statement that returns nothing, which is not an error: an {@code INSERT}
 *     succeeding is the result
 */
record SqlAnswer(List<String> columns, List<List<String>> rows, double durationMs) {

    static final SqlAnswer NOTHING = new SqlAnswer(List.of(), List.of(), 0.0);

    SqlAnswer withDuration(double millis) {
        return new SqlAnswer(columns, rows, millis);
    }
}
