package com.thelastpickle.htap.backend.engine;

import java.util.List;

/**
 * One statement's answer: the column names, the values in that order, and what the read cost.
 *
 * <p>Values by position rather than by name, which is the shape the pages read and one list
 * per row rather than one map. The columns come from the result set's own metadata, so an
 * answer of no rows still names its columns where the Python's did not: it took the names from
 * the first row and had none to take them from.
 *
 * <p>The figures travel with the rows because they belong to this read. The Python held them
 * on the client in thread-local fields and the caller asked for them afterwards, which is
 * correct only while one thread runs one statement per client; the comparison runs the five
 * paths at once on purpose.
 */
public record QueryRows(List<String> columns, List<List<Object>> rows, ReadFigures figures) {

    public QueryRows(List<String> columns, List<List<Object>> rows) {
        this(columns, rows, ReadFigures.NONE);
    }

    public int rowCount() {
        return rows.size();
    }

    /** The same rows with figures attached, for a path that measures its read after it. */
    public QueryRows withFigures(ReadFigures measured) {
        return new QueryRows(columns, rows, measured);
    }
}
