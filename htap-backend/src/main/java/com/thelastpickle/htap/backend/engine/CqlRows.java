package com.thelastpickle.htap.backend.engine;

import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.thelastpickle.htap.backend.support.Cells;
import java.util.ArrayList;
import java.util.List;

/** Reads a CQL result set into the shape every path answers in. */
final class CqlRows {

    private CqlRows() {}

    /**
     * The result set drained into columns and rows.
     *
     * <p>Iterating pages, so a statement whose {@code LIMIT} is larger than the page size still
     * arrives whole; the console bounds every statement, which is what keeps that finite.
     */
    static QueryRows read(ResultSet rows) {
        ColumnDefinitions definitions = rows.getColumnDefinitions();
        List<String> columns = new ArrayList<>(definitions.size());
        for (ColumnDefinition definition : definitions) {
            // asInternal, so a column CQL considers case-sensitive is not reported in the quotes
            // that spell it in a statement; the compare page lines the paths up by this name.
            columns.add(definition.getName().asInternal());
        }
        List<List<Object>> values = new ArrayList<>();
        for (Row row : rows) {
            List<Object> cells = new ArrayList<>(columns.size());
            for (int i = 0; i < columns.size(); i++) {
                cells.add(Cells.plain(row.getObject(i)));
            }
            values.add(cells);
        }
        return new QueryRows(List.copyOf(columns), values);
    }
}
