package com.thelastpickle.htap.backend.engine;

import com.thelastpickle.htap.backend.support.Cells;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/** Reads a JDBC result set into the shape every path answers in. */
final class JdbcRows {

    private JdbcRows() {}

    /**
     * The result set drained into columns and rows.
     *
     * <p>{@code stripQualifier} drops everything before the last dot in a column name.
     * HiveServer2 qualifies a projected column as {@code view.column} where Presto does not,
     * and the compare page lines the five paths up by column name.
     */
    static QueryRows read(ResultSet rows, boolean stripQualifier) throws SQLException {
        ResultSetMetaData metadata = rows.getMetaData();
        int width = metadata.getColumnCount();
        List<String> columns = new ArrayList<>(width);
        for (int i = 1; i <= width; i++) {
            String label = metadata.getColumnLabel(i);
            columns.add(stripQualifier ? label.substring(label.lastIndexOf('.') + 1) : label);
        }
        List<List<Object>> values = new ArrayList<>();
        while (rows.next()) {
            List<Object> row = new ArrayList<>(width);
            for (int i = 1; i <= width; i++) {
                row.add(Cells.plain(rows.getObject(i)));
            }
            values.add(row);
        }
        return new QueryRows(List.copyOf(columns), values);
    }
}
