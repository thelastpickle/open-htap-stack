package com.thelastpickle.htap.backend.api.dto;

import java.util.List;

/**
 * One table, its key, and whether Accord fronts it.
 *
 * @param transactionalMode {@code off}, {@code mixed_reads} or {@code full}, read out of the
 *     {@code DESCRIBE} statement because {@code system_schema.tables} carries no such column.
 *     Empty on the SQL side, where the question does not arise: cassandra-sql's own tables are all
 *     Accord tables, and it is the engine rather than the table that decides
 * @param rowCount rows where the engine will answer cheaply, absent rather than zero where it will
 *     not; {@code COUNT(*)} over an empty table raises on the SQL side
 * @param createStatement the whole statement, for a reader who wants the options as well
 */
public record SchemaTable(
        String name,
        List<SchemaColumn> columns,
        String transactionalMode,
        Integer rowCount,
        String createStatement,
        String note) {}
