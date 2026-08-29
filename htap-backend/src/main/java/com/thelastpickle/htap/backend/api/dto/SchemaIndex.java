package com.thelastpickle.htap.backend.api.dto;

/**
 * One index.
 *
 * @param detail the index class on the CQL side, and the {@code CREATE} statement on the SQL side
 * @param target the column the index is on, as the engine spells it
 */
public record SchemaIndex(String name, String table, String detail, String target) {}
