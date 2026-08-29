package com.thelastpickle.htap.backend.api.dto;

/**
 * One column of the table, out of the envelope's nested {@code payload} record.
 *
 * @param avroType the Avro type the publisher chose, or its logical type where it declared one
 * @param cqlType the CQL type it converted from, which the publisher writes onto the Avro type as a
 *     {@code cqlType} property. Reading it back is what lets the page say {@code timestamp} where
 *     Avro says {@code long}
 */
public record CdcPayloadColumn(String name, String avroType, String cqlType) {}
