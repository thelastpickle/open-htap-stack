package com.thelastpickle.htap.backend.api.dto;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;

/**
 * The Avro schema the topic's records are written against.
 *
 * <p>Read from the registry and not from a record, because the point is that the contract lives
 * there.
 *
 * @param fields the envelope's own fields, in declaration order
 * @param payloadFields the table's columns, from the nested {@code payload} record. Separate from
 *     the envelope's fields, because the two answer different questions
 * @param avroSchema the registry's reply as it stands, for a reader who wants the whole document
 */
public record CdcSchemaView(
        String subject,
        Integer schemaId,
        Integer version,
        List<CdcSchemaField> fields,
        List<CdcPayloadColumn> payloadFields,
        String registry,
        JsonNode avroSchema,
        String error) {

    /** A view carrying nothing but the reason the registry could not answer. */
    public static CdcSchemaView failed(String subject, String registry, String error) {
        return new CdcSchemaView(subject, null, null, List.of(), List.of(), registry, null, error);
    }
}
