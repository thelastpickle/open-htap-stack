package com.thelastpickle.htap.backend.api.dto;

/**
 * One field of the envelope, as the registry declares it.
 *
 * @param type whatever Avro allows there: a type name, a union as a list, or a nested record as an
 *     object. Passed through rather than flattened, because the page shows the contract
 */
public record CdcSchemaField(String name, Object type) {}
