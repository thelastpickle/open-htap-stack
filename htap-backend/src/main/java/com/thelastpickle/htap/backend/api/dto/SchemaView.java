package com.thelastpickle.htap.backend.api.dto;

import java.util.List;

/**
 * One engine's whole schema, and what it could not answer.
 *
 * <p>Two routes rather than one, because the two engines fail apart: Cassandra can be up while
 * cassandra-sql is down, and a page that read both in one call would blank the half it could still
 * answer.
 *
 * @param storageKeyspaces the keyspaces the rows are physically encoded into. On the SQL side these
 *     are the three cassandra-sql owns, which is how the page shows that this is SQL over Cassandra
 *     rather than a second database
 * @param warnings read fresh on every request and never held, because a catalog here can go stale
 */
public record SchemaView(
        String engine,
        String keyspace,
        List<SchemaTable> tables,
        List<SchemaIndex> indexes,
        List<String> storageKeyspaces,
        List<String> warnings,
        String error) {}
