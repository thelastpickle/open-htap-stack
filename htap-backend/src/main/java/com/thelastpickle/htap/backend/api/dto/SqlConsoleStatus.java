package com.thelastpickle.htap.backend.api.dto;

import java.util.List;

/**
 * Whether cassandra-sql is reachable, and what it is.
 *
 * @param keyspaces the three keyspaces it keeps its own rows in, named so the page can say plainly
 *     that these are not the demo's tables
 */
public record SqlConsoleStatus(
        String engine,
        boolean connected,
        String host,
        int port,
        String database,
        List<String> keyspaces) {}
