package com.thelastpickle.htap.backend.api.dto;

import java.util.List;

/**
 * One query to cancel, named by the handle its own engine gave it.
 *
 * <p>Only the two engines that hand out a handle can be asked. Cassandra keeps no list to read, and
 * the cqlite reader gives a scan no handle at all: both are stopped with the comparison instead.
 */
public record KillQueryRequest(String engine, String id) {

    /** The engines a handle can be aimed at, which the route refuses anything outside of. */
    public static final List<String> ENGINES = List.of("presto", "spark");
}
