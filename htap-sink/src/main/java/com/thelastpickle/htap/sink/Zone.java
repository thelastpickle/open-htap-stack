package com.thelastpickle.htap.sink;

import com.thelastpickle.htap.common.Geometry;
import java.util.List;

/**
 * One restricted zone, with its boundary already parsed.
 *
 * <p>Parsed once per reload rather than once per event: at demo throughput that is the difference
 * between parsing three polygons a minute and thousands a second.
 */
record Zone(String zoneId, String zoneName, String severity, List<Geometry.LonLat> ring) {

    /** Whether the boundary is usable at all; two points enclose nothing. */
    boolean usable() {
        return ring.size() >= 3;
    }
}
