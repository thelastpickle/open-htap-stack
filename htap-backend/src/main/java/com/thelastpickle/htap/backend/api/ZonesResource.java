package com.thelastpickle.htap.backend.api;

import com.thelastpickle.htap.backend.api.dto.RestrictedZone;
import com.thelastpickle.htap.backend.api.dto.WhatIfZoneRequest;
import com.thelastpickle.htap.backend.api.dto.WhatIfZoneResponse;
import com.thelastpickle.htap.backend.api.dto.ZonesResponse;
import com.thelastpickle.htap.backend.engine.CassandraPath;
import com.thelastpickle.htap.common.ZoneRules;
import com.thelastpickle.htap.backend.geo.Polygon;
import com.thelastpickle.htap.backend.read.CassandraReads;
import com.thelastpickle.htap.backend.read.FleetRow;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.List;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

/** Restricted airspace, and what a proposed zone would have caught. */
@Path("/api/zones")
@Produces(MediaType.APPLICATION_JSON)
@Tag(name = "zones")
public class ZonesResource {

    /**
     * Distance at which the ingest sink starts flagging proximity. The what-if simulation uses
     * the same figure, so its answer matches the live alerting.
     */
    static final double WARNING_DISTANCE_M = ZoneRules.WARNING_DISTANCE_M;

    private final CassandraPath cassandra;
    private final CassandraReads reads;

    ZonesResource(CassandraPath cassandra, CassandraReads reads) {
        this.cassandra = cassandra;
        this.reads = reads;
    }

    /** Every enabled restricted zone. */
    @GET
    public ZonesResponse zones() {
        if (!cassandra.connected()) {
            return ZonesResponse.empty();
        }
        return Answers.orElse(
                "/api/zones",
                () -> new ZonesResponse(reads.zones().stream().map(Dtos::zone).toList()),
                ZonesResponse::empty);
    }

    /** Score a hypothetical zone against the live fleet, without persisting it. */
    @POST
    @Path("/what-if")
    @Consumes(MediaType.APPLICATION_JSON)
    public WhatIfZoneResponse whatIf(WhatIfZoneRequest request) {
        // A body-less POST arrives as null, where pydantic refused the request before the
        // Python's route ran; without this the dereference below is a 500 carrying no detail
        // field, which is the one field the pages read.
        if (request == null) {
            throw new ApiException(400, "Could not parse polygon_wkt");
        }
        // Parsed before the connection is tested, so a malformed polygon is refused whether
        // or not the fleet can be read.
        Polygon polygon = Polygon.parseWkt(request.polygonWkt())
                .orElseThrow(() -> new ApiException(400, "Could not parse polygon_wkt"));
        RestrictedZone zone = new RestrictedZone(
                "what-if", request.zoneName(), request.polygonWkt(), request.severity(), true);
        if (!cassandra.connected()) {
            return WhatIfZoneResponse.unscored(zone);
        }
        return Answers.orElse(
                "/api/zones/what-if",
                () -> score(zone, polygon),
                () -> WhatIfZoneResponse.unscored(zone));
    }

    private WhatIfZoneResponse score(RestrictedZone zone, Polygon polygon) {
        List<String> inside = new ArrayList<>();
        List<String> nearby = new ArrayList<>();
        for (FleetRow row : reads.drones(CassandraReads.FLEET_SCAN_LIMIT, false)) {
            if (!row.located()) {
                continue;
            }
            if (polygon.contains(row.latitude(), row.longitude())) {
                inside.add(row.entityId());
            } else if (polygon.distanceM(row.latitude(), row.longitude()) < WARNING_DISTANCE_M) {
                nearby.add(row.entityId());
            }
        }
        List<String> affected = new ArrayList<>(inside);
        affected.addAll(nearby);
        return new WhatIfZoneResponse(zone, inside.size(), nearby.size(), affected);
    }
}
