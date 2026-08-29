package com.thelastpickle.htap.backend.api;

import com.thelastpickle.htap.backend.api.dto.DronePosition;
import com.thelastpickle.htap.backend.api.dto.DroneTrail;
import com.thelastpickle.htap.backend.api.dto.MapLiveResponse;
import com.thelastpickle.htap.backend.api.dto.NearbyDroneResult;
import com.thelastpickle.htap.backend.api.dto.NearbyResponse;
import com.thelastpickle.htap.backend.api.dto.PolygonStatsRequest;
import com.thelastpickle.htap.backend.api.dto.TrailPoint;
import com.thelastpickle.htap.backend.engine.CassandraPath;
import com.thelastpickle.htap.backend.geo.Haversine;
import com.thelastpickle.htap.backend.geo.Polygon;
import com.thelastpickle.htap.backend.read.CassandraReads;
import com.thelastpickle.htap.backend.read.FleetRow;
import com.thelastpickle.htap.backend.read.PolygonSummary;
import com.thelastpickle.htap.backend.read.TrailRow;
import com.thelastpickle.htap.backend.support.Round;
import com.thelastpickle.htap.common.Timestamps;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

/** Live fleet positions, zone polygons and the spatial questions asked of them. */
@Path("/api/map")
@Produces(MediaType.APPLICATION_JSON)
@Tag(name = "map")
public class MapResource {

    static final int DEFAULT_MAP_LIMIT = 2000;
    static final int NEARBY_MAX_METRES = 5000;

    private final CassandraPath cassandra;
    private final CassandraReads reads;

    MapResource(CassandraPath cassandra, CassandraReads reads) {
        this.cassandra = cassandra;
        this.reads = reads;
    }

    /** Latest position per asset plus the restricted zones, for the live map. */
    @GET
    @Path("/live")
    public MapLiveResponse live(
            @QueryParam("limit") @DefaultValue("" + DEFAULT_MAP_LIMIT) int limit) {
        if (!cassandra.connected()) {
            cassandra.connect();
        }
        if (!cassandra.connected()) {
            return MapLiveResponse.empty(now());
        }
        return Answers.orElse(
                "/api/map/live",
                () -> new MapLiveResponse(
                        reads.drones(limit, false).stream().map(Dtos::drone).toList(),
                        reads.zones().stream().map(Dtos::zone).toList(),
                        now()),
                () -> MapLiveResponse.empty(now()));
    }

    /**
     * Aggregate the fleet inside an arbitrary polygon.
     *
     * <p>Cassandra has no spatial predicate, so containment is decided here over the bounded
     * latest-state table. The same question over history is asked of Presto on the Explore
     * page.
     */
    @POST
    @Path("/polygon-stats")
    @Consumes(MediaType.APPLICATION_JSON)
    public PolygonSummary polygonStats(PolygonStatsRequest request) {
        // A body-less POST arrives as null and a body of {} as a null field, where pydantic
        // refused both identically: polygon_wkt was a required str. Both refused before the
        // connection is tested for the same reason: the request is answered on what it says,
        // and it says nothing. Text that will not parse is different, and is answered with
        // zeros while Cassandra is down, because that ordering is what the page relies on.
        if (request == null || request.polygonWkt() == null) {
            throw new ApiException(400, "Could not parse polygon_wkt");
        }
        if (!cassandra.connected()) {
            return PolygonSummary.empty();
        }
        Polygon polygon = parse(request.polygonWkt());
        return Answers.orElse(
                "/api/map/polygon-stats",
                () -> PolygonSummary.of(inside(reads.drones(CassandraReads.FLEET_SCAN_LIMIT, false),
                        polygon)),
                PolygonSummary::empty);
    }

    @GET
    @Path("/drone/{entity_id}")
    public DronePosition drone(@PathParam("entity_id") String entityId) {
        requireCassandra();
        Optional<FleetRow> row = reads.drone(entityId);
        if (row.isEmpty()) {
            throw new ApiException(404, "No such asset: " + entityId);
        }
        return Dtos.drone(row.get());
    }

    /**
     * The asset's recent flight path, read from the event history table.
     *
     * <p>The recorded track, not an extrapolation from the current heading: the history is
     * already in Cassandra, so the map may as well show it.
     */
    @GET
    @Path("/drone/{entity_id}/trail")
    public DroneTrail trail(
            @PathParam("entity_id") String entityId,
            @QueryParam("points") @DefaultValue("60") int points) {
        requireCassandra();
        List<TrailRow> rows;
        try {
            rows = reads.trail(entityId, points);
        } catch (RuntimeException e) {
            throw new ApiException(500, String.valueOf(e.getMessage()));
        }
        // The table is clustered newest-first; a path reads better oldest-first.
        List<TrailPoint> path = new ArrayList<>(rows.size());
        for (int i = rows.size() - 1; i >= 0; i--) {
            TrailRow row = rows.get(i);
            if (row.located()) {
                path.add(Dtos.trailPoint(row));
            }
        }
        return new DroneTrail(entityId, path);
    }

    /** Other assets within a radius of this one. */
    @GET
    @Path("/drone/{entity_id}/nearby")
    public NearbyResponse nearby(
            @PathParam("entity_id") String entityId,
            @QueryParam("meters") @DefaultValue("500") int meters) {
        int radius = Math.clamp(meters, 1, NEARBY_MAX_METRES);
        if (!cassandra.connected()) {
            return NearbyResponse.empty();
        }
        // An asset with no position cannot anchor a radius, so an unlocated target answers
        // the same empty list as an absent one.
        Optional<FleetRow> target = reads.drone(entityId).filter(FleetRow::located);
        if (target.isEmpty()) {
            return NearbyResponse.empty();
        }
        double lat = target.get().latitude();
        double lon = target.get().longitude();
        List<NearbyDroneResult> nearby = new ArrayList<>();
        for (FleetRow row : reads.drones(CassandraReads.FLEET_SCAN_LIMIT, true)) {
            if (entityId.equals(row.entityId()) || !row.located()) {
                continue;
            }
            double distance = Haversine.metres(lat, lon, row.latitude(), row.longitude());
            if (distance <= radius) {
                nearby.add(Dtos.nearby(row, Round.tenth(distance)));
            }
        }
        nearby.sort(Comparator.comparingDouble(NearbyDroneResult::distanceM));
        return new NearbyResponse(nearby);
    }

    private void requireCassandra() {
        if (!cassandra.connected()) {
            throw new ApiException(503, "Cassandra unavailable");
        }
    }

    private static Polygon parse(String wkt) {
        return Polygon.parseWkt(wkt)
                .orElseThrow(() -> new ApiException(400, "Could not parse polygon_wkt"));
    }

    private static List<FleetRow> inside(List<FleetRow> fleet, Polygon polygon) {
        return fleet.stream()
                .filter(FleetRow::located)
                .filter(row -> polygon.contains(row.latitude(), row.longitude()))
                .toList();
    }

    private static String now() {
        return Timestamps.isoOffset(Instant.now());
    }
}
