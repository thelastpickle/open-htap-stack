package com.thelastpickle.htap.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Geometry for the restricted-zone tools.
 *
 * <p>Zones are a few kilometres across, so distances are computed on a local
 * equirectangular projection in metres. That is accurate to well under a metre at
 * this scale and far cheaper than a spherical solution.
 */
public final class Geometry {

    private static final double EARTH_RADIUS_M = 6_371_000.0;
    private static final double M_PER_DEG_LAT = 111_320.0;

    private Geometry() {}

    /** A ring vertex in the x, y order Well-Known Text (WKT) uses: longitude first. */
    public record LonLat(double lon, double lat) {}

    /**
     * Parses {@code POLYGON((lon lat, lon lat, ...))}, a single ring, returning an empty
     * ring on anything it cannot read, null included. The ring is unmodifiable.
     *
     * <p>An interior ring is not detected, and what happens instead is worth writing down
     * because the answer is a shape rather than a refusal. {@code indexOf("((")} and
     * {@code lastIndexOf("))")} span every ring at once, so the two vertices carrying a
     * stray bracket fail to parse and are dropped and the rest are appended into one ring.
     * Measured on {@code POLYGON((0 0,10 0,10 10,0 10,0 0),(2 2,3 2,3 3,2 3,2 2))}: eight
     * vertices of the ten survive and a point inside the hole answers inside. Both Python
     * copies answer the same eight, vertex for vertex, so this is the behaviour carried
     * over rather than one the port introduced, and the three zones the sink seeds are
     * simple rings that never reach it. Refusing the
     * input instead would trade a wrong shape for no shape, which for a breach check is
     * not obviously the better answer, so the behaviour stays and the claim is corrected.
     *
     * <p>The two Python copies disagreed about null and the sink's behaviour is the one
     * kept: the sink stripped {@code (wkt or "")} where the backend's own helper stripped the
     * argument itself and raised. The sink is the copy that needed it, because it passed
     * {@code polygon_wkt} straight from a Cassandra row and that column is nullable, so
     * refusing here would move a zone with no polygon from an empty ring to an exception
     * inside the write path.
     *
     * <p>Which literals a vertex may use is {@link Double#parseDouble}'s answer and not
     * Python's {@code float()}'s, and the two differ in both directions, so one copy drops a
     * vertex the other keeps. Measured on Zulu 25.0.2 against the Python 3.14.7 the backend
     * container runs: {@code 1d}, {@code 1f} and {@code 0x1p3} parse here and raise
     * {@code ValueError} there; {@code 1_0}, {@code nan} and {@code infinity} parse there, as
     * 10.0, nan and inf, and are dropped here. No zone this stack writes carries such a
     * literal, and {@code GeometryTest} pins all six, so a change on either side is a failing
     * test rather than a ring that differs by one vertex.
     */
    public static List<LonLat> parseWktPolygon(String wkt) {
        if (wkt == null) return List.of();
        String text = wkt.strip();
        if (!text.toUpperCase(Locale.ROOT).startsWith("POLYGON")) return List.of();

        int start = text.indexOf("((");
        int end = text.lastIndexOf("))");
        if (start < 0 || end < start + 2) return List.of();

        List<LonLat> ring = new ArrayList<>();
        for (String pair : text.substring(start + 2, end).split(",", -1)) {
            String[] parts = pair.strip().split("\\s+");
            if (parts.length < 2) continue;
            try {
                ring.add(new LonLat(Double.parseDouble(parts[0]), Double.parseDouble(parts[1])));
            } catch (NumberFormatException ignored) {
                // A malformed vertex is dropped rather than failing the ring, which is
                // what the zone tools want: a zone with one bad vertex still has a shape.
            }
        }
        // Immutable, so a parsed ring is the same kind of value on every path out of
        // this method: the two refusals above answer List.of(), and a caller that
        // could mutate one answer but not the others has a trap waiting for it.
        return List.copyOf(ring);
    }

    /** Ray-casting containment test against a (lon, lat) ring. */
    public static boolean pointInPolygon(double lat, double lon, List<LonLat> polygon) {
        if (polygon.size() < 3) return false;

        boolean inside = false;
        int j = polygon.size() - 1;
        for (int i = 0; i < polygon.size(); i++) {
            LonLat vi = polygon.get(i);
            LonLat vj = polygon.get(j);
            // Only edges straddling the ray's latitude can cross it, which also
            // guarantees vj.lat() != vi.lat() in the division below.
            if ((vi.lat() > lat) != (vj.lat() > lat)) {
                double xAtLat = vi.lon() + (vj.lon() - vi.lon()) * (lat - vi.lat()) / (vj.lat() - vi.lat());
                if (lon < xAtLat) inside = !inside;
            }
            j = i;
        }
        return inside;
    }

    /** Great-circle distance in metres. */
    public static double haversineDistanceMetres(double lat1, double lon1, double lat2, double lon2) {
        double rlat1 = Math.toRadians(lat1);
        double rlat2 = Math.toRadians(lat2);
        double dlat = rlat2 - rlat1;
        double dlon = Math.toRadians(lon2 - lon1);
        double a = Math.pow(Math.sin(dlat / 2), 2)
                + Math.cos(rlat1) * Math.cos(rlat2) * Math.pow(Math.sin(dlon / 2), 2);
        return 2 * EARTH_RADIUS_M * Math.asin(Math.min(1.0, Math.sqrt(a)));
    }

    /**
     * Shortest distance in metres from a point to a polygon's boundary, 0.0 inside it
     * and {@link Double#POSITIVE_INFINITY} for a ring of fewer than three vertices.
     *
     * <p>Measured to the nearest point on each edge rather than to the nearest vertex: a
     * drone alongside a long edge is close to the zone even when it is far from either
     * corner.
     */
    public static double distanceToPolygonMetres(double lat, double lon, List<LonLat> polygon) {
        if (polygon.size() < 3) return Double.POSITIVE_INFINITY;
        if (pointInPolygon(lat, lon, polygon)) return 0.0;

        // Project to metres about the query point. The floor on the cosine stops the x
        // axis collapsing near the pole, where the scale would otherwise reach 6.8e-12 m
        // per degree at latitude 90 and every distance would be measured in latitude
        // alone. It holds a degree of longitude at 1113.2 m from about 89.43 upward, so
        // the answer there is a floor and not a projection.
        double mPerDegLon = M_PER_DEG_LAT * Math.max(Math.cos(Math.toRadians(lat)), 0.01);

        double nearest = Double.POSITIVE_INFINITY;
        for (int i = 0; i < polygon.size(); i++) {
            LonLat a = polygon.get(i);
            LonLat b = polygon.get((i + 1) % polygon.size());
            double ax = (a.lon() - lon) * mPerDegLon;
            double ay = (a.lat() - lat) * M_PER_DEG_LAT;
            double bx = (b.lon() - lon) * mPerDegLon;
            double by = (b.lat() - lat) * M_PER_DEG_LAT;

            // Distance from the origin, which is the query point, to segment AB.
            double dx = bx - ax;
            double dy = by - ay;
            double segLenSq = dx * dx + dy * dy;
            double t = segLenSq == 0.0
                    ? 0.0
                    : Math.max(0.0, Math.min(1.0, -(ax * dx + ay * dy) / segLenSq));
            nearest = Math.min(nearest, Math.hypot(ax + t * dx, ay + t * dy));
        }
        return nearest;
    }

    /**
     * Initial heading from point 1 to point 2, in degrees clockwise from north.
     *
     * <p>Ports {@code compute_bearing_deg}; the sink writes the result to
     * {@code demo.drone_latest_status.heading_deg}, which is what the name follows.
     */
    public static double initialHeadingDegrees(double lat1, double lon1, double lat2, double lon2) {
        double rlat1 = Math.toRadians(lat1);
        double rlat2 = Math.toRadians(lat2);
        double dlon = Math.toRadians(lon2 - lon1);
        double x = Math.sin(dlon) * Math.cos(rlat2);
        double y = Math.cos(rlat1) * Math.sin(rlat2) - Math.sin(rlat1) * Math.cos(rlat2) * Math.cos(dlon);
        return (Math.toDegrees(Math.atan2(x, y)) + 360.0) % 360.0;
    }
}
