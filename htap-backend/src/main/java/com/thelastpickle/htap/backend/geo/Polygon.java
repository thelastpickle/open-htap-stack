package com.thelastpickle.htap.backend.geo;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

/**
 * A restricted-zone ring, as Well-Known Text (WKT) holds it: (lon, lat) pairs, x before y.
 *
 * <p>Zones are a few kilometres across, so {@link #distanceM} projects to metres about the
 * query point rather than solving on the sphere. At this scale the error is well under a
 * metre, and the projection is far cheaper.
 */
public record Polygon(List<Point> ring) {

    /** Metres per degree of latitude, which is constant enough at zone scale. */
    private static final double M_PER_DEG_LAT = 111_320.0;

    public record Point(double lon, double lat) {}

    public Polygon {
        ring = List.copyOf(ring);
    }

    /**
     * Parse {@code POLYGON((lon lat, lon lat, ...))}.
     *
     * <p>Empty when the text names no polygon or holds no coordinate pair. A ring of one or
     * two points does parse, and then contains nothing and is infinitely far from everything;
     * that is what the Python did, and the routes answer 400 only for the empty case.
     */
    public static Optional<Polygon> parseWkt(String wkt) {
        if (wkt == null) {
            return Optional.empty();
        }
        String text = wkt.strip();
        if (!text.toUpperCase(Locale.ROOT).startsWith("POLYGON")) {
            return Optional.empty();
        }
        int start = text.indexOf("((");
        int end = text.lastIndexOf("))");
        if (start == -1 || end == -1) {
            return Optional.empty();
        }
        List<Point> ring = new ArrayList<>();
        for (String pair : text.substring(start + 2, end).split(",", -1)) {
            String[] parts = pair.strip().split("\\s+");
            if (parts.length < 2) {
                continue;
            }
            try {
                ring.add(new Point(Double.parseDouble(parts[0]), Double.parseDouble(parts[1])));
            } catch (NumberFormatException e) {
                continue;
            }
        }
        return ring.isEmpty() ? Optional.empty() : Optional.of(new Polygon(ring));
    }

    /** Ray-casting containment test. */
    public boolean contains(double lat, double lon) {
        if (ring.size() < 3) {
            return false;
        }
        boolean inside = false;
        int j = ring.size() - 1;
        for (int i = 0; i < ring.size(); i++) {
            Point a = ring.get(i);
            Point b = ring.get(j);
            // Only an edge straddling the ray's latitude can cross it, which also
            // guarantees the division below has a non-zero divisor.
            if (a.lat() > lat != b.lat() > lat) {
                double lonAtLat =
                        a.lon() + (b.lon() - a.lon()) * (lat - a.lat()) / (b.lat() - a.lat());
                if (lon < lonAtLat) {
                    inside = !inside;
                }
            }
            j = i;
        }
        return inside;
    }

    /**
     * Shortest distance in metres to the ring, and 0.0 for a point inside it.
     *
     * <p>Measured to the nearest point on each edge rather than to the nearest vertex: an
     * asset alongside a long edge is close to the zone although it is far from either corner.
     */
    public double distanceM(double lat, double lon) {
        if (ring.size() < 3) {
            return Double.POSITIVE_INFINITY;
        }
        if (contains(lat, lon)) {
            return 0.0;
        }
        double mPerDegLon = M_PER_DEG_LAT * Math.max(Math.cos(Math.toRadians(lat)), 0.01);
        double nearest = Double.POSITIVE_INFINITY;
        for (int i = 0; i < ring.size(); i++) {
            Point from = ring.get(i);
            Point to = ring.get((i + 1) % ring.size());
            double ax = (from.lon() - lon) * mPerDegLon;
            double ay = (from.lat() - lat) * M_PER_DEG_LAT;
            double bx = (to.lon() - lon) * mPerDegLon;
            double by = (to.lat() - lat) * M_PER_DEG_LAT;
            double dx = bx - ax;
            double dy = by - ay;
            double lengthSq = dx * dx + dy * dy;
            double t = lengthSq == 0.0
                    ? 0.0
                    : Math.clamp(-(ax * dx + ay * dy) / lengthSq, 0.0, 1.0);
            nearest = Math.min(nearest, Math.hypot(ax + t * dx, ay + t * dy));
        }
        return nearest;
    }
}
