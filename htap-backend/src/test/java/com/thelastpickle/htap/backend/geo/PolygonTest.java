package com.thelastpickle.htap.backend.geo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class PolygonTest {

    /** A square about 1 km on a side, at a latitude where a degree of longitude is shorter. */
    private static final String SQUARE = "POLYGON((10.0 50.0, 10.01 50.0, 10.01 50.01, "
            + "10.0 50.01, 10.0 50.0))";

    private static Polygon square() {
        return Polygon.parseWkt(SQUARE).orElseThrow();
    }

    @Test
    void aRingParsesAsLonThenLat() {
        Polygon polygon = square();

        assertEquals(5, polygon.ring().size());
        assertEquals(new Polygon.Point(10.0, 50.0), polygon.ring().get(0));
        assertEquals(new Polygon.Point(10.01, 50.01), polygon.ring().get(2));
    }

    @Test
    void whitespaceAndCaseAreToleratedAsWktAllows() {
        assertTrue(Polygon.parseWkt("  polygon ((10 50,\n11 50, 11 51, 10 50))  ").isPresent());
    }

    @Test
    void textThatNamesNoPolygonIsRefused() {
        assertEquals(Optional.empty(), Polygon.parseWkt("LINESTRING(10 50, 11 51)"));
        assertEquals(Optional.empty(), Polygon.parseWkt("POLYGON"));
        assertEquals(Optional.empty(), Polygon.parseWkt("POLYGON(())"));
        assertEquals(Optional.empty(), Polygon.parseWkt(""));
        assertEquals(Optional.empty(), Polygon.parseWkt(null));
    }

    /** A pair that is not two numbers is dropped, which is what the Python's parser did. */
    @Test
    void anUnreadablePairIsSkippedAndTheRestKept() {
        Polygon polygon = Polygon.parseWkt("POLYGON((10 50, north 50, 11 51, 10 51))")
                .orElseThrow();

        assertEquals(3, polygon.ring().size());
    }

    @Test
    void containmentIsDecidedByRayCasting() {
        Polygon polygon = square();

        assertTrue(polygon.contains(50.005, 10.005));
        assertFalse(polygon.contains(50.005, 10.02));
        assertFalse(polygon.contains(50.02, 10.005));
    }

    /** Fewer than three points bounds nothing, and is infinitely far from everywhere. */
    @Test
    void aDegenerateRingContainsNothing() {
        Polygon line = Polygon.parseWkt("POLYGON((10 50, 11 51))").orElseThrow();

        assertFalse(line.contains(50.5, 10.5));
        assertEquals(Double.POSITIVE_INFINITY, line.distanceM(50.5, 10.5));
    }

    @Test
    void aPointInsideIsZeroFromTheZone() {
        assertEquals(0.0, square().distanceM(50.005, 10.005));
    }

    /**
     * The distance is to the nearest point on an edge, not to the nearest corner: a position
     * level with the middle of the northern edge is about 111 m from it, where the nearest
     * corner is some 350 m away.
     */
    @Test
    void theDistanceIsMeasuredToTheEdgeRatherThanToACorner() {
        double toEdge = square().distanceM(50.011, 10.005);

        assertEquals(111.3, toEdge, 0.5);
    }

    @Test
    void theProjectionShortensADegreeOfLongitudeWithLatitude() {
        // 0.01 degrees of longitude east of the square's eastern edge, at latitude 50, is
        // 111_320 * cos(50) metres and not 1113.2.
        double eastwards = square().distanceM(50.005, 10.02);

        assertEquals(715.4, eastwards, 1.0);
    }

    @Test
    void aRingIsHeldAsGivenAndCannotBeChangedAfterwards() {
        List<Polygon.Point> ring = new java.util.ArrayList<>(square().ring());
        Polygon polygon = new Polygon(ring);
        ring.clear();

        assertEquals(5, polygon.ring().size());
    }
}
