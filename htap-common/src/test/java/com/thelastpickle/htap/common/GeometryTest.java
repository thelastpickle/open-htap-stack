package com.thelastpickle.htap.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.thelastpickle.htap.common.Geometry.LonLat;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

/**
 * Every expected value here was produced by running the Python this class replaces,
 * {@code backend/app/utils/geometry.py}, rather than derived again from the formula.
 * That is one of two copies, and {@code ingress/consumer/consumer.py:608} is the other;
 * they agree on every value and differ only on a null argument, which the sink accepts
 * and the backend does not, so the null case below is asserted against the sink.
 * The tolerances are there because {@link Math#sin} and its neighbours are specified to
 * within an ulp rather than bit-for-bit against C's libm; they are far tighter than any
 * difference a wrong formula would produce.
 */
class GeometryTest {

    /** Metres. A micrometre is some three orders of magnitude beyond the ulp at these sizes. */
    private static final double METRE_TOLERANCE = 1e-6;

    /** Degrees. */
    private static final double DEGREE_TOLERANCE = 1e-9;

    private static final String ZONE_WKT =
            "POLYGON((-122.42 37.77, -122.40 37.77, -122.40 37.79, -122.42 37.79, -122.42 37.77))";

    private static final List<LonLat> ZONE = Geometry.parseWktPolygon(ZONE_WKT);

    @Test
    void parsesAClosedRingInLonLatOrder() {
        assertEquals(5, ZONE.size());
        assertEquals(new LonLat(-122.42, 37.77), ZONE.get(0));
        assertEquals(new LonLat(-122.40, 37.79), ZONE.get(2));
        assertEquals(ZONE.get(0), ZONE.get(4));
    }

    @Test
    void parseIsCaseInsensitiveAndIgnoresSurroundingSpace() {
        assertEquals(
                List.of(new LonLat(1, 2), new LonLat(3, 4), new LonLat(5, 6)),
                Geometry.parseWktPolygon("  polygon((1 2,3 4,5 6))  "));
    }

    @Test
    @DisplayName("a vertex with a third ordinate keeps its first two")
    void parseIgnoresAThirdOrdinate() {
        assertEquals(
                List.of(new LonLat(1, 2), new LonLat(3, 4), new LonLat(5, 6)),
                Geometry.parseWktPolygon("POLYGON((1 2 99, 3 4 99, 5 6 99))"));
    }

    @Test
    @DisplayName("a malformed vertex is dropped, and the rest of the ring survives")
    void parseDropsOnlyTheBadVertex() {
        assertEquals(
                List.of(new LonLat(1, 2), new LonLat(5, 6)),
                Geometry.parseWktPolygon("POLYGON((1 2, oops 4, 5 6))"));
    }

    @Test
    @DisplayName("Java and Python accept different numeric literals, and the six are pinned")
    void parseFollowsJavasNumberGrammarAndNotPythons() {
        // Measured on Zulu 25.0.2 and on the Python 3.14.7 in the backend container. The
        // vertex either side drops is the one at index 1, so a kept literal gives three
        // vertices and a dropped one gives two.
        for (String literal : new String[] {"1d", "1f", "0x1p3"}) {
            assertEquals(3, Geometry.parseWktPolygon("POLYGON((1 2, " + literal + " 4, 5 6))").size(),
                    literal + " parses here, where Python raises ValueError");
        }
        for (String literal : new String[] {"1_0", "nan", "infinity"}) {
            assertEquals(2, Geometry.parseWktPolygon("POLYGON((1 2, " + literal + " 4, 5 6))").size(),
                    literal + " is dropped here, where Python parses it");
        }
    }

    @Test
    @DisplayName("a parsed ring is unmodifiable, as the refusals' empty ring already was")
    void parsedRingRefusesMutation() {
        List<LonLat> ring = Geometry.parseWktPolygon("POLYGON((1 2, 3 4, 5 6))");
        assertThrows(UnsupportedOperationException.class, () -> ring.add(new LonLat(7, 8)));
        assertThrows(UnsupportedOperationException.class, () -> ring.set(0, new LonLat(7, 8)));
    }

    @Test
    void parseRefusesAnythingThatIsNotAPolygon() {
        assertEquals(List.of(), Geometry.parseWktPolygon("LINESTRING(1 2, 3 4)"));
        assertEquals(List.of(), Geometry.parseWktPolygon("MULTIPOLYGON(((1 2,3 4,5 6)))"));
        assertEquals(List.of(), Geometry.parseWktPolygon("POLYGON(1 2)"));
        assertEquals(List.of(), Geometry.parseWktPolygon("POLYGON(())"));
        assertEquals(List.of(), Geometry.parseWktPolygon(""));
    }

    @Test
    @DisplayName("an interior ring merges into the outer one, as it does in both Python copies")
    void parseMergesAnInteriorRingIntoTheOuterOne() {
        // Pinned rather than asserted as correct: the bracket scan spans every ring, so the
        // two vertices carrying a stray bracket drop and the remaining eight of ten become
        // one ring. Both Python copies answer these same eight, vertex for vertex, which is
        // why the port keeps the behaviour and documents it instead of refusing the input.
        List<LonLat> merged =
                Geometry.parseWktPolygon("POLYGON((0 0,10 0,10 10,0 10,0 0),(2 2,3 2,3 3,2 3,2 2))");
        assertEquals(
                List.of(new LonLat(0, 0), new LonLat(10, 0), new LonLat(10, 10), new LonLat(0, 10),
                        new LonLat(3, 2), new LonLat(3, 3), new LonLat(2, 3), new LonLat(2, 2)),
                merged);
        // And the consequence a caller would meet: the hole is inside the merged shape.
        assertTrue(Geometry.pointInPolygon(2.5, 2.5, merged));
    }

    @Test
    @DisplayName("a null polygon is an empty ring, as it is in the sink the class replaces")
    void parseAnswersAnEmptyRingForNull() {
        // consumer.py:811 reads a nullable Cassandra column into this call, so a zone row
        // with no polygon must answer an empty ring rather than raising in the write path.
        assertEquals(List.of(), Geometry.parseWktPolygon(null));
    }

    @Test
    void containsAnInteriorPointAndRejectsAnExteriorOne() {
        assertTrue(Geometry.pointInPolygon(37.78, -122.41, ZONE));
        assertFalse(Geometry.pointInPolygon(37.80, -122.41, ZONE));
    }

    @Test
    @DisplayName("the ray cast's boundary answers are pinned, because the UI shows them")
    void containmentOnTheBoundaryMatchesThePython() {
        // The south-west corner and a point on the southern edge both read as inside.
        // Neither is a decision this port made; both are what the Python answers, and a
        // zone tool that flipped either would move an alert on and off with no code change.
        assertTrue(Geometry.pointInPolygon(37.77, -122.42, ZONE));
        assertTrue(Geometry.pointInPolygon(37.7700001, -122.4100001, ZONE));
    }

    @Test
    void aRingOfFewerThanThreeVerticesContainsNothing() {
        assertFalse(Geometry.pointInPolygon(0, 0, List.of()));
        assertFalse(Geometry.pointInPolygon(0, 0, List.of(new LonLat(0, 0), new LonLat(1, 1))));
    }

    @Test
    void haversineMeasuresTheGreatCircle() {
        assertEquals(4129086.1650573094,
                Geometry.haversineDistanceMetres(37.7749, -122.4194, 40.7128, -74.0060), METRE_TOLERANCE);
        assertEquals(0.0,
                Geometry.haversineDistanceMetres(37.7749, -122.4194, 37.7749, -122.4194), METRE_TOLERANCE);
        assertEquals(20015086.79602057,
                Geometry.haversineDistanceMetres(0.0, 0.0, 0.0, 180.0), METRE_TOLERANCE);
    }

    @Test
    void distanceIsZeroInsideAndMeasuredToTheEdgeOutside() {
        assertEquals(0.0, Geometry.distanceToPolygonMetres(37.78, -122.41, ZONE), METRE_TOLERANCE);
        assertEquals(1113.1999999997786,
                Geometry.distanceToPolygonMetres(37.80, -122.41, ZONE), METRE_TOLERANCE);
    }

    @Test
    @DisplayName("a point alongside an edge is nearer than either corner")
    void distanceIsToTheEdgeAndNotToTheNearestVertex() {
        // Due north of the middle of the northern edge. The corners are about 880 m away
        // to the east and west, so a vertex-only solution would answer some 1.4 km.
        double toEdge = Geometry.distanceToPolygonMetres(37.80, -122.41, ZONE);
        double toNearestCorner = Math.min(
                Geometry.haversineDistanceMetres(37.80, -122.41, 37.79, -122.42),
                Geometry.haversineDistanceMetres(37.80, -122.41, 37.79, -122.40));
        assertTrue(toEdge < toNearestCorner,
                "expected " + toEdge + " m to the edge to beat " + toNearestCorner + " m to a corner");
    }

    @Test
    @DisplayName("near the pole the cosine floor decides the answer, and it is the Python's")
    void distanceNearThePoleUsesTheFlooredLongitudeScale() {
        // No zone in the demo is here, but the floor is a branch nothing else reaches, and
        // removing it changes an answer rather than raising. Both values are the Python's;
        // beside each is what the same ring answers with the floor taken out, computed the
        // same way, so the test fails on a wrong number and not merely on a missing one.
        List<LonLat> polar = List.of(
                new LonLat(10.0, 89.9), new LonLat(11.0, 89.9),
                new LonLat(11.0, 89.95), new LonLat(10.0, 89.95), new LonLat(10.0, 89.9));

        // At the pole itself: unfloored, the x axis collapses to 6.8e-12 m per degree and
        // the answer is the 0.05 degrees of latitude alone, 5566.0 m.
        assertEquals(12445.954362763689,
                Geometry.distanceToPolygonMetres(90.0, 0.0, polar), METRE_TOLERANCE);

        // Due east of the ring at its own latitude: 9 degrees of longitude at the floored
        // 1113.2 m each. Unfloored the scale is 194.28 m and the answer 1748.6 m.
        assertEquals(10018.800000000001,
                Geometry.distanceToPolygonMetres(89.9, 20.0, polar), METRE_TOLERANCE);
    }

    @Test
    void aRingOfFewerThanThreeVerticesIsInfinitelyFarAway() {
        assertEquals(Double.POSITIVE_INFINITY,
                Geometry.distanceToPolygonMetres(0, 0, List.of(new LonLat(0, 0), new LonLat(1, 1))));
    }

    @Test
    void headingIsClockwiseFromNorth() {
        assertEquals(0.0, Geometry.initialHeadingDegrees(0, 0, 1, 0), DEGREE_TOLERANCE);
        assertEquals(90.0, Geometry.initialHeadingDegrees(0, 0, 0, 1), DEGREE_TOLERANCE);
        assertEquals(69.90820315804166,
                Geometry.initialHeadingDegrees(37.7749, -122.4194, 40.7128, -74.0060), DEGREE_TOLERANCE);
        assertEquals(281.66968403797364,
                Geometry.initialHeadingDegrees(40.7128, -74.0060, 37.7749, -122.4194), DEGREE_TOLERANCE);
    }
}
