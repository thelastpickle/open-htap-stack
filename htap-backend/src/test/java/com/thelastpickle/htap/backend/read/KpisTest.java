package com.thelastpickle.htap.backend.read;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;

class KpisTest {

    private static FleetSample flying(Double speed, Double altitude) {
        return new FleetSample(true, speed, altitude, false, false);
    }

    private static FleetSample grounded(Double speed, Double altitude) {
        return new FleetSample(false, speed, altitude, false, false);
    }

    @Test
    void anEmptyScanIsAllZeros() {
        assertEquals(Kpis.zero(), Kpis.of(List.of(), 0L, 0.0));
    }

    /** Speed and altitude describe the flying subset; a grounded row's zero would drag them. */
    @Test
    void speedAndAltitudeCoverTheFlyingRowsOnly() {
        Kpis kpis = Kpis.of(
                List.of(flying(10.0, 100.0), flying(20.0, 300.0), grounded(0.0, 0.0)),
                0L,
                0.0);

        assertEquals(3, kpis.totalDrones());
        assertEquals(2, kpis.activeFlyingDrones());
        assertEquals(1, kpis.groundedDrones());
        assertEquals(20.0, kpis.maxSpeedMps());
        assertEquals(10.0, kpis.minSpeedMps());
        assertEquals(15.0, kpis.avgSpeedMps());
        assertEquals(300.0, kpis.maxAltitudeM());
        assertEquals(100.0, kpis.minAltitudeM());
        assertEquals(200.0, kpis.avgAltitudeM());
    }

    /** A missing reading is skipped rather than averaged as a zero. */
    @Test
    void aNullReadingLeavesTheMeanAlone() {
        Kpis kpis = Kpis.of(List.of(flying(10.0, null), flying(20.0, 300.0)), 0L, 0.0);

        assertEquals(15.0, kpis.avgSpeedMps());
        assertEquals(300.0, kpis.avgAltitudeM());
        assertEquals(300.0, kpis.minAltitudeM());
    }

    /** A flying row with no reading at all leaves the figures at zero rather than raising. */
    @Test
    void aFleetWithNoReadingsReportsZeros() {
        Kpis kpis = Kpis.of(List.of(flying(null, null)), 0L, 0.0);

        assertEquals(1, kpis.activeFlyingDrones());
        assertEquals(0.0, kpis.maxSpeedMps());
        assertEquals(0.0, kpis.minSpeedMps());
        assertEquals(0.0, kpis.avgSpeedMps());
    }

    /** The two flags count the whole scan: an asset near a zone is near it while grounded. */
    @Test
    void theZoneCountersCountEveryRow() {
        Kpis kpis = Kpis.of(
                List.of(
                        new FleetSample(true, 5.0, 50.0, true, false),
                        new FleetSample(false, null, null, true, true)),
                7L,
                12.5);

        assertEquals(2, kpis.nearZoneCount());
        assertEquals(1, kpis.predictedBreachCount());
        assertEquals(7L, kpis.totalEvents());
        assertEquals(12.5, kpis.ingestionRatePerSec());
    }

    @Test
    void theMeanIsRoundedToATenth() {
        Kpis kpis = Kpis.of(List.of(flying(10.0, 0.0), flying(10.0, 0.0), flying(11.0, 0.0)),
                0L, 0.0);

        assertEquals(10.3, kpis.avgSpeedMps());
    }
}
