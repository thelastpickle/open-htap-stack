package com.thelastpickle.htap.backend.read;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class PolygonSummaryTest {

    private static FleetRow row(Double speed, Double altitude, Double tempInternal) {
        return new FleetRow(
                "asset-1",
                Instant.EPOCH,
                1.0,
                2.0,
                altitude,
                speed,
                0.0,
                true,
                tempInternal,
                null,
                false,
                false,
                null);
    }

    @Test
    void noRowsInsideIsAllZeros() {
        assertEquals(PolygonSummary.empty(), PolygonSummary.of(List.of()));
    }

    @Test
    void everyRowCountsTowardsTheAggregate() {
        PolygonSummary summary = PolygonSummary.of(
                List.of(row(10.0, 100.0, 20.0), row(20.0, 200.0, 24.0)));

        assertEquals(2, summary.droneCount());
        assertEquals(15.0, summary.avgSpeedMps());
        assertEquals(20.0, summary.maxSpeedMps());
        assertEquals(150.0, summary.avgAltitudeM());
        assertEquals(200.0, summary.maxAltitudeM());
        assertEquals(22.0, summary.avgTempInternalC());
    }

    /**
     * Unlike the key performance indicator (KPI) reduction, this one counts every row inside
     * the polygon whether it is flying or not, and the count is the row count rather than the
     * count of readings: two assets inside with one temperature between them is a count of 2
     * and that one reading's mean.
     */
    @Test
    void aMissingReadingLeavesTheCountAloneAndTheMeanHonest() {
        PolygonSummary summary = PolygonSummary.of(
                List.of(row(10.0, null, null), row(20.0, 200.0, 24.0)));

        assertEquals(2, summary.droneCount());
        assertEquals(200.0, summary.avgAltitudeM());
        assertEquals(24.0, summary.avgTempInternalC());
    }
}
