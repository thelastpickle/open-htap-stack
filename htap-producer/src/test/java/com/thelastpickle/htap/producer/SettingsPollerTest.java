package com.thelastpickle.htap.producer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Adopting the dashboard's controls, and keeping the current ones when it cannot be reached.
 *
 * <p>The direction of that dependency is the point: the fleet must keep generating with the
 * dashboard down, so every failure here is a reason to hold the values rather than to stop.
 */
class SettingsPollerTest {

    private static final String BODY = """
            {"settings": {"events_per_sec": 400, "drones_enabled": 250,
                          "outlier_percent": 12.5, "paused": false},
             "applied_at": "2026-08-29T10:00:00"}""";

    private final LiveSettings live = new LiveSettings(2000, 100, 5.0);
    private final List<String> said = new ArrayList<>();

    /** The four controls, out of the route's own {@code settings} object. */
    @Test
    void thePollAdoptsWhatTheBackendSaid() {
        poller(url -> BODY).poll();
        LiveSettings.Snapshot now = live.snapshot();

        assertEquals(400, now.eventsPerSec());
        assertEquals(250, now.nEntities(), "drones_enabled is the dashboard's name for the fleet");
        assertEquals(12.5, now.outlierPercent());
        assertEquals(false, now.paused());
        assertEquals(
                List.of("settings updated: eps=400 n_entities=250 outlier_percent=12.5 paused=false"),
                said);
    }

    /** A poll that changed nothing says nothing, so a steady stack keeps a quiet log. */
    @Test
    void anUnchangedPollIsSilent() {
        SettingsPoller poller = poller(url -> BODY);
        poller.poll();
        said.clear();

        poller.poll();

        assertEquals(List.of(), said);
    }

    /** A body naming only one control leaves the others where they were. */
    @Test
    void aPartialBodyLeavesTheRestAlone() {
        poller(url -> "{\"settings\": {\"paused\": true}}").poll();
        LiveSettings.Snapshot now = live.snapshot();

        assertTrue(now.paused());
        assertEquals(2000, now.eventsPerSec());
        assertEquals(100, now.nEntities());
        assertEquals(5.0, now.outlierPercent());
    }

    /** A field the producer does not act on is skipped rather than refused. */
    @Test
    void anUnknownFieldIsSkipped() {
        poller(url -> "{\"settings\": {\"events_per_sec\": 700, \"future\": {\"a\": [1, 2]}}}").poll();

        assertEquals(700, live.snapshot().eventsPerSec());
    }

    /** A backend that is not there keeps the values and reports once, not once per poll. */
    @Test
    void aFailureKeepsTheValuesAndIsSaidOnce() {
        SettingsPoller poller = poller(url -> {
            throw new IOException("Connection refused");
        });

        poller.poll();
        poller.poll();
        poller.poll();

        assertEquals(2000, live.snapshot().eventsPerSec(), "a failed poll must change nothing");
        assertEquals(1, said.size(), said.toString());
        assertEquals(
                "settings poll failed, keeping current values (IOException: Connection refused)",
                said.getFirst());
    }

    /** A second, different failure is worth a line: it is a different thing going wrong. */
    @Test
    void aDifferentFailureIsSaidAgain() {
        List<String> reasons = new ArrayList<>(List.of("first", "first", "second"));
        SettingsPoller poller = poller(url -> {
            throw new IOException(reasons.removeFirst());
        });

        poller.poll();
        poller.poll();
        poller.poll();

        assertEquals(2, said.size(), said.toString());
    }

    /** A body that is not an object at all is a failure like any other, not a crash. */
    @Test
    void rubbishIsAFailureLikeAnyOther() {
        poller(url -> "[]").poll();

        assertEquals(2000, live.snapshot().eventsPerSec());
        assertEquals(1, said.size());
        assertTrue(said.getFirst().contains("answered no object"), said.getFirst());
    }

    /** A body with no {@code settings} object changes nothing and reports nothing. */
    @Test
    void aBodyWithNoSettingsObjectChangesNothing() {
        poller(url -> "{\"applied_at\": \"2026-08-29T10:00:00\"}").poll();

        assertEquals(2000, live.snapshot().eventsPerSec());
        assertEquals(List.of(), said);
    }

    private SettingsPoller poller(SettingsPoller.Fetch fetch) {
        return new SettingsPoller(
                "http://backend:8000/api/settings/demo", live, Duration.ofSeconds(10), fetch, said::add);
    }
}
