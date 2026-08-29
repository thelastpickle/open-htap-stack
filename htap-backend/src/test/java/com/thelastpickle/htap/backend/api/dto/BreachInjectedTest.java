package com.thelastpickle.htap.backend.api.dto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;
import org.junit.jupiter.api.Test;

class BreachInjectedTest {

    private static final UUID ALERT_ID = UUID.fromString("d4e1f6a2-8b3c-11ee-9c00-0242ac120002");

    @Test
    void theResponseNamesTheAssetTheAlertAndTheSeverity() {
        BreachInjected injected = BreachInjected.of("drone-0042", 51.5, -0.12, ALERT_ID);

        assertTrue(injected.success());
        assertEquals("zone_breach", injected.scenario());
        assertEquals("drone-0042", injected.entityId());
        assertEquals(51.5, injected.latitude());
        assertEquals(-0.12, injected.longitude());
        assertEquals(ALERT_ID.toString(), injected.alertId());
        assertEquals("critical", injected.severity());
    }

    /** The page shows this line, so it names the asset rather than saying a scenario ran. */
    @Test
    void theMessageNamesTheAssetItFlagged() {
        assertEquals(
                "drone-0042 flagged for a predicted zone breach; alert written",
                BreachInjected.of("drone-0042", 51.5, -0.12, ALERT_ID).message());
    }
}
