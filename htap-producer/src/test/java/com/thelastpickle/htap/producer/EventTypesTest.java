package com.thelastpickle.htap.producer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashSet;
import org.junit.jupiter.api.Test;

/**
 * The event types, which the compare page groups by.
 *
 * <p>The order matters as much as the names: the type is the asset's index into this list, so
 * reordering it would change which type every asset reports and make a comparison against a stack
 * that had run before disagree for no reason in the data.
 */
class EventTypesTest {

    @Test
    void thereAreTwentyDistinctTypesInThePythonsOrder() {
        assertEquals(20, EventTypes.ALL.size());
        assertEquals(20, new HashSet<>(EventTypes.ALL).size());
        assertEquals("telemetry_update", EventTypes.ALL.getFirst());
        assertEquals("activity_log", EventTypes.ALL.getLast());
    }

    /** An asset keeps one type, and the list wraps for a fleet larger than it. */
    @Test
    void anAssetsTypeIsItsIndexIntoTheList() {
        assertEquals("telemetry_update", EventTypes.of(0));
        assertEquals("position_report", EventTypes.of(1));
        assertEquals("activity_log", EventTypes.of(19));
        assertEquals("telemetry_update", EventTypes.of(20));
        assertEquals("position_report", EventTypes.of(41));
    }

    /**
     * The compare page keeps five of the twenty, so a fleet spreads over all of them.
     *
     * <p>Which is why the presets' rows sum to about a quarter of a window rather than to its total,
     * a figure CLAUDE.md records; a fleet reporting one type would make that preset return
     * everything or nothing.
     */
    @Test
    void aFleetOfAHundredCoversEveryType() {
        HashSet<String> types = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            types.add(EventTypes.of(i));
        }

        assertEquals(20, types.size());
    }
}
