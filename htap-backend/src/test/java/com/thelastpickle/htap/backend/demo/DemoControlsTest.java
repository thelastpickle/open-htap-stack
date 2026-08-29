package com.thelastpickle.htap.backend.demo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.thelastpickle.htap.backend.api.dto.DemoSettings;
import com.thelastpickle.htap.backend.api.dto.DemoSettingsResponse;
import com.thelastpickle.htap.backend.config.DemoDefaults;
import org.junit.jupiter.api.Test;

/** The demo settings in force: what a restart returns to, what the ceiling does, and the toggle. */
class DemoControlsTest {

    private final DemoControls controls = new DemoControls(new Declared(100, 2000, 2000, 5.0));

    @Test
    void theStartupStateIsWhatTheEnvironmentDeclared() {
        assertEquals(new DemoSettings(100, 2000, 5.0, false), controls.startupState());
        assertEquals(controls.startupState(), controls.current());
    }

    /**
     * A fleet declared above the ceiling is capped before the page ever opens.
     *
     * <p>Compose can set the two independently, and the page's first read is what its slider is
     * positioned from; a position the very next POST would cap is a control that moves on its own.
     */
    @Test
    void aDeclaredFleetLargerThanTheCeilingIsAlreadyCapped() {
        DemoControls capped = new DemoControls(new Declared(5000, 2000, 2000, 5.0));

        assertEquals(2000, capped.startupState().dronesEnabled());
    }

    @Test
    void whatWasAskedForIsInForceForTheNextPoll() {
        DemoSettingsResponse answered = controls.update(new DemoSettings(40, 800, 1.5, false));

        assertEquals(new DemoSettings(40, 800, 1.5, false), answered.settings());
        assertEquals(answered.settings(), controls.current());
        assertTrue(answered.success());
        assertEquals("Settings updated; the producer picks them up within its poll interval",
                answered.message());
    }

    /** Capped rather than refused, and the message is where the page learns the drag was cut. */
    @Test
    void aFleetOverTheCeilingIsCappedAndSaidSo() {
        DemoSettingsResponse answered = controls.update(new DemoSettings(9000, 800, 1.5, false));

        assertEquals(2000, answered.settings().dronesEnabled());
        assertEquals(2000, controls.current().dronesEnabled());
        assertEquals("Fleet size capped at MAX_ENTITIES (2000)", answered.message());
    }

    /** The ceiling itself is a legal fleet, so the cap must not fire on it. */
    @Test
    void aFleetExactlyAtTheCeilingIsNotReportedAsCapped() {
        DemoSettingsResponse answered = controls.update(new DemoSettings(2000, 800, 1.5, false));

        assertEquals(2000, answered.settings().dronesEnabled());
        assertTrue(answered.message().startsWith("Settings updated"), answered.message());
    }

    /** A toggle and not a setter: the page sends no state, so the answer has to say which it is. */
    @Test
    void pauseTogglesAndTheMessageSaysWhichWay() {
        DemoSettingsResponse paused = controls.togglePause();

        assertTrue(paused.settings().paused());
        assertEquals("Data generation paused", paused.message());

        DemoSettingsResponse resumed = controls.togglePause();

        assertFalse(resumed.settings().paused());
        assertEquals("Data generation resumed", resumed.message());
        assertFalse(controls.current().paused());
    }

    @Test
    void pausingLeavesTheOtherThreeFiguresAlone() {
        controls.update(new DemoSettings(40, 800, 1.5, false));

        DemoSettings paused = controls.togglePause().settings();

        assertEquals(new DemoSettings(40, 800, 1.5, true), paused);
    }

    /** A POST carries {@code paused}, so it is how the page resumes as well as how it retimes. */
    @Test
    void anUpdateReplacesThePauseFlagAsWellAsTheFigures() {
        controls.togglePause();

        DemoSettings adopted = controls.update(new DemoSettings(40, 800, 1.5, false)).settings();

        assertFalse(adopted.paused());
    }

    /** Startup is a computation over the environment, so it survives whatever has been set since. */
    @Test
    void theStartupStateIsUnaffectedByWhatHasBeenSet() {
        controls.update(new DemoSettings(40, 800, 1.5, true));

        assertEquals(new DemoSettings(100, 2000, 5.0, false), controls.startupState());
    }

    private record Declared(int nEntities, int maxEntities, int eventsPerSec, double outlierPercent)
            implements DemoDefaults {}
}
