package com.thelastpickle.htap.backend.demo;

import com.thelastpickle.htap.backend.api.dto.DemoSettings;
import com.thelastpickle.htap.backend.api.dto.DemoSettingsResponse;
import com.thelastpickle.htap.backend.config.DemoDefaults;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The demo settings in force, and the two ways a request changes them.
 *
 * <p>In memory only, and deliberately: restarting this backend returns the demo to what its
 * environment declares, so a workshop that has been dragged about recovers by a restart rather
 * than by finding the control that was moved. The producer notices within its poll interval.
 *
 * <p>An {@link AtomicReference} rather than the Python's lock. The pause toggle is the reason a
 * lock was there at all, since it reads the current value and writes its opposite; {@code
 * updateAndGet} does that read-modify-write without one, and every other field is replaced whole.
 */
@ApplicationScoped
public class DemoControls {

    private final DemoDefaults defaults;
    private final AtomicReference<DemoSettings> current;

    @Inject
    DemoControls(DemoDefaults defaults) {
        this.defaults = defaults;
        this.current = new AtomicReference<>(startup(defaults));
    }

    /** What the settings were at startup, which the page offers as a reset. */
    public DemoSettings startupState() {
        return startup(defaults);
    }

    /** The settings the producer is polling for. */
    public DemoSettings current() {
        return current.get();
    }

    /**
     * Adopt what the page asked for, with the fleet size held under {@code MAX_ENTITIES}.
     *
     * <p>Capped rather than refused, because the ceiling is this stack's and not the caller's
     * mistake: a page that offers a slider to 100,000 on a producer sized for 2,000 should say
     * what it did instead of rejecting the drag.
     */
    public DemoSettingsResponse update(DemoSettings asked) {
        int allowed = Math.min(asked.dronesEnabled(), defaults.maxEntities());
        DemoSettings adopted = asked.withDronesEnabled(allowed);
        current.set(adopted);
        String note = allowed == asked.dronesEnabled()
                ? "Settings updated; the producer picks them up within its poll interval"
                : "Fleet size capped at MAX_ENTITIES (" + defaults.maxEntities() + ")";
        return DemoSettingsResponse.of(adopted, note);
    }

    /** Stop or resume event generation, whichever the current state is not. */
    public DemoSettingsResponse togglePause() {
        DemoSettings now = current.updateAndGet(state -> state.withPaused(!state.paused()));
        return DemoSettingsResponse.of(
                now, "Data generation " + (now.paused() ? "paused" : "resumed"));
    }

    /**
     * The startup state, worked out without touching the instance.
     *
     * <p>Static because the constructor needs it, and a normal-scoped bean is proxied by a subclass,
     * so an instance method called from there is an overridable call during construction.
     */
    private static DemoSettings startup(DemoDefaults defaults) {
        return new DemoSettings(
                Math.min(defaults.nEntities(), defaults.maxEntities()),
                defaults.eventsPerSec(),
                defaults.outlierPercent(),
                false);
    }
}
