package com.thelastpickle.htap.backend.api.dto;

/**
 * The settings in force, with a line saying what the request did to them.
 *
 * <p>The message is the whole answer on two of the routes: a fleet size that was capped, and a
 * pause that toggled rather than being set, are both cases where what the caller asked for and
 * what is now running differ.
 */
public record DemoSettingsResponse(DemoSettings settings, boolean success, String message) {

    /** A read, which changed nothing and so has nothing to say. */
    public static DemoSettingsResponse of(DemoSettings settings) {
        return new DemoSettingsResponse(settings, true, "");
    }

    public static DemoSettingsResponse of(DemoSettings settings, String message) {
        return new DemoSettingsResponse(settings, true, message);
    }
}
