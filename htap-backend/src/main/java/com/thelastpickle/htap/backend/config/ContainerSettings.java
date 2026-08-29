package com.thelastpickle.htap.backend.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

/**
 * Which container command the dashboard tells an operator to run.
 *
 * <p>The Health page renders a restart and two log commands as copyable text, and this
 * repository runs podman. A workshop attendee runs docker, and a dashboard that hands them
 * a command their machine does not have is the one failure they cannot diagnose: the
 * product told them the wrong thing. A setting rather than a build argument, because the
 * alternative is two frontend images.
 */
@ConfigMapping(prefix = "container")
public interface ContainerSettings {

    @WithDefault("podman")
    String cli();
}
