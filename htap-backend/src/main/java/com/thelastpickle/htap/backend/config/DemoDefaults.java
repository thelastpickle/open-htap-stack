package com.thelastpickle.htap.backend.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;

/**
 * What the data producer was started with, which is what the Settings page opens showing.
 *
 * <p>Compose gives the same four figures to the producer under shorter names and to this backend
 * under these, so the page's initial state is the fleet that is actually running rather than a
 * guess. The producer owns them from then on: it polls {@code /api/settings/demo} and adopts
 * whatever it finds, so a change made here is a change to the running stack.
 */
@ConfigMapping(prefix = "demo")
public interface DemoDefaults {

    /** How many assets the producer reports on at startup. */
    @WithName("n-entities")
    @WithDefault("100")
    int nEntities();

    /**
     * The ceiling the Settings page's fleet size is held under.
     *
     * <p>A ceiling and not a default: the producer holds one position per asset in memory and
     * sends a reading for each, so a slider dragged to its own maximum would ask for a fleet
     * neither the producer nor the sink was sized for.
     */
    @WithName("max-entities")
    @WithDefault("2000")
    int maxEntities();

    /**
     * The ingest rate the producer starts at, which the Settings page opens showing.
     *
     * <p>Five a second, matching what compose gives the producer. The rate the docs' figures were
     * measured at is 2,000, and the page reaches 5,000: the demo starts as a trickle and is turned
     * up for the run it is being shown in, because a stack left at 2,000 fills a laptop's disk in an
     * afternoon and nothing downstream of the generator bounds it.
     */
    @WithName("events-per-sec")
    @WithDefault("5")
    int eventsPerSec();

    /**
     * The share of telemetry carrying an anomalous internal temperature, as a percentage.
     *
     * <p>The Explore page's outlier queries need something to find, and at zero they answer
     * correctly with nothing, which reads as a broken query rather than a clean fleet.
     */
    @WithName("outlier-percent")
    @WithDefault("5.0")
    double outlierPercent();
}
