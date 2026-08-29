package com.thelastpickle.htap.backend.query;

import java.util.List;

/**
 * The comparison in flight, as the Health page shows it.
 *
 * <p>What is running is reported and not merely that something is: a browser that gives up on a
 * long run leaves it going here, and "already running" with no age, statement or progress reads like
 * a stuck dashboard rather than a run somebody can stop.
 *
 * @param done the paths that have answered, in the order the run asks them
 */
public record ComparisonRun(
        double runningForS, RunMode mode, List<String> engines, String sql, List<String> done) {}
