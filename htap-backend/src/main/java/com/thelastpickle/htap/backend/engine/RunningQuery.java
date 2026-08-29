package com.thelastpickle.htap.backend.engine;

/**
 * One query the Presto coordinator has not finished with.
 *
 * @param sql the statement on one line and bounded in length, because the Health page shows a line
 *     rather than a plan
 * @param user the label the client connected as, which is how a scan is attributed to this dashboard
 *     rather than to somebody running the CLI in the container
 */
public record RunningQuery(
        String id, String state, String sql, double runningS, String user, String source) {}
