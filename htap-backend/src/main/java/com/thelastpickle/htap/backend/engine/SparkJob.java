package com.thelastpickle.htap.backend.engine;

/**
 * One job the Thrift Server's application is running.
 *
 * @param sql the job's description collapsed to one line and bounded, which is also what a cancel
 *     matches its own statements against
 * @param tasksDone how many of {@code tasksTotal} have finished, which is the only progress any
 *     engine here reports
 */
public record SparkJob(
        String id, String state, String sql, double runningS, int tasksDone, int tasksTotal) {}
