package com.thelastpickle.htap.backend.api.dto;

import com.thelastpickle.htap.backend.engine.RunningQuery;
import com.thelastpickle.htap.backend.engine.SparkJob;

/**
 * One query an engine is still working on, whoever submitted it.
 *
 * <p>The engines are asked directly rather than this backend reporting what it submitted, so work it
 * knows nothing about is included: a {@code presto-cli} query or a {@code spark-sql} session in the
 * container is usually exactly what a viewer wants to see when the dashboard has gone slow.
 *
 * @param submitter who submitted it, where the engine records that, and empty where it does not
 * @param tasksTotal progress, for the one engine that reports it; Presto exposes nothing this simple,
 *     so both counts stay 0 there
 */
public record QueryInFlight(
        String engine,
        String id,
        String state,
        double runningS,
        String sql,
        String submitter,
        int tasksDone,
        int tasksTotal) {

    /** A Presto query, attributed to the source it declared or else to its user. */
    public static QueryInFlight of(RunningQuery query) {
        return new QueryInFlight(
                "presto",
                query.id(),
                query.state(),
                query.runningS(),
                query.sql(),
                query.source().isEmpty() ? query.user() : query.source(),
                0,
                0);
    }

    /** A Spark job, which records no submitter: the Thrift Server is the submitter of all of them. */
    public static QueryInFlight of(SparkJob job) {
        return new QueryInFlight(
                "spark",
                job.id(),
                job.state(),
                job.runningS(),
                job.sql(),
                "",
                job.tasksDone(),
                job.tasksTotal());
    }
}
