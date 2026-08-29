package com.thelastpickle.htap.backend.engine;

import com.thelastpickle.htap.backend.support.Round;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;

/**
 * How long ago an engine says it started something.
 *
 * <p>Shared by the two engines that report a start rather than an age: Presto formats
 * {@code queryStats.elapsedTime} for people ("17.44m"), which would have to be parsed back, and
 * Spark reports no age at all. One rule for both, because the running-work page sorts the two
 * kinds of work into one list.
 */
final class Stamps {

    private Stamps() {}

    /**
     * The seconds since the stamp, rounded as the page shows it, and 0 for anything unreadable.
     *
     * <p>Spark stamps a job "2026-08-17T12:01:52.041GMT", which is ISO-8601 with a zone
     * abbreviation where an offset belongs, so the suffix is swapped before parsing; a Presto stamp
     * carries a real offset and passes through the swap untouched. An unparseable stamp gives 0
     * rather than hiding the query or the job, which is the point of reading this page at all.
     */
    static double ageS(String stamp, Instant now) {
        if (stamp == null || stamp.isBlank()) {
            return 0.0;
        }
        try {
            Instant started = OffsetDateTime.parse(stamp.replace("GMT", "+00:00")).toInstant();
            return Round.tenth(Math.max(0.0, Duration.between(started, now).toNanos() / 1e9));
        } catch (DateTimeParseException e) {
            return 0.0;
        }
    }
}
