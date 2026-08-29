package com.thelastpickle.htap.common;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DecimalStyle;

/**
 * The partition keys of the two demo tables that the sink writes and the dashboard reads.
 *
 * <p>{@code ingestion_counts} is keyed by a 30-minute window and {@code alerts_by_bucket} by
 * an hour, and in the Python each key was computed in more than one place: the 30-minute one in
 * the sink and again in the backend's Cassandra client, the hour one in the sink, in that client
 * and once more in its demo route, five copies of two functions. A writer and a reader that spell
 * a key differently agree on nothing and report no error, so the throughput chart or the alert
 * list simply reads empty. One copy here is what removes that.
 *
 * <p>{@code demo.events}'s own key is not here, in {@link EventPartitions}: its width is
 * configurable and its value carries a shard beside it, so it is a different function of a
 * different table's row rather than a third case of this one.
 *
 * <p>Both keys are UTC and both are ASCII digits whatever locale the JVM took; see the note in
 * {@link EventPartitions} for the mechanism and {@link BucketLocaleProbe} for the case that
 * makes it falsifiable.
 */
public final class BucketKeys {

    private static final DateTimeFormatter HOUR_FORMAT = DateTimeFormatter
            .ofPattern("uuuu-MM-dd'T'HH")
            .withDecimalStyle(DecimalStyle.STANDARD)
            .withZone(ZoneOffset.UTC);

    private BucketKeys() {}

    /** The hour partition of {@code alerts_by_bucket}, as {@code "YYYY-MM-DDTHH"}. */
    public static String hour(Instant at) {
        return HOUR_FORMAT.format(at);
    }

    /**
     * The 30-minute partition of {@code ingestion_counts}, as {@code "YYYY-MM-DDTHH:00"} or
     * {@code "…:30"}.
     *
     * <p>The two-character suffix is appended rather than formatted, because a
     * {@code mm} field would print the real minute. The Python did the same, and this is
     * the one place where its {@code f"{0 if t.minute < 30 else 30:02d}"} is worth reading
     * as a literal pair of strings instead.
     */
    public static String thirtyMinute(Instant at) {
        return HOUR_FORMAT.format(at) + (at.atOffset(ZoneOffset.UTC).getMinute() < 30 ? ":00" : ":30");
    }
}
