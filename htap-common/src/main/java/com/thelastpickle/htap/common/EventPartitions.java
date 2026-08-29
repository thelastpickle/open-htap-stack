package com.thelastpickle.htap.common;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DecimalStyle;
import java.util.UUID;
import java.util.zip.CRC32;

/**
 * Where a row of {@code demo.events} goes, whose primary key is
 * {@code ((event_bucket, shard), event_id)}.
 *
 * <p>Both figures are declared once in {@code podman-compose.yml} and read by the sink
 * and the backend alike, because the two disagreeing produces queries that match
 * nothing.
 */
public final class EventPartitions {

    // STANDARD restates ofPattern's own default rather than correcting it: on Zulu 25.0.2,
    // DateTimeFormatterBuilder.toFormatter loads DecimalStyle.STANDARD unconditionally, at
    // offset 46 under `javap -c`. What ofPattern does take from the FORMAT locale is the
    // formatter's locale, which decides text fields, and this pattern has none. The digits do
    // move for withDecimalStyle(DecimalStyle.of(locale)): under fa-IR that prints
    // 2026-01-01T12:45 in Persian digits, and a bucket in Persian digits matches no partition
    // any engine wrote. So this call is here to make that one line an edit rather than an
    // omission, and BucketLocaleProbe runs in a forked JVM under fa-IR because the field is
    // initialised once and an in-process test cannot set the locale before it.
    private static final DateTimeFormatter BUCKET_FORMAT =
            DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm").withDecimalStyle(DecimalStyle.STANDARD);

    private EventPartitions() {}

    /**
     * The window an event belongs to, as {@code "YYYY-MM-DDTHH:MM"} in UTC.
     *
     * <p>Text rather than a timestamp on purpose. This value is written by hand into
     * queries that four engines have to parse, and a quoted string means the same thing
     * in CQL, Presto SQL and Spark SQL where a timestamp literal does not. It also sorts
     * lexicographically, so a range predicate still reads naturally on the paths that
     * cannot prune on it.
     *
     * <p>The width must divide 60, and that is a refusal the Python it replaces did not
     * make. Only the minute within the hour is floored, so 90 would silently give hourly
     * windows and 7 would give a four-minute one from :56, and an operator supplies the
     * value through {@code DEMO_EVENT_BUCKET_MINUTES}. A window narrower than it claims
     * is worse than a rejected setting: the compare page names the width in its copy, and
     * whether a closed window can still grow is answered against the window's end.
     *
     * <p>The refusal also replaces the backend's {@code max(1, settings.event_bucket_minutes)},
     * deliberately, so a width of 0 fails at the caller rather than answering with a
     * one-minute window nobody asked for. The clamp was written twice in the Python
     * backend's query route, once where a bucket started and once where the window stepped
     * back over closed ones; both are gone rather than one. The sink's copy divides by the
     * width and so already raises, which makes the clamp the outlier of the three, and
     * whoever ports either site would otherwise read it as a tolerance to keep.
     */
    public static String bucket(Instant eventTime, int bucketMinutes) {
        // No separate upper bound: a width above 60 leaves a remainder of 60, so the
        // divisor test already refuses it.
        if (bucketMinutes < 1 || 60 % bucketMinutes != 0) {
            throw new IllegalArgumentException(
                    "bucketMinutes must be a divisor of 60, got " + bucketMinutes);
        }
        // The Instant parameter is what removes the hazard, and it is a deliberate
        // narrowing of the Python's signature: the sink's own event_bucket took an aware
        // datetime, so a caller there could hand it +05:30 and have the local minute floored.
        // An Instant carries no offset to floor by, so this conversion cannot pick the
        // wrong one; ZoneOffset.UTC is the schema's window boundary rather than a defence.
        OffsetDateTime utc = OffsetDateTime.ofInstant(eventTime, ZoneOffset.UTC);
        return utc.withMinute((utc.getMinute() / bucketMinutes) * bucketMinutes)
                .withSecond(0)
                .withNano(0)
                .format(BUCKET_FORMAT);
    }

    /**
     * Which partition of a bucket this event goes to.
     *
     * <p>Taken from the event's own id, so the spread is even whatever the fleet size:
     * deriving it from the asset would put every row of a one-asset demo in a single
     * partition.
     *
     * <p>Hashed rather than taken modulo the id itself, so that the spread does not depend
     * on which id source wrote the row, and two sources write them. Measured over 4,096 ids
     * of each on driver 3.30.1 in the running backend: {@code uuid.uuid1()}, which the Python
     * sink minted itself and again for an event whose id would not parse, draws one node for
     * the host and puts all 4,096 rows in one shard
     * under {@code id % 16}; the producer's {@code uuid_from_time} draws a node per call,
     * 4,096 distinct ones, and spreads over all 16 either way. Under crc32 both sources
     * spread over all 16, which is the property the schema needs and the reason to hash.
     *
     * <p>{@link CRC32} is zlib's crc32, which is what the Python sink used, so a stack
     * whose rows were written by either one is readable by either one.
     */
    public static int shard(UUID eventId, int shardCount) {
        if (shardCount < 1) {
            throw new IllegalArgumentException("shardCount must be at least 1, got " + shardCount);
        }
        CRC32 crc = new CRC32();
        crc.update(ByteBuffer.allocate(16)
                .putLong(eventId.getMostSignificantBits())
                .putLong(eventId.getLeastSignificantBits())
                .array());
        return (int) (crc.getValue() % shardCount);
    }
}
