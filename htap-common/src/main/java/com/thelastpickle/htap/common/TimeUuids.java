package com.thelastpickle.htap.common;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Version-1 UUIDs, minted and read back.
 *
 * <p>{@code demo.events} is keyed on {@code ((event_bucket, shard), event_id)} and the
 * sink derives {@code event_time} from {@code event_id}, so two events sharing a UUID are
 * not a duplicate row but a lost one. The driver's own {@code Uuids.startOf(millis)} fixes
 * the clock sequence and the node to constants, which makes every event minted within the
 * same millisecond identical; at 2,000 events a second the producer stamps them 0.5 ms
 * apart. So this mints its own, matching {@code cassandra.util.uuid_from_time}: the same
 * 100-nanosecond interval count, and a random clock sequence and node.
 */
public final class TimeUuids {

    /** 100-nanosecond intervals between 1582-10-15 00:00:00 UTC and the Unix epoch. */
    private static final long GREGORIAN_OFFSET_100NS = 0x01b21dd213814000L;

    private static final long MICROS_PER_SECOND = 1_000_000L;
    private static final long NANOS_PER_MICRO = 1_000L;
    private static final long INTERVALS_PER_SECOND = 10_000_000L;
    private static final long NANOS_PER_INTERVAL = 100L;

    /**
     * The first epoch second the 60-bit timestamp field reaches, 1582-10-15, where the
     * interval count is exactly 0. Every instant in it is representable, which is why
     * {@link #timeUuid} tests this bound and no more at the bottom.
     */
    private static final long FIRST_EPOCH_SECOND = -12_219_292_800L;

    /**
     * The last epoch second the field reaches at all, 5236-03-31T21:21:00Z.
     *
     * <p>It bounds the second alone and not the instant: the field stops partway through
     * this second, at .684697Z, where the count is 2^60 - 6. So {@link #timeUuid} tests the
     * count again after computing it, and the two checks are not redundant. Testing the
     * second first is what keeps that multiplication inside {@code long}, which it leaves
     * well within {@link Instant}'s own range.
     *
     * <p>Six short of the field's top rather than at it because the mint carries
     * microseconds: both terms of the count are multiples of ten, so no reachable value ends
     * in anything but a zero.
     */
    private static final long LAST_EPOCH_SECOND = 103_072_857_660L;

    private TimeUuids() {}

    /** A version-1 UUID stamped at {@code instant}, with a random clock sequence and node. */
    public static UUID timeUuid(Instant instant) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        // All 48 node bits are left as drawn, the multicast bit included, which RFC 9562
        // would have set for a node that is not a hardware address. Deliberate: the
        // Python this replaces takes random.getrandbits(48) and forces nothing, measured
        // in the running backend on driver 3.30.1 as 1,994 of 4,000 draws with bit 40 of
        // the node set, that being the bit the RFC names. Forcing it here would mint
        // values the Python could not, and TimeUuidsTest holds the property.
        return timeUuid(instant, random.nextInt(1 << 14), random.nextLong() & 0xFFFFFFFFFFFFL);
    }

    /**
     * A version-1 UUID with the clock sequence and node given rather than drawn, which is
     * what makes the interval arithmetic testable against the Python driver's output.
     *
     * @param clockSeq 14 bits
     * @param node 48 bits
     * @throws IllegalArgumentException if the instant is outside the range a 60-bit
     *     timestamp holds. The Python does not refuse it: {@code uuid_from_time} masks the
     *     count into three fields, each of which is then in range, so a millisecond value
     *     passed where seconds were wanted mints a well-formed UUID carrying a different
     *     time. That is worse than a refusal here, because {@link EventPartitions#bucket}
     *     files the row under the instant given while the sink derives {@code event_time}
     *     from the id, and nothing reports the disagreement.
     */
    public static UUID timeUuid(Instant instant, int clockSeq, long node) {
        if (clockSeq < 0 || clockSeq > 0x3FFF) {
            throw new IllegalArgumentException("clockSeq needs a 14-bit value, got " + clockSeq);
        }
        if (node < 0 || node > 0xFFFFFFFFFFFFL) {
            throw new IllegalArgumentException("node needs a 48-bit value, got " + node);
        }
        if (instant.getEpochSecond() < FIRST_EPOCH_SECOND
                || instant.getEpochSecond() > LAST_EPOCH_SECOND) {
            throw new IllegalArgumentException(outOfRange(instant));
        }
        long intervals = micros(instant) * 10 + GREGORIAN_OFFSET_100NS;
        // The second check LAST_EPOCH_SECOND describes: that bound admits the whole of the
        // last second and the field holds only part of it.
        if (intervals >= 1L << 60) {
            throw new IllegalArgumentException(outOfRange(instant));
        }

        return uuidFromIntervals(intervals, clockSeq, node);
    }

    /**
     * The version-1 layout itself: the low 32 bits of the interval count, then the middle 16,
     * then the version nibble above the high 12, and the variant bits above the clock
     * sequence.
     *
     * <p>Package-private and unchecked, where {@link #timeUuid} bounds the count first.
     * {@code TimeUuidsTest} needs counts a mint cannot reach, those that are no whole
     * microsecond, and calls this rather than writing the packing a second time.
     */
    static UUID uuidFromIntervals(long intervals, int clockSeq, long node) {
        long msb = (intervals & 0xFFFFFFFFL) << 32
                | (intervals >>> 32 & 0xFFFFL) << 16
                | 0x1000L | (intervals >>> 48 & 0x0FFFL);
        long lsb = (long) (0x80 | (clockSeq >> 8 & 0x3F)) << 56
                | (long) (clockSeq & 0xFF) << 48
                | node;
        return new UUID(msb, lsb);
    }

    /**
     * The instant a version-1 UUID was stamped at, at the full 100-nanosecond resolution the
     * field carries.
     *
     * <p>{@link #timeUuid} mints on a microsecond, so a UUID this class minted reads back
     * with its last two digits zero and the resolution is unobservable. A UUID minted
     * elsewhere is where it shows, and Cassandra's own {@code now()} is such a mint.
     *
     * <p>This is exact where the Python driver is not, and the divergence is at most one
     * microsecond. Measured against driver 3.30.1 in the running backend: {@code
     * unix_time_from_uuid1} divides the interval count by 1e7 as a double, whose resolution
     * at 1.7e9 seconds is coarser than 100 ns, and {@code datetime_from_timestamp} then
     * rounds what comes out to the microsecond. So over the ten counts from
     * 139192928001234560 upward, ten distinct instants collapse into four doubles and then
     * into two answers, .123456 for the first six and .123457 for the last four, where this
     * method gives all ten. Emulating that was rejected: the Java is exact, the Python's
     * answer is an artefact of its own arithmetic rather than a decision, and
     * {@code TimeUuidsTest} holds the divergence so it is recorded rather than incidental.
     *
     * @throws UnsupportedOperationException if the UUID is not version 1
     */
    public static Instant instantOf(UUID uuid) {
        long intervals = uuid.timestamp() - GREGORIAN_OFFSET_100NS;
        return Instant.ofEpochSecond(
                Math.floorDiv(intervals, INTERVALS_PER_SECOND),
                Math.floorMod(intervals, INTERVALS_PER_SECOND) * NANOS_PER_INTERVAL);
    }

    private static String outOfRange(Instant instant) {
        return "instant is outside the version-1 timestamp range,"
                + " 1582-10-15T00:00:00Z to 5236-03-31T21:21:00.684697Z: " + instant;
    }

    private static long micros(Instant instant) {
        return instant.getEpochSecond() * MICROS_PER_SECOND + instant.getNano() / NANOS_PER_MICRO;
    }
}
