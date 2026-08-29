package com.thelastpickle.htap.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The five expected UUIDs were produced by the driver the Python services actually run,
 * {@code cassandra.util.uuid_from_time}, executed inside the running backend container
 * with the clock sequence and node fixed so the whole value is comparable. That is what
 * makes the interval arithmetic checkable rather than merely plausible.
 */
class TimeUuidsTest {

    /** A fleet's worth: the largest the dashboard may ask for, and a batch at the demo's rate. */
    private static final int A_FLEET = 2000;

    private static final int THREADS = 8;

    private static final int CLOCK_SEQ = 0x0abc;
    private static final long NODE = 0x010203040506L;

    /**
     * Truncating, not rounding, because {@code uuid_from_time} does {@code int(time_arg *
     * 1e6)}. Measured on the driver the stack runs: 1767225600.1234567 gives
     * ...123456 micros where rounding would give ...123457.
     */
    private static Instant fromSeconds(double epochSeconds) {
        long micros = (long) (epochSeconds * 1_000_000d);
        return Instant.ofEpochSecond(
                Math.floorDiv(micros, 1_000_000L), Math.floorMod(micros, 1_000_000L) * 1_000L);
    }

    @Test
    void mintsWhatTheCassandraDriverMints() {
        assertEquals(UUID.fromString("13814000-1dd2-11b2-8abc-010203040506"),
                TimeUuids.timeUuid(fromSeconds(0.0), CLOCK_SEQ, NODE));
        assertEquals(UUID.fromString("1419d680-1dd2-11b2-8abc-010203040506"),
                TimeUuids.timeUuid(fromSeconds(1.0), CLOCK_SEQ, NODE));
        assertEquals(UUID.fromString("d0c3c000-e6a4-11f0-8abc-010203040506"),
                TimeUuids.timeUuid(fromSeconds(1767225600.0), CLOCK_SEQ, NODE));
        assertEquals(UUID.fromString("d0d69680-e6a4-11f0-8abc-010203040506"),
                TimeUuids.timeUuid(fromSeconds(1767225600.123456), CLOCK_SEQ, NODE));
        assertEquals(UUID.fromString("cbad4b40-d3b1-11f1-8abc-010203040506"),
                TimeUuids.timeUuid(fromSeconds(1793289600.5), CLOCK_SEQ, NODE));
        // The discriminating case: the driver truncates the microsecond, so this instant
        // mints the same UUID as 1767225600.123456 above. Rounding would carry it to
        // ...123457 and a different value entirely.
        assertEquals(UUID.fromString("d0d69680-e6a4-11f0-8abc-010203040506"),
                TimeUuids.timeUuid(fromSeconds(1767225600.1234567), CLOCK_SEQ, NODE));
    }

    @Test
    void isAVersionOneUuidWithTheRfcVariant() {
        UUID id = TimeUuids.timeUuid(Instant.now());
        assertEquals(1, id.version());
        assertEquals(2, id.variant());
    }

    @Test
    @DisplayName("the sink derives event_time from event_id, so the microsecond must survive")
    void readsBackTheInstantItWasStampedAt() {
        for (double seconds : new double[] {0.0, 1.0, 1767225600.0, 1767225600.123456, 1793289600.5}) {
            Instant stamped = fromSeconds(seconds);
            assertEquals(stamped, TimeUuids.instantOf(TimeUuids.timeUuid(stamped, CLOCK_SEQ, NODE)),
                    "round trip failed at " + seconds);
        }
    }

    @Test
    @DisplayName("a count that is no whole microsecond reads back at 100-nanosecond resolution")
    void readsBackTheFullHundredNanosecondResolution() {
        // The reference: uuid_from_time(1700000000.123456) with these fields bound, run on
        // driver 3.30.1 in the running backend, whose 60-bit count is the value below.
        long base = 139_192_928_001_234_560L;
        assertEquals(UUID.fromString("04c29680-833b-11ee-9234-0123456789ab"),
                withIntervalCount(base), "the value the remainders below are measured from");

        // A mint always lands on a multiple of ten, so no UUID this class makes exercises the
        // last digit; Cassandra's own now() is not so bounded, and these are the nine counts
        // above the reference that instantOf must read and timeUuid cannot produce.
        for (int remainder = 0; remainder < 10; remainder++) {
            assertEquals(Instant.ofEpochSecond(1_700_000_000L, 123_456_000 + remainder * 100),
                    TimeUuids.instantOf(withIntervalCount(base + remainder)),
                    "remainder " + remainder);
        }

        // And the divergence that leaves, recorded rather than incidental. The Python driver
        // answers .123456 for the first six of those counts and .123457 for the last four:
        // unix_time_from_uuid1 divides the count by 1e7 as a double, whose resolution at
        // 1.7e9 seconds is coarser than 100 ns, and datetime_from_timestamp then rounds to
        // the microsecond, so the ten instants collapse into four doubles and then into two
        // answers. This assertion is the one that would fail if instantOf were made to
        // emulate that, which was measured and rejected: the Java is exact.
        assertEquals(Instant.ofEpochSecond(1_700_000_000L, 123_456_900),
                TimeUuids.instantOf(withIntervalCount(base + 9)),
                "the Python's .123457 is its arithmetic, not a decision to match");
    }

    /**
     * A version-1 UUID carrying {@code intervals} exactly, which the mint cannot make. The
     * clock sequence and node are those of the driver run the reference value came from, not
     * the pair the rest of this class binds.
     */
    private static UUID withIntervalCount(long intervals) {
        return TimeUuids.uuidFromIntervals(intervals, 0x1234, 0x0123456789ABL);
    }

    @Test
    void refusesToReadANonVersionOneUuid() {
        assertThrows(UnsupportedOperationException.class, () -> TimeUuids.instantOf(UUID.randomUUID()));
    }

    @Test
    void refusesAClockSequenceOrNodeThatWillNotFit() {
        Instant now = Instant.now();
        assertThrows(IllegalArgumentException.class, () -> TimeUuids.timeUuid(now, 0x4000, NODE));
        assertThrows(IllegalArgumentException.class, () -> TimeUuids.timeUuid(now, -1, NODE));
        assertThrows(IllegalArgumentException.class, () -> TimeUuids.timeUuid(now, CLOCK_SEQ, 1L << 48));
        assertThrows(IllegalArgumentException.class, () -> TimeUuids.timeUuid(now, CLOCK_SEQ, -1L));
    }

    @Test
    @DisplayName("an instant the 60-bit field cannot hold is refused, where the Python wraps")
    void refusesAnInstantOutsideTheVersionOneRange() {
        // 1582-10-15 and 5236-03-31T21:21:00.684697Z, the two ends, both accepted.
        Instant first = Instant.ofEpochSecond(-12_219_292_800L);
        Instant last = Instant.ofEpochSecond(103_072_857_660L, 684_697_000);
        assertEquals(0L, TimeUuids.timeUuid(first, CLOCK_SEQ, NODE).timestamp());
        // 2^60 - 6 is the top a mint can reach, six below the field's own, because the
        // count moves in steps of ten: the mint carries microseconds, so nine of every ten
        // values of the field are unreachable by construction.
        assertEquals((1L << 60) - 6, TimeUuids.timeUuid(last, CLOCK_SEQ, NODE).timestamp());

        // One microsecond past each, which is where the field wraps.
        assertThrows(IllegalArgumentException.class,
                () -> TimeUuids.timeUuid(first.minusNanos(1_000L), CLOCK_SEQ, NODE));
        assertThrows(IllegalArgumentException.class,
                () -> TimeUuids.timeUuid(last.plusNanos(1_000L), CLOCK_SEQ, NODE));

        // The port slip this is for: a millisecond value passed where seconds were wanted.
        // `uuid_from_time(1767225600000.0)` does not raise, because it masks the count into
        // three fields that are each then in range; measured on the driver in the running
        // backend, it answers 5f454000-9505-16f2-… whose count of 500626358897295360 reads
        // back as 3169-03-17T15:44:49.729536Z. That last figure is from integer arithmetic
        // and not from `datetime.fromtimestamp(micros / 1e6)`, which answers .729538: a
        // double's 53-bit mantissa cannot hold microseconds at 3.8e16, and two reviews
        // disagreed on the digit for exactly that reason.
        assertThrows(IllegalArgumentException.class,
                () -> TimeUuids.timeUuid(Instant.ofEpochSecond(1767225600000L), CLOCK_SEQ, NODE));

        // Instant's own extremes, which overflow the multiplication rather than the field.
        assertThrows(IllegalArgumentException.class,
                () -> TimeUuids.timeUuid(Instant.MIN, CLOCK_SEQ, NODE));
        assertThrows(IllegalArgumentException.class,
                () -> TimeUuids.timeUuid(Instant.MAX, CLOCK_SEQ, NODE));
    }

    @Test
    @DisplayName("a fixed clock sequence and node lose a row, which is why these are random")
    void aFixedClockSequenceAndNodeCollideWithinTheSameMicrosecond() {
        Instant instant = Instant.ofEpochSecond(1767225600L, 123_456_000);
        assertEquals(TimeUuids.timeUuid(instant, 0, 0), TimeUuids.timeUuid(instant, 0, 0));

        // Which is a lost row and not a duplicate one, because event_id is the clustering
        // column of demo.events. The driver's Uuids.startOf(millis) behaves this way, and
        // it is why this class exists rather than calling it.
        assertNotEquals(TimeUuids.timeUuid(instant), TimeUuids.timeUuid(instant));
    }

    @Test
    @DisplayName("a million mints at one instant stay distinct")
    void randomFieldsKeepAMillionMintsApart() {
        // The producer stamps a batch 0.5 ms apart at 2,000 events a second, so the
        // interval count alone would separate them. This is the harder case: every mint
        // at the same microsecond, so only the 62 random bits can tell them apart. A
        // collision among a million draws over 2^62 has probability about 1.1e-7.
        assertEquals(1_000_000, mintedAt(Instant.ofEpochSecond(1767225600L), 0, 1_000_000).size());
    }

    @Test
    @DisplayName("at the producer's 500 µs cadence the interval count alone separates the mints")
    void theIntervalCountAdvancesWithTheCadence() {
        // Ten thousand rather than a million, because at 500 µs apart every count differs
        // and distinctness holds whatever the random fields do; the million above is the
        // case where only those fields can separate the mints. What this checks is that
        // the count advances with the instant, so it is sized for that and not for a
        // collision probability.
        assertEquals(10_000, mintedAt(Instant.ofEpochSecond(1767225600L), 500, 10_000).size());
    }

    @Test
    @DisplayName("the node's multicast bit is drawn, not forced, as uuid_from_time draws it")
    void theMulticastBitIsLeftAsDrawn() {
        // Bit 40 of the node is the least significant bit of its first octet, which RFC 9562
        // names the multicast bit and would have set for a node that is no hardware address.
        // uuid_from_time forces nothing: 1,994 of 4,000 draws had it set, measured on driver
        // 3.30.1 in the running backend. Every reference assertion above binds the node, so
        // this is the only test that would fail on a mint that forced the bit.
        Set<Boolean> seen = new HashSet<>();
        for (int i = 0; i < 1_000; i++) {
            seen.add((TimeUuids.timeUuid(Instant.EPOCH).getLeastSignificantBits() & 1L << 40) != 0);
        }
        assertEquals(Set.of(true, false), seen);
    }

    /**
     * Sixteen thousand identifiers minted at once are all distinct.
     *
     * <p>Eight threads of a fleet each, every one of them stamped at the same instant, which is the
     * case the driver's own {@code Uuids.startOf} fails: it fixes the clock sequence and the node,
     * so every mint within one millisecond is identical. Here the two are drawn per call, which
     * leaves 62 bits of difference between two events of the same microsecond.
     */
    @Test
    @Timeout(30)
    void everyIdentifierIsDistinctAcrossThreads() throws InterruptedException {
        Instant sameInstant = Instant.parse("2026-08-27T15:55:33.000500Z");
        ConcurrentLinkedQueue<UUID> minted = new ConcurrentLinkedQueue<>();
        CountDownLatch ready = new CountDownLatch(THREADS);
        CountDownLatch go = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(THREADS);

        for (int thread = 0; thread < THREADS; thread++) {
            Thread.ofPlatform().start(() -> {
                ready.countDown();
                try {
                    go.await();
                    for (int i = 0; i < A_FLEET; i++) {
                        minted.add(TimeUuids.timeUuid(sameInstant));
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        assertTrue(ready.await(10, TimeUnit.SECONDS));
        go.countDown();
        assertTrue(done.await(20, TimeUnit.SECONDS));

        assertEquals(THREADS * A_FLEET, minted.size());
        assertEquals(
                THREADS * A_FLEET,
                new HashSet<>(minted).size(),
                "two of " + THREADS * A_FLEET + " identifiers were the same");
    }

    private static Set<UUID> mintedAt(Instant start, long stepMicros, int count) {
        Set<UUID> minted = new HashSet<>(count * 2);
        for (int i = 0; i < count; i++) {
            minted.add(TimeUuids.timeUuid(start.plusNanos(i * stepMicros * 1_000L)));
        }
        return minted;
    }
}
