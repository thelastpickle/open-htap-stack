package com.thelastpickle.htap.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DecimalStyle;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The bucket strings and shard numbers here were produced by running the Python this
 * class replaces: {@code event_bucket} and {@code event_shard} in
 * {@code ingress/consumer/consumer.py}. They are exact, not approximate, because a
 * disagreement of one shard is a query that matches nothing.
 */
class EventPartitionsTest {

    private static Instant at(String iso) {
        return OffsetDateTime.parse(iso).toInstant();
    }

    @Test
    void floorsToTheFifteenMinuteWindow() {
        assertEquals("2026-08-28T14:00", EventPartitions.bucket(at("2026-08-28T14:00:00Z"), 15));
        assertEquals("2026-08-28T14:00", EventPartitions.bucket(at("2026-08-28T14:14:59.999999Z"), 15));
        assertEquals("2026-08-28T14:15", EventPartitions.bucket(at("2026-08-28T14:15:00Z"), 15));
        assertEquals("2026-08-28T14:45", EventPartitions.bucket(at("2026-08-28T14:59:59Z"), 15));
        assertEquals("2026-01-01T00:00", EventPartitions.bucket(at("2026-01-01T00:00:00Z"), 15));
    }

    @Test
    void honoursOtherWindowWidths() {
        assertEquals("2026-08-28T14:10", EventPartitions.bucket(at("2026-08-28T14:14:59.999999Z"), 5));
        assertEquals("2026-08-28T14:55", EventPartitions.bucket(at("2026-08-28T14:59:59Z"), 5));
        assertEquals("2026-08-28T14:00", EventPartitions.bucket(at("2026-08-28T14:59:59Z"), 60));
    }

    @Test
    @DisplayName("an offset-carrying literal is normalised to UTC before the window is chosen")
    void normalisesAnOffsetToUtc() {
        // 19:44+05:30 is 14:14 UTC, so the window is 14:00. What this pins is the
        // normalisation and not a hazard the code could meet: the parameter is an Instant,
        // which carries no offset to floor by, so no implementation of this signature can
        // answer 14:30 the way the Python's aware-datetime input could. Kept because the
        // Python is what a reader compares against, and because a later widening of the
        // parameter to a ZonedDateTime would have to keep this answer.
        assertEquals("2026-08-28T14:00", EventPartitions.bucket(at("2026-08-28T19:44:00+05:30"), 15));
        assertEquals("2026-08-28T14:10", EventPartitions.bucket(at("2026-08-28T19:44:00+05:30"), 5));
    }

    @Test
    void aWindowWidthBelowOneMinuteIsRefused() {
        assertThrows(IllegalArgumentException.class, () -> EventPartitions.bucket(Instant.EPOCH, 0));
        assertThrows(IllegalArgumentException.class, () -> EventPartitions.bucket(Instant.EPOCH, -15));
    }

    @Test
    @DisplayName("a width that does not divide 60 is refused, since only the minute is floored")
    void aWindowWidthThatDoesNotDivideSixtyIsRefused() {
        // 90 would give hourly windows and 7 a four-minute one from :56, both silently.
        for (int width : new int[] {7, 11, 90, 120}) {
            assertThrows(IllegalArgumentException.class,
                    () -> EventPartitions.bucket(Instant.EPOCH, width), "accepted " + width);
        }
        for (int width : new int[] {1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30, 60}) {
            String value = EventPartitions.bucket(at("2026-08-28T14:47:31Z"), width);
            int minute = Integer.parseInt(value.substring(value.length() - 2));
            assertEquals(0, minute % width, value + " is not on a " + width + "-minute boundary");
            assertTrue(minute <= 47 && 47 - minute < width,
                    value + " is not the " + width + "-minute window 14:47 falls in");
        }
    }

    @Test
    @DisplayName("the bucket is ASCII digits under a JVM whose own locale numbers otherwise")
    void theBucketIsAsciiUnderAJvmThatNumbersInItsOwnScript() throws Exception {
        // The falsifiable form of the case below, and it needs a second JVM. EventPartitions
        // initialises its formatter once and a DateTimeFormatter keeps the DecimalStyle it was
        // built with, so the locale that decides the digits is the one in force at class
        // initialisation; a test that sets the locale in this JVM has almost certainly missed
        // that moment. Here fa-IR arrives on the command line, which is how a container gets
        // its locale, and the probe loads the class afterwards. Measured: the same field
        // written as withDecimalStyle(DecimalStyle.of(Locale.getDefault(FORMAT))) prints
        // 2026-01-01T12:45 in Persian digits in this JVM and passes the case below.
        Process probe = new ProcessBuilder(
                        Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                        "-Duser.language=fa", "-Duser.country=IR",
                        "-cp", System.getProperty("java.class.path"),
                        BucketLocaleProbe.class.getName())
                .redirectErrorStream(true)
                .start();
        String printed = new String(probe.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, probe.waitFor(), "the probe JVM failed:\n" + printed);
        Map<String, String> lines = printed.lines()
                .filter(line -> line.contains("="))
                .collect(Collectors.toMap(line -> line.substring(0, line.indexOf('=')),
                        line -> line.substring(line.indexOf('=') + 1)));
        // The premise: a JVM that took the locale and numbers in ASCII anyway would make the
        // assertion below pass while testing nothing. U+06F0 is the Persian zero.
        assertEquals("fa-IR", lines.get("locale"), printed);
        assertEquals(String.valueOf(0x06F0), lines.get("zeroDigit"), printed);
        assertEquals("2026-01-01T12:45", lines.get("bucket"), printed);
    }

    @Test
    @DisplayName("a locale changed after the class loads cannot reach the formatter")
    void theBucketIsAsciiWhateverTheFormatLocaleBecomes() {
        // Weaker than the fork above and it says so: by the time this runs the formatter
        // exists, so what these locales pin is that no later step of bucket() consults the
        // FORMAT locale. Derived from the JDK's own data rather than listed, because a list
        // of tags is a snapshot of the JDK it was written against and says nothing about the
        // next one; the filter is also the premise, since a locale numbering in ASCII would
        // pass while exercising nothing. On Zulu 25.0.2 it selects 99 of the 1,158 available.
        List<Locale> numberingOtherwise = Arrays.stream(Locale.getAvailableLocales())
                .filter(locale -> DecimalStyle.of(locale).getZeroDigit() != '0')
                .toList();
        assertFalse(numberingOtherwise.isEmpty(), "this JDK numbers every locale in ASCII");
        Locale saved = Locale.getDefault(Locale.Category.FORMAT);
        try {
            for (Locale locale : numberingOtherwise) {
                Locale.setDefault(Locale.Category.FORMAT, locale);
                assertEquals("2026-01-01T12:45",
                        EventPartitions.bucket(at("2026-01-01T12:45:00Z"), 15),
                        "under " + locale.toLanguageTag());
            }
        } finally {
            Locale.setDefault(Locale.Category.FORMAT, saved);
        }
    }

    @Test
    void shardsExactlyAsZlibCrc32Does() {
        assertEquals(7, EventPartitions.shard(UUID.fromString("00000000-0000-1000-8000-000000000000"), 16));
        assertEquals(7, EventPartitions.shard(UUID.fromString("0d5c2f8a-8b7e-11f0-9c3d-6a5b4c3d2e1f"), 16));
        assertEquals(6, EventPartitions.shard(UUID.fromString("ffffffff-ffff-1fff-bfff-ffffffffffff"), 16));
        assertEquals(0, EventPartitions.shard(UUID.fromString("123e4567-e89b-12d3-a456-426614174000"), 16));
    }

    @Test
    @DisplayName("the shard count divides the same hash, so a different count is not a different hash")
    void shardCountOnlyChangesTheModulus() {
        assertEquals(2, EventPartitions.shard(UUID.fromString("00000000-0000-1000-8000-000000000000"), 3));
        assertEquals(0, EventPartitions.shard(UUID.fromString("0d5c2f8a-8b7e-11f0-9c3d-6a5b4c3d2e1f"), 3));
        assertEquals(2, EventPartitions.shard(UUID.fromString("ffffffff-ffff-1fff-bfff-ffffffffffff"), 3));
        assertEquals(1, EventPartitions.shard(UUID.fromString("123e4567-e89b-12d3-a456-426614174000"), 3));
    }

    @Test
    void everyShardIsInRangeAndNoneIsEmpty() {
        Set<Integer> seen = new HashSet<>();
        for (int i = 0; i < 20_000; i++) {
            int shard = EventPartitions.shard(TimeUuids.timeUuid(Instant.EPOCH), 16);
            assertTrue(shard >= 0 && shard < 16, "shard out of range: " + shard);
            seen.add(shard);
        }
        assertEquals(16, seen.size(), "20,000 events did not reach all 16 shards");
    }

    @Test
    @DisplayName("the id is hashed rather than taken modulo, which measured as one shard")
    void hashingIsWhatSpreadsVersionOneUuids() {
        // The low bits of a version-1 UUID are the node field, and one id source draws one
        // node for the host: `uuid.uuid1()`, which the sink mints at consumer.py:911 and
        // :1052, put all 4,096 of a measured batch in one shard under `id % 16`. A fixed
        // node here is what reproduces that source; the producer's `uuid_from_time` draws a
        // node per call and would have spread on its own, so the modulo's answer depends on
        // which source wrote the row and the hash is what removes the dependency.
        Set<Long> lowBitsModulus = new HashSet<>();
        Set<Integer> hashed = new HashSet<>();
        for (int i = 0; i < 1_000; i++) {
            UUID id = TimeUuids.timeUuid(Instant.EPOCH.plusNanos(i * 1_000L), 0x0abc, 0x010203040506L);
            lowBitsModulus.add(Math.floorMod(id.getLeastSignificantBits(), 16L));
            hashed.add(EventPartitions.shard(id, 16));
        }
        assertEquals(1, lowBitsModulus.size());
        assertEquals(16, hashed.size());

        // And the drawn-node source, which is what the producer uses: the hash spreads it
        // too, so the shard is the same function of the id whichever source minted it.
        Set<Integer> drawnNode = new HashSet<>();
        for (int i = 0; i < 1_000; i++) {
            drawnNode.add(EventPartitions.shard(TimeUuids.timeUuid(Instant.EPOCH), 16));
        }
        assertEquals(16, drawnNode.size());
    }

    @Test
    void aShardCountBelowOneIsRefused() {
        UUID id = UUID.fromString("00000000-0000-1000-8000-000000000000");
        assertThrows(IllegalArgumentException.class, () -> EventPartitions.shard(id, 0));
    }
}
