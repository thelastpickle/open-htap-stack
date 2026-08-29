package com.thelastpickle.htap.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.time.Instant;
import java.time.format.DecimalStyle;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("an instant, spelled as every access path spells it")
class TimestampsTest {

    @Test
    @DisplayName("six fractional digits, or none")
    void theFractionIsSixDigitsOrAbsent() {
        assertEquals("2026-08-29T12:34:56", Timestamps.iso(Instant.parse("2026-08-29T12:34:56Z")));
        // The case LocalDateTime.toString() gets wrong: it gives three digits here and none
        // above, where Python's isoformat gives six and none.
        assertEquals(
                "2026-08-29T12:34:56.789000", Timestamps.iso(Instant.parse("2026-08-29T12:34:56.789Z")));
        assertEquals(
                "2026-08-29T12:34:56.000001", Timestamps.iso(Instant.parse("2026-08-29T12:34:56.000001Z")));
    }

    @Test
    @DisplayName("a nanosecond a Cassandra timestamp cannot hold is truncated, not rounded")
    void nanosecondsBelowAMicrosecondAreDropped() {
        // Unreachable through any of the five paths, all of which are millisecond precision;
        // pinned because truncation is what Python did and rounding would carry a value into
        // the next microsecond on one path only.
        assertEquals(
                "2026-08-29T12:34:56.000001", Timestamps.iso(Instant.parse("2026-08-29T12:34:56.000001999Z")));
    }

    @Test
    @DisplayName("an instant the backend mints carries the offset the Python's printed")
    void theOffsetFormAddsUtc() {
        assertEquals(
                "2026-08-29T12:34:56.789000+00:00",
                Timestamps.isoOffset(Instant.parse("2026-08-29T12:34:56.789Z")));
        assertEquals("2026-08-29T12:34:56+00:00", Timestamps.isoOffset(Instant.parse("2026-08-29T12:34:56Z")));
    }

    @Test
    @DisplayName("the fraction is ASCII digits under a locale that numbers otherwise")
    void theFractionDoesNotFollowTheFormatLocale() {
        // In process, and that is the point: the fraction is formatted per call rather than by
        // the class's formatter, so setting the locale here does reach it. It reached it as a
        // defect first, under the fa-IR fork in EventPartitionsTest: the seconds field printed
        // 12:45:00 and the fraction .۷۸۹۰۰۰ in one string. Both cases stay, because they fail
        // for different reasons and a later change could break either alone.
        List<Locale> numberingOtherwise = Arrays.stream(Locale.getAvailableLocales())
                .filter(locale -> DecimalStyle.of(locale).getZeroDigit() != '0')
                .toList();
        assertFalse(numberingOtherwise.isEmpty(), "this JDK numbers every locale in ASCII");
        Locale saved = Locale.getDefault(Locale.Category.FORMAT);
        try {
            for (Locale locale : numberingOtherwise) {
                Locale.setDefault(Locale.Category.FORMAT, locale);
                assertEquals("2026-08-29T12:34:56.789000",
                        Timestamps.iso(Instant.parse("2026-08-29T12:34:56.789Z")),
                        "under " + locale.toLanguageTag());
            }
        } finally {
            Locale.setDefault(Locale.Category.FORMAT, saved);
        }
    }

    @Test
    @DisplayName("an instant before 1970 keeps its calendar date")
    void anInstantBeforeTheEpochIsNotShiftedByTheFloor() {
        // The arithmetic that goes wrong when a negative nano-of-second is divided rather
        // than taken from a normalised LocalDateTime.
        assertEquals("1969-12-31T23:59:59.999000", Timestamps.iso(Instant.ofEpochMilli(-1L)));
    }
}
