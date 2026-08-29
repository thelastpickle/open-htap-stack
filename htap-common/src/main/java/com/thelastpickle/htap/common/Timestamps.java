package com.thelastpickle.htap.common;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DecimalStyle;
import java.util.Locale;

/**
 * How every access path spells an instant, so that a row-for-row comparison of the five
 * is about the data rather than about who formatted it.
 *
 * <p>Python's {@code datetime.isoformat} is the spelling to match, because the dashboard
 * and its copy were written against it: six fractional digits or none, never the three
 * that {@link LocalDateTime#toString()} gives a millisecond value. The frontend reads
 * these strings by index in places, {@code alert_time.slice(11, 19)} for one, so the
 * width of the field ahead of a digit is part of the contract.
 *
 * <p>Two spellings and not one, and which applies depends on where the value came from.
 * A Cassandra {@code timestamp} column arrives with no offset, because the Python driver
 * handed the route a naive datetime; an instant the backend mints itself carries
 * {@code +00:00}, because that route called {@code datetime.now(timezone.utc)}.
 */
public final class Timestamps {

    /** Seconds precision, with the fraction appended separately; see {@link #iso}. */
    private static final DateTimeFormatter TO_SECONDS =
            DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss").withDecimalStyle(DecimalStyle.STANDARD);

    private Timestamps() {}

    /**
     * A stored timestamp, in UTC and with no offset: {@code 2026-08-29T12:34:56.789000}.
     *
     * <p>Truncated to microseconds rather than rounded, which is what Python's
     * {@code isoformat} does and what no value in this demo reaches: Cassandra's
     * {@code timestamp} is millisecond precision on every path.
     */
    public static String iso(Instant at) {
        LocalDateTime utc = LocalDateTime.ofInstant(at, ZoneOffset.UTC);
        int micros = utc.getNano() / 1000;
        String text = utc.format(TO_SECONDS);
        // Locale.ROOT, and measured: String.formatted reads the FORMAT locale on every call,
        // where the formatter above took its DecimalStyle once. Under -Duser.language=fa the
        // seconds field printed 12:45:00 and the fraction printed .۷۸۹۰۰۰ in the same string.
        return micros == 0 ? text : text + "." + String.format(Locale.ROOT, "%06d", micros);
    }

    /** The same instant with the offset Python's aware {@code isoformat} prints. */
    public static String isoOffset(Instant at) {
        return iso(at) + "+00:00";
    }
}
