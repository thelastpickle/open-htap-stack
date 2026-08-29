package com.thelastpickle.htap.common;

import java.time.Instant;
import java.time.format.DecimalStyle;
import java.util.Locale;

/**
 * Prints the four strings this module formats from an instant, to be run in a forked JVM by
 * {@link EventPartitionsTest#theBucketIsAsciiUnderAJvmThatNumbersInItsOwnScript()}.
 *
 * <p>A separate JVM is what makes that case falsifiable. {@code EventPartitions},
 * {@code Timestamps} and {@code BucketKeys} each initialise a formatter once, and a
 * {@code DateTimeFormatter} keeps the {@link DecimalStyle} it was built with, so an in-process
 * test that sets the FORMAT locale is setting it after the decision has been made. Here the
 * locale arrives on the command line and none of the three has yet loaded.
 *
 * <p>All three are probed in this one fork, since a fork costs a JVM start and the classes
 * share the hazard rather than each having its own.
 *
 * <p>The zero digit is printed beside the bucket so a failure says which of the two went
 * wrong: a locale the JVM did not take, or a bucket that followed it.
 */
final class BucketLocaleProbe {

    private BucketLocaleProbe() {}

    public static void main(String[] args) {
        Instant at = Instant.ofEpochSecond(1767271500L);
        Locale format = Locale.getDefault(Locale.Category.FORMAT);
        System.out.println("locale=" + format.toLanguageTag());
        System.out.println("zeroDigit=" + (int) DecimalStyle.of(format).getZeroDigit());
        System.out.println("bucket=" + EventPartitions.bucket(at, 15));
        System.out.println("stamp=" + Timestamps.iso(at.plusNanos(789_000_000L)));
        System.out.println("thirtyMinute=" + BucketKeys.thirtyMinute(at));
        System.out.println("hour=" + BucketKeys.hour(at));
    }
}
