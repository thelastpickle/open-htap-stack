package com.thelastpickle.htap.common;

import java.time.Instant;
import java.time.format.DecimalStyle;
import java.util.Locale;

/**
 * Prints one bucket, to be run in a forked JVM by
 * {@link EventPartitionsTest#theBucketIsAsciiUnderAJvmThatNumbersInItsOwnScript()}.
 *
 * <p>A separate JVM is what makes that case falsifiable. {@code EventPartitions}
 * initialises its formatter once, and a {@code DateTimeFormatter} keeps the
 * {@link DecimalStyle} it was built with, so an in-process test that sets the FORMAT
 * locale is setting it after the decision has been made. Here the locale arrives on the
 * command line and the class has not yet loaded.
 *
 * <p>The zero digit is printed beside the bucket so a failure says which of the two went
 * wrong: a locale the JVM did not take, or a bucket that followed it.
 */
final class BucketLocaleProbe {

    private BucketLocaleProbe() {}

    public static void main(String[] args) {
        Locale format = Locale.getDefault(Locale.Category.FORMAT);
        System.out.println("locale=" + format.toLanguageTag());
        System.out.println("zeroDigit=" + (int) DecimalStyle.of(format).getZeroDigit());
        System.out.println("bucket=" + EventPartitions.bucket(Instant.ofEpochSecond(1767271500L), 15));
    }
}
