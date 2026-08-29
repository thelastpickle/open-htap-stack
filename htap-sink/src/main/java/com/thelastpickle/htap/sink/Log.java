package com.thelastpickle.htap.sink;

import java.util.Locale;

/**
 * The sink's own lines, on standard output with the prefix the Python used.
 *
 * <p>Printed rather than logged, and that is a decision about what these lines are for:
 * {@code podman logs data-cassandra-sink} is where an operator reads them and the compose suite
 * greps them, so the prefixes {@code [sink]} and {@code [alert]} are an interface. Keeping them
 * identical to the Python's is what lets the two implementations be compared line for line while
 * both exist.
 *
 * <p>What this gives up is the driver's own logging: driver 4.19 logs through SLF4J, and with no
 * provider on the classpath its warnings (a schema agreement timeout, a request retried on
 * another node) go nowhere, and it says so once on standard error. Adding
 * {@code org.slf4j:slf4j-simple} is the whole of the fix, and it is left out until something needs
 * it rather than added on the chance.
 *
 * <p>{@code println} rather than {@code printf}: {@code System.out} is line-buffered with autoflush
 * and flushes on a println, which is what {@code python -u} was doing for the Python in its
 * Dockerfile. A container whose logs appear in eight-kilobyte bursts is worse than no logs.
 */
final class Log {

    private Log() {}

    /** One line from the sink itself. */
    static void sink(String format, Object... values) {
        say("[sink] ", format, values);
    }

    /** One line from the alert scorer, which the Python tagged separately. */
    static void alert(String format, Object... values) {
        say("[alert] ", format, values);
    }

    private static void say(String tag, String format, Object... values) {
        // Locale.ROOT, because a formatted figure under a locale with its own digits would print
        // them: the same hazard EventPartitions documents for a bucket key.
        System.out.println(tag + String.format(Locale.ROOT, format, values));
    }
}
