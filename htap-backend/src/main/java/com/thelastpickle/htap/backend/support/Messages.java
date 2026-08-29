package com.thelastpickle.htap.backend.support;

/**
 * A failure's own words, reduced to what a dashboard column can show.
 *
 * <p>Shared by the paths whose engines are verbose: a JVM engine answers with frames and a
 * DataFusion failure carries the whole plan, and both arrive across several lines.
 */
public final class Messages {

    /**
     * How much of a message is kept.
     *
     * <p>The Python's {@code _MESSAGE_LIMIT}, so the dashboard's message column keeps the width it
     * had.
     */
    public static final int LIMIT = 400;

    private Messages() {}

    /** The text on one line, cut at {@link #LIMIT}. */
    public static String oneLine(String text) {
        if (text == null) {
            return "";
        }
        String flattened = text.strip().replaceAll("\\s+", " ");
        return flattened.length() <= LIMIT ? flattened : flattened.substring(0, LIMIT);
    }

    /** The failure's message on one line, or its type where it carries no message. */
    public static String oneLine(Throwable failure) {
        String message = oneLine(failure.getMessage());
        return message.isEmpty() ? failure.getClass().getSimpleName() : message;
    }
}
