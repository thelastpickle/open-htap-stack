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
        return oneLine(text, LIMIT);
    }

    /**
     * The same, cut at a caller's own limit.
     *
     * <p>The statement columns are narrower than the message one, and a statement's limit has to be
     * the same everywhere: the Spark cancel recognises its own jobs by comparing a statement it
     * collapsed against a description the job list collapsed, so a difference of one character
     * between the two would match nothing.
     */
    public static String oneLine(String text, int limit) {
        if (text == null) {
            return "";
        }
        String flattened = text.strip().replaceAll("\\s+", " ");
        return flattened.length() <= limit ? flattened : flattened.substring(0, limit);
    }

    /** The failure's message on one line, or its type where it carries no message. */
    public static String oneLine(Throwable failure) {
        String message = oneLine(failure.getMessage());
        return message.isEmpty() ? failure.getClass().getSimpleName() : message;
    }
}
