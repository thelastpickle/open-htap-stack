package com.thelastpickle.htap.cqlite;

/**
 * A refusal from the reader, with the code the boundary returned beside it.
 *
 * <p>Unchecked, because every caller of this module either reports the failure to a
 * viewer or lets it end the request; there is nothing a caller can do to make the
 * files readable that a {@code catch} would express.
 */
public class CqliteException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * How long a message may be before it is cut.
     *
     * <p>A DataFusion failure carries the whole plan, and what a viewer can act on
     * is the first part. The figure is the Python's, so the dashboard's message
     * column keeps the width it had.
     */
    static final int MESSAGE_LIMIT = 400;

    /**
     * The prefixes each layer adds, stripped repeatedly rather than once because
     * they nest.
     *
     * <p>Five, as the Python's list is, with one change either way. {@code FFI error: }
     * is gone: it came from the DataFusion capsule the wheel handed Python, which this
     * boundary replaces. {@code Arrow error: } is new, because a failure the reader
     * raises during the drain reaches this side through Arrow's own stream. {@code
     * cqlite: } is still produced, by {@code Error::Cqlite}, so it stays; but the
     * boundary now unwraps a provider failure out of DataFusion's own nesting before it
     * reports it, so the deep chains this list existed for arrive already flat.
     */
    private static final String[] PREFIXES = {
        "DataFusion error: ",
        "External error: ",
        "Execution error: ",
        "Arrow error: ",
        "cqlite: ",
    };

    private final CqliteStatus status;
    private final int code;
    private final boolean cancelled;

    CqliteException(CqliteStatus status, int code, boolean cancelled, String message) {
        super(message);
        this.status = status;
        this.code = code;
        this.cancelled = cancelled;
    }

    CqliteException(CqliteStatus status, String message, Throwable cause) {
        super(message, cause);
        this.status = status;
        this.code = status.code();
        this.cancelled = false;
    }

    /** What the boundary answered, as far as this binding knows the code. */
    public CqliteStatus status() {
        return status;
    }

    /**
     * The integer the boundary returned, which is the code and not the status: a
     * number this binding does not know reports {@link CqliteStatus#ERROR} and
     * keeps its own value here.
     */
    public int code() {
        return code;
    }

    /**
     * Whether this statement was stopped rather than having failed.
     *
     * <p>The two are one code at the boundary, because a cancelled merge reports
     * itself as an error like any other; only the operator's intent tells them
     * apart, and an operator who pressed stop should read that they stopped it.
     */
    public boolean cancelled() {
        return cancelled;
    }

    /**
     * A failure's own message, reduced to what a viewer can act on.
     *
     * <p>Whitespace is collapsed first, because a DataFusion message spans lines and
     * the dashboard shows it in one.
     */
    static String readable(String raw) {
        String text = raw == null ? "" : raw.strip().replaceAll("\\s+", " ");
        boolean stripped = true;
        while (stripped) {
            stripped = false;
            for (String prefix : PREFIXES) {
                if (text.startsWith(prefix)) {
                    text = text.substring(prefix.length());
                    stripped = true;
                }
            }
        }
        return text.length() <= MESSAGE_LIMIT ? text : text.substring(0, MESSAGE_LIMIT);
    }
}
