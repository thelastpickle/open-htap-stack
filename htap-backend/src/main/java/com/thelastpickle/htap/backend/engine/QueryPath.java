package com.thelastpickle.htap.backend.engine;

/**
 * An access path the query console and the comparison can aim a statement at.
 *
 * <p>A path rewrites the statement for its own engine and then runs it, so the two live
 * together here. The Python paired a client with a rewriting function in a module-level table,
 * which left a path's dialect somewhere its client could not see and made adding a path an
 * edit in two places.
 */
public interface QueryPath extends EnginePath {

    /**
     * The same question in this engine's own spelling, bounded by {@code limit}.
     *
     * <p>Every path is given the one statement a caller typed. What differs is the name each
     * table is reachable under and which clauses the engine accepts, and a path that cannot
     * express the question at all is expected to say so when the statement runs rather than
     * here: the refusal is the finding the compare page reports.
     */
    String dialect(String sql, int limit);

    /** Run the statement this path's {@link #dialect} produced. */
    QueryRows query(String sql);

    /**
     * Run it, reading what the last read prepared if this path has anything to reuse.
     *
     * <p>Only the bulk reader has, so the default ignores the request rather than every caller
     * asking whether a path is that one.
     */
    default QueryRows query(String sql, boolean reusePrepared) {
        return query(sql);
    }

    /** Whether asking to reuse means anything on this path. */
    default boolean supportsSnapshotReuse() {
        return false;
    }

    /** True while a statement is in flight, which is what a cancel and a reconnect ask. */
    default boolean busy() {
        return false;
    }

    /**
     * Stop a statement that is running, from another thread.
     *
     * <p>False when there was nothing to stop. Each path stops differently and one does not
     * stop at all, so the caller gets no promise beyond the return value; the four mechanisms
     * are in the paths that own them.
     */
    default boolean abort() {
        return false;
    }
}
