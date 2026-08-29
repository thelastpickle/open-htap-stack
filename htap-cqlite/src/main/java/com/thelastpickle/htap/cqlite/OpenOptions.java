package com.thelastpickle.htap.cqlite;

/**
 * How a table is opened. A zero takes the library's own default, which is measured.
 *
 * @param splits how many slices of the token ring a full scan divides into; 1 by
 *     default, because the walk repeats most of a slice's work, so N slices cost N
 *     times the processor time and buy no wall clock
 * @param batchRows how many rows accumulate before a batch is emitted; 8192 by
 *     default, which is DataFusion's own figure and not one measured here
 * @param keyChunk how many of the partitions a predicate names are read at a time; 1
 *     by default, because the seek merger decodes every row of every partition it is
 *     given before the merge starts, some 3.9 GB per million rows
 */
public record OpenOptions(long splits, long batchRows, long keyChunk) {

    /**
     * The largest count the library accepts.
     *
     * <p>Each count is a length something allocates, and {@code token_splits} builds a
     * {@code Vec} of {@code splits} entries, so a value near {@code u64::MAX} aborts
     * the process rather than failing. The library refuses above this figure as well,
     * with {@code CQLITE_ERROR_BAD_ARGUMENT}; the check here is what turns a Java
     * mistake into a message that names the field.
     */
    public static final long MAX_COUNT = 1_048_576L;

    /** Every field zero, so the library decides all three. */
    public static final OpenOptions DEFAULTS = new OpenOptions(0L, 0L, 0L);

    public OpenOptions {
        check("splits", splits);
        check("batchRows", batchRows);
        check("keyChunk", keyChunk);
    }

    /**
     * Refuses a count the boundary cannot carry.
     *
     * <p>A negative value is refused here and nowhere else: it crosses as a {@code
     * uint64_t} near its maximum, so the library would see an enormous length rather
     * than a mistake, and one of the three would then abort the process.
     */
    private static void check(String field, long value) {
        if (value < 0L || value > MAX_COUNT) {
            throw new IllegalArgumentException(
                    field + " is " + value + ", and must be 0 to " + MAX_COUNT
                            + " where 0 takes the library's default");
        }
    }
}
