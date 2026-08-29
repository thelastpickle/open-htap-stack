package com.thelastpickle.htap.backend.engine;

/**
 * What a path measured about its own read, where it measures anything.
 *
 * <p>Only the two file-reading paths can say much: the bulk reader reports the snapshot it
 * took, and the cqlite reader the live files it merged instead of taking one. The other three
 * return {@link #NONE}, which serialises as the nulls the pages already handle.
 *
 * <p>Each field is labelled with what it measures rather than with what a viewer might want
 * it to mean. {@code snapshotBytes} is the size of the snapshot a bulk read was taken over
 * and {@code sstableBytes} the size of the live files a cqlite read opened; neither is what
 * the read consumed when the statement names partitions, which is why the compare page quotes
 * a rate only for a statement with no {@code WHERE}.
 *
 * <p>A field is boxed where the Python sent {@code None}, since absent and zero are different
 * answers: a path that could not size its snapshot has to say so rather than report nothing
 * read.
 */
public record ReadFigures(
        Long snapshotBytes,
        Double snapshotMs,
        boolean snapshotReused,
        Double snapshotAgeS,
        Long sstableFiles,
        Long sstableBytes,
        Double readerOpenMs,
        Long dataAgeS) {

    /** A path that measures nothing about its read, which is three of the five. */
    public static final ReadFigures NONE =
            new ReadFigures(null, null, false, null, null, null, null, null);

    /** The bulk reader's four, which describe a snapshot. */
    public static ReadFigures snapshot(Long bytes, double prepareMs, boolean reused, Double ageS) {
        return new ReadFigures(bytes, prepareMs, reused, ageS, null, null, null, null);
    }

    /** The cqlite reader's four, which describe the live files. */
    public static ReadFigures sstables(long files, long bytes, double openMs, Long ageS) {
        return new ReadFigures(null, null, false, null, files, bytes, openMs, ageS);
    }
}
