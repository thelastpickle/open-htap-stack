package com.thelastpickle.htap.cqlite;

import java.time.Duration;
import java.util.Optional;

/**
 * What one statement read, summed over its scans by the library.
 *
 * <p>Readable while the statement runs and complete once its stream has ended, so a
 * caller reporting figures reads it after the drain.
 *
 * @param tables how many table scans the statement planned; a statement reading one
 *     table twice counts two, because each scan lists the directory again
 * @param files the files those scans opened
 * @param bytes the total size of those files, which is not what the statement read when
 *     it names partitions; quote a rate from it only for a statement with no {@code
 *     WHERE}
 * @param readerOpenMillis the time spent opening readers, unrounded; the dashboard
 *     rounds to one decimal where it reports the figure, as the Python did, rather than
 *     losing the precision here
 * @param dataAge how long ago the newest file any scan opened was written, the largest
 *     of the scans' ages because an answer is as stale as its stalest table; empty when
 *     no scan reported one
 */
public record ScanFigures(
        long tables, long files, long bytes, double readerOpenMillis, Optional<Duration> dataAge) {

    static ScanFigures of(
            long tables, long files, long bytes, double readerOpenMillis, long ageSeconds) {
        return new ScanFigures(tables, files, bytes, readerOpenMillis, Discovery.age(ageSeconds));
    }
}
