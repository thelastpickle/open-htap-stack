package com.thelastpickle.htap.backend.api.dto;

import com.thelastpickle.htap.backend.engine.ReadFigures;
import com.thelastpickle.htap.backend.query.OltpImpact;
import com.thelastpickle.htap.backend.query.PathResult;
import java.util.List;

/**
 * What one path answered a comparison, as the compare page reads it.
 *
 * <p>The figures a path measured are flat here rather than nested, because that is the shape the page
 * already reads and each field is labelled with what it measures: {@code snapshotBytes} is the size
 * of the snapshot a bulk read was taken over and {@code sstableBytes} the size of the live files a
 * cqlite read opened, and neither is what the read consumed when the statement names partitions.
 *
 * @param available false means the path could not be reached at all, which the page distinguishes
 *     from a path that reached its engine and was refused
 * @param oltp what the point read cost while this path worked, or null when there was no asset to
 *     read
 */
public record EngineResult(
        List<String> columns,
        List<List<Object>> rows,
        int rowCount,
        double queryTimeMs,
        String sql,
        boolean available,
        String error,
        OltpImpact oltp,
        Long snapshotBytes,
        Double snapshotMs,
        boolean snapshotReused,
        Double snapshotAgeS,
        Long sstableFiles,
        Long sstableBytes,
        Double readerOpenMs,
        Long dataAgeS) {

    public static EngineResult of(PathResult result, OltpImpact oltp) {
        ReadFigures figures = result.figures();
        return new EngineResult(
                result.columns(),
                result.rows(),
                result.rowCount(),
                // Zero rather than absent for a path that was never reached, because the page
                // divides bytes by this figure and would render a rate of NaN.
                result.queryTimeMs() == null ? 0.0 : result.queryTimeMs(),
                result.sql(),
                result.available(),
                result.error(),
                oltp,
                figures.snapshotBytes(),
                figures.snapshotMs(),
                figures.snapshotReused(),
                figures.snapshotAgeS(),
                figures.sstableFiles(),
                figures.sstableBytes(),
                figures.readerOpenMs(),
                figures.dataAgeS());
    }
}
