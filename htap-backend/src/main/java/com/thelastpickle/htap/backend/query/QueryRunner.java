package com.thelastpickle.htap.backend.query;

import com.thelastpickle.htap.backend.engine.EngineFailed;
import com.thelastpickle.htap.backend.engine.EngineUnavailable;
import com.thelastpickle.htap.backend.engine.QueryPath;
import com.thelastpickle.htap.backend.engine.QueryRows;
import com.thelastpickle.htap.backend.engine.ReadFigures;
import com.thelastpickle.htap.backend.support.Messages;
import com.thelastpickle.htap.backend.support.Round;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.List;
import java.util.function.LongSupplier;

/**
 * Runs one statement on one path and times it, reporting a failure rather than raising.
 *
 * <p>Shared by the console, which turns a failure back into an HTTP status, and by the comparison,
 * which shows it beside the paths that answered. Both need the same three outcomes told apart:
 * unreachable, refused, and answered.
 */
@ApplicationScoped
public class QueryRunner {

    private final LongSupplier nanoClock;

    QueryRunner() {
        this(System::nanoTime);
    }

    QueryRunner(LongSupplier nanoClock) {
        this.nanoClock = nanoClock;
    }

    /**
     * Rewrite the statement for this path, run it, and time it.
     *
     * <p>{@code reuseSnapshot} is offered to every path and honoured by the one that has anything
     * to reuse, which says so itself rather than being named here.
     */
    public PathResult run(QueryPath path, String sql, int limit, boolean reuseSnapshot) {
        if (!path.connected()) {
            try {
                path.connect();
            } catch (RuntimeException e) {
                return PathResult.unavailable(path.name(), Messages.oneLine(e));
            }
        }
        if (!path.connected()) {
            return PathResult.unavailable(path.name(), "Engine not connected");
        }
        String statement = path.dialect(sql, limit);
        long started = nanoClock.getAsLong();
        try {
            QueryRows rows = path.query(statement, reuseSnapshot && path.supportsSnapshotReuse());
            return new PathResult(
                    path.name(),
                    true,
                    statement,
                    rows.columns(),
                    rows.rows(),
                    rows.rowCount(),
                    elapsedMs(started),
                    null,
                    rows.figures());
        } catch (EngineUnavailable e) {
            return PathResult.unavailable(path.name(), Messages.oneLine(e));
        } catch (EngineFailed e) {
            // The figures come with the failure: a scan that was stopped had already opened its
            // files, and how much it had in front of it is part of why it was still running.
            return failed(path, statement, started, Messages.oneLine(e), e.figures());
        } catch (RuntimeException e) {
            return failed(path, statement, started, Messages.oneLine(e), ReadFigures.NONE);
        }
    }

    private PathResult failed(
            QueryPath path, String statement, long started, String error, ReadFigures figures) {
        return new PathResult(
                path.name(), true, statement, List.of(), List.of(), 0,
                elapsedMs(started), error, figures);
    }

    private double elapsedMs(long startedNanos) {
        return Round.tenth((nanoClock.getAsLong() - startedNanos) / 1_000_000.0);
    }
}
