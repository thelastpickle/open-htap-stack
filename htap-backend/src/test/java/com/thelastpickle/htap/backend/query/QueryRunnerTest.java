package com.thelastpickle.htap.backend.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.thelastpickle.htap.backend.engine.EngineFailed;
import com.thelastpickle.htap.backend.engine.EngineUnavailable;
import com.thelastpickle.htap.backend.engine.QueryPath;
import com.thelastpickle.htap.backend.engine.QueryRows;
import com.thelastpickle.htap.backend.engine.ReadFigures;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

/**
 * The three outcomes the console and the comparison both need told apart: unreachable, refused,
 * and answered.
 *
 * <p>Against a path of this test's own rather than an engine, because what is decided here is
 * which outcome a failure becomes and what is timed; no engine is needed to settle either.
 */
class QueryRunnerTest {

    /** One millisecond per reading, so an elapsed figure is exactly one. */
    private final AtomicLong nanos = new AtomicLong();

    private final QueryRunner runner = new QueryRunner(() -> nanos.getAndAdd(1_000_000));

    @Test
    void anAnsweredStatementCarriesThePathsOwnSpellingAndItsTime() {
        PathResult result = runner.run(path(sql -> rows()), "SELECT 1", 10, false);

        assertTrue(result.answered());
        assertEquals("test", result.path());
        assertEquals("SELECT 1 LIMIT 10", result.sql());
        assertEquals(List.of("n"), result.columns());
        assertEquals(1, result.rowCount());
        assertEquals(1.0, result.queryTimeMs());
        assertNull(result.error());
    }

    /** Unreachable, so there is nothing to time and the statement was never spelled. */
    @Test
    void aPathThatWillNotConnectIsUnavailable() {
        TestPath path = path(sql -> rows());
        path.connects = false;

        PathResult result = runner.run(path, "SELECT 1", 10, false);

        assertFalse(result.available());
        assertEquals("Engine not connected", result.error());
        assertNull(result.sql());
        assertNull(result.queryTimeMs());
    }

    @Test
    void aConnectThatRaisesIsUnavailableWithItsOwnWords() {
        TestPath path = path(sql -> rows());
        path.connects = false;
        path.connectFailure = new EngineUnavailable("no route to host");

        assertEquals("no route to host", runner.run(path, "SELECT 1", 10, false).error());
    }

    /**
     * Reachable and refusing, which is the finding the demo exists to show: available stays true,
     * so the compare page reports a decline rather than a service that is down.
     */
    @Test
    void aRefusalIsAvailableAndTimedAndKeepsItsFigures() {
        ReadFigures measured = ReadFigures.sstables(4, 488_777_346L, 12.5, 30L);
        PathResult result = runner.run(
                path(sql -> {
                    throw new EngineFailed("GROUP BY is not supported on a non-key column",
                            null, measured);
                }),
                "SELECT event_type, count(*) FROM events GROUP BY event_type",
                10,
                false);

        assertTrue(result.available());
        assertFalse(result.answered());
        assertEquals("GROUP BY is not supported on a non-key column", result.error());
        assertEquals(1.0, result.queryTimeMs());
        assertEquals(measured, result.figures());
        assertEquals(0, result.rowCount());
    }

    /** An engine failing in a way no path anticipated is still a refusal, not a dead service. */
    @Test
    void anUnexpectedFailureIsReportedAsARefusalOnOneLine() {
        PathResult result = runner.run(
                path(sql -> {
                    throw new IllegalStateException("line one\n  line two");
                }),
                "SELECT 1",
                10,
                false);

        assertTrue(result.available());
        assertEquals("line one line two", result.error());
        assertEquals(ReadFigures.NONE, result.figures());
    }

    /** An unreachable engine raised from the query rather than the connect is still unavailable. */
    @Test
    void aQueryThatFindsThePathGoneIsUnavailable() {
        PathResult result = runner.run(
                path(sql -> {
                    throw new EngineUnavailable("session closed");
                }),
                "SELECT 1",
                10,
                false);

        assertFalse(result.available());
        assertEquals("session closed", result.error());
    }

    /** Every path is asked, and the one with nothing to reuse says so rather than the runner. */
    @Test
    void reuseIsOnlyPassedOnToAPathThatSupportsIt() {
        TestPath plain = path(sql -> rows());
        runner.run(plain, "SELECT 1", 10, true);

        assertFalse(plain.reuseAsked);

        TestPath reusing = path(sql -> rows());
        reusing.supportsReuse = true;
        runner.run(reusing, "SELECT 1", 10, true);

        assertTrue(reusing.reuseAsked);
    }

    private static QueryRows rows() {
        return new QueryRows(List.of("n"), List.of(List.of(1)));
    }

    private static TestPath path(Function<String, QueryRows> answer) {
        return new TestPath(answer);
    }

    private static final class TestPath implements QueryPath {

        private final Function<String, QueryRows> answer;

        private boolean connects = true;
        private RuntimeException connectFailure;
        private boolean supportsReuse;
        private boolean reuseAsked;

        private TestPath(Function<String, QueryRows> answer) {
            this.answer = answer;
        }

        @Override
        public String name() {
            return "test";
        }

        @Override
        public void connect(boolean force) {
            if (connectFailure != null) {
                throw connectFailure;
            }
        }

        @Override
        public boolean connected() {
            return connects;
        }

        @Override
        public String dialect(String sql, int limit) {
            return sql + " LIMIT " + limit;
        }

        @Override
        public QueryRows query(String sql) {
            return answer.apply(sql);
        }

        @Override
        public QueryRows query(String sql, boolean reusePrepared) {
            reuseAsked = reusePrepared;
            return query(sql);
        }

        @Override
        public boolean supportsSnapshotReuse() {
            return supportsReuse;
        }
    }
}
