package com.thelastpickle.htap.backend.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.thelastpickle.htap.backend.engine.QueryPath;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The five paths, in the order the dashboard shows them and the comparison runs them.
 *
 * <p>Through the container, because the order is the thing under test and the container injects in
 * no order a viewer could rely on: a list built by hand here would prove nothing about what the
 * running application offers.
 */
@QuarkusTest
class QueryPathsTest {

    private static final List<String> DECLARED =
            List.of("cassandra", "presto", "spark", "spark_bulk", "cqlite");

    @Inject
    QueryPaths paths;

    @Test
    void thePathsArriveInTheDeclaredOrder() {
        assertEquals(DECLARED, paths.all().stream().map(QueryPath::name).toList());
    }

    @Test
    void eachPathIsFoundByTheNameItReports() {
        for (String name : DECLARED) {
            assertEquals(name, paths.byName(name).orElseThrow().name());
        }
    }

    /** A caller may name anything, so an unknown name is empty rather than a failure. */
    @Test
    void anUnknownNameIsEmpty() {
        assertTrue(paths.byName("duckdb").isEmpty());
        assertTrue(paths.byName("Cassandra").isEmpty());
        assertTrue(paths.byName("").isEmpty());
    }

    /** The selector reads this map, and it is ordered: a plain map literal would not be. */
    @Test
    void theStatusIsOrderedAndNothingIsConnectedHere() {
        assertEquals(DECLARED, List.copyOf(paths.status().keySet()));
        assertEquals(List.of(false, false, false, false, false), List.copyOf(paths.status().values()));
    }
}
