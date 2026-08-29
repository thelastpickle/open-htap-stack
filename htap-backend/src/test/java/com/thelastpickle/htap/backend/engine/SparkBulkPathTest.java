package com.thelastpickle.htap.backend.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.thelastpickle.htap.backend.config.CassandraSettings;
import com.thelastpickle.htap.backend.config.SparkSettings;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * The two decisions the bulk reader makes before it reads: how long its snapshot lives, and which
 * tables it takes one of.
 *
 * <p>Both are settled from the settings alone. What the reader then does with the snapshot needs a
 * Sidecar and a Thrift Server, and is verified by running a query against the stack.
 */
class SparkBulkPathTest {

    /**
     * Twice the query timeout, rounded up to the minute: a snapshot has to outlast the read, and
     * Cassandra clears it on time regardless of who is reading. A fixed fifteen minutes lost a
     * sixteen-minute contended run mid-scan, which failed with "Required 1 replicas but only 0
     * responded".
     */
    @Test
    void theSnapshotOutlivesTwiceTheQueryTimeout() {
        assertEquals(30, path(900).snapshotTtl().toMinutes());
        assertEquals(60, path(1800).snapshotTtl().toMinutes());
        assertEquals(32, path(901).snapshotTtl().toMinutes());
    }

    /** Never under a quarter of an hour, whatever a short timeout would otherwise give. */
    @Test
    void theSnapshotLivesAtLeastFifteenMinutes() {
        assertEquals(15, path(300).snapshotTtl().toMinutes());
        assertEquals(15, path(1).snapshotTtl().toMinutes());
    }

    @Test
    void theSnapshotAlwaysOutlastsTheReadItIsTakenFor() {
        for (int timeout : new int[] {1, 60, 300, 900, 901, 1800, 3600}) {
            assertTrue(
                    path(timeout).snapshotTtl().toSeconds() > timeout,
                    "a " + timeout + "s query may outlive its snapshot");
        }
    }

    /**
     * Only the tables the statement reads, matched on the names the dialect has already rewritten,
     * so nothing here parses SQL and no table is snapshotted for a statement that never reads it.
     */
    @Test
    void onlyTheTablesTheStatementNamesAreSnapshotted() {
        assertEquals(
                List.of("events"),
                SparkBulkPath.tablesIn("SELECT count(*) FROM bulk_events LIMIT 10"));
        assertEquals(
                List.of("drone_latest_status", "events"),
                SparkBulkPath.tablesIn(
                        "SELECT * FROM bulk_events e JOIN BULK_DRONE_LATEST_STATUS s"
                                + " ON e.entity_id = s.drone_id"));
        assertEquals(List.of(), SparkBulkPath.tablesIn("SELECT 1"));
    }

    /** The unprefixed name is not one of this path's views, so it is nothing to snapshot. */
    @Test
    void anUnprefixedTableNameIsNotOneOfThisPathsViews() {
        assertEquals(List.of(), SparkBulkPath.tablesIn("SELECT * FROM events"));
    }

    /** The bulk views carry a prefix of their own, so the two Spark paths cannot collide. */
    @Test
    void theDialectAimsAtTheBulkViewsAndBoundsTheStatement() {
        assertEquals(
                "SELECT * FROM bulk_events LIMIT 10",
                path(900).dialect("SELECT * FROM events ALLOW FILTERING", 10));
    }

    private static SparkBulkPath path(int queryTimeoutSeconds) {
        return new SparkBulkPath(cassandra(), spark(queryTimeoutSeconds), null);
    }

    private static CassandraSettings cassandra() {
        return new CassandraSettings() {
            @Override
            public String host() {
                return "cassandra";
            }

            @Override
            public int port() {
                return 9042;
            }

            @Override
            public String keyspace() {
                return "demo";
            }

            @Override
            public String datacenter() {
                return "datacenter1";
            }

            @Override
            public int sidecarPort() {
                return 9043;
            }

            @Override
            public Optional<String> translateAddressesTo() {
                return Optional.empty();
            }
        };
    }

    private static SparkSettings spark(int seconds) {
        return Engines.spark(4040, seconds);
    }
}
