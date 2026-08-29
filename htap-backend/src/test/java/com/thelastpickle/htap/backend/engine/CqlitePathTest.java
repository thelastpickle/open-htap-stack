package com.thelastpickle.htap.backend.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.thelastpickle.htap.backend.config.CassandraSettings;
import com.thelastpickle.htap.backend.config.CqliteSettings;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Which directory the reader opens, and the statement it parses the files with.
 *
 * <p>Both are settled before an SSTable is touched, so both are answered here over a directory tree
 * this test makes. What the reader then reads out of real files is verified by running a query
 * against the stack.
 */
class CqlitePathTest {

    private static final String KEYSPACE = "demo";

    @TempDir
    Path dataDir;

    private Path keyspace;

    @BeforeEach
    void makeKeyspaceDirectory() throws IOException {
        keyspace = Files.createDirectories(dataDir.resolve(KEYSPACE));
    }

    /**
     * The id the cluster holds names the directory, and it is what tells two incarnations of a
     * dropped and recreated table apart: the older directory is still on disk, and reading it would
     * answer from data the cluster has forgotten.
     */
    @Test
    void theDirectoryTheClustersIdNamesIsTheOneOpened() throws IOException {
        UUID id = UUID.randomUUID();
        Path current = directory("events", id);
        directory("events", UUID.randomUUID());

        assertEquals(current, path().tableDirectory("events", Optional.of(id)));
    }

    /** With no id to go on, one matching directory is unambiguous and is taken. */
    @Test
    void withNoIdTheOneMatchingDirectoryIsTaken() throws IOException {
        Path only = directory("events", UUID.randomUUID());

        assertEquals(only, path().tableDirectory("events", Optional.empty()));
    }

    /** An id whose directory is absent falls back to the same single match rather than refusing. */
    @Test
    void anIdWithNoDirectoryOfItsOwnFallsBackToTheOneMatch() throws IOException {
        Path only = directory("events", UUID.randomUUID());

        assertEquals(only, path().tableDirectory("events", Optional.of(UUID.randomUUID())));
    }

    /**
     * A table Cassandra has never flushed has no directory, and the message names the setting to
     * look at: on a stack whose data directory is not mounted, every table fails this way.
     */
    @Test
    void aTableWithNoDirectoryReportsWhereItLooked() {
        NoSuchFileException refused = assertThrows(
                NoSuchFileException.class,
                () -> path().tableDirectory("events", Optional.empty()));

        assertTrue(String.valueOf(refused.getReason()).contains("cqlite.data-dir"), refused.toString());
    }

    /** Two incarnations and nothing to choose between them: guessing would answer from either. */
    @Test
    void twoDirectoriesAndNoIdToChooseBetweenThemAreRefused() throws IOException {
        directory("events", UUID.randomUUID());
        directory("events", UUID.randomUUID());

        IOException refused = assertThrows(
                IOException.class, () -> path().tableDirectory("events", Optional.empty()));

        assertTrue(refused.getMessage().startsWith("2 directories match demo.events"), refused.toString());
    }

    /** A prefix match is not a match: another table's directory begins with this table's name. */
    @Test
    void aTableWhoseNameIsAnothersPrefixIsNotConfusedWithIt() throws IOException {
        Path events = directory("events", UUID.randomUUID());
        directory("events_by_entity", UUID.randomUUID());

        assertEquals(events, path().tableDirectory("events", Optional.empty()));
    }

    /**
     * The reader parses one statement, so the semicolon the driver appends would make it two.
     */
    @Test
    void theCreateStatementLosesTheSemicolonTheDriverAppends() {
        assertEquals(
                "CREATE TABLE demo.events (event_id uuid PRIMARY KEY)",
                CqlitePath.createTableCql(describedAs(
                        "CREATE TABLE demo.events (event_id uuid PRIMARY KEY);\n")));
    }

    /** A description that never had one is left as it is, rather than losing its last character. */
    @Test
    void aStatementWithNoSemicolonIsUnchanged() {
        assertEquals(
                "CREATE TABLE demo.events (event_id uuid PRIMARY KEY)",
                CqlitePath.createTableCql(
                        describedAs("  CREATE TABLE demo.events (event_id uuid PRIMARY KEY)  ")));
    }

    /** Nothing to stop is false, so the Health page does not report having stopped a scan. */
    @Test
    void aStopWithNoScanRunningIsRefused() {
        assertFalse(path().abort());
    }

    /**
     * A scan that holds the lock but has no statement yet is still stopped.
     *
     * <p>This is the window in which {@code query} is planning the scan and opening its files, which
     * on a large table is seconds; the statement the binding cancels through does not exist yet, so
     * the flag is the whole answer and the drain tests it the moment it has one. That the flag then
     * turns the drain's error into the cancelled message needs a {@code CqliteException}, which the
     * binding does not let a caller construct, so it is verified by stopping a real scan.
     */
    @Test
    void aStopPressedBeforeTheScanHasAStatementIsStillAStop() {
        CqlitePath scanning = new CqlitePath(cassandra(), cqlite(dataDir), null) {
            @Override
            public boolean busy() {
                return true;
            }
        };

        assertTrue(scanning.abort());
    }

    /** No session, so no library to report on: a build string here would name a path that cannot read. */
    @Test
    void aPathThatHasOpenedNothingReportsNoBuildInfo() {
        assertTrue(path().buildInfo().isEmpty());
    }

    /**
     * A statement on a path with no session is refused, and the lock is given back: the refusal is
     * one a young stack meets, and a lock left held would make every later scan report a scan
     * running.
     */
    @Test
    void aStatementWithNoSessionIsRefusedAndLeavesNoScanRunning() {
        CqlitePath path = path();

        assertThrows(EngineUnavailable.class, () -> path.query("SELECT 1"));
        assertFalse(path.busy());
        assertFalse(path.abort());
    }

    private Path directory(String table, UUID id) throws IOException {
        return Files.createDirectories(
                keyspace.resolve(table + "-" + id.toString().replace("-", "")));
    }

    private CqlitePath path() {
        return new CqlitePath(cassandra(), cqlite(dataDir), null);
    }

    private static TableMetadata describedAs(String description) {
        return (TableMetadata) Proxy.newProxyInstance(
                CqlitePathTest.class.getClassLoader(),
                new Class<?>[] {TableMetadata.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "describe" -> description;
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static CassandraSettings cassandra() {
        return (CassandraSettings) Proxy.newProxyInstance(
                CqlitePathTest.class.getClassLoader(),
                new Class<?>[] {CassandraSettings.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "keyspace" -> KEYSPACE;
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static CqliteSettings cqlite(Path dataDir) {
        return (CqliteSettings) Proxy.newProxyInstance(
                CqlitePathTest.class.getClassLoader(),
                new Class<?>[] {CqliteSettings.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "dataDir" -> dataDir;
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }
}
