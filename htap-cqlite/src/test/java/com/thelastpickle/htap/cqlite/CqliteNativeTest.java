package com.thelastpickle.htap.cqlite;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The boundary against the real library, and against real SSTable files when they are
 * given.
 *
 * <p>Every test here reports itself skipped unless it is told where the library is,
 * because the library is a linux artefact and a developer machine here is darwin: {@code
 * mvn verify} must pass on a machine that cannot load it at all. The four properties are
 * {@code htap.cqlite.library}, and for the tests that read data, {@code
 * htap.cqlite.it.table}, {@code htap.cqlite.it.directory} and {@code
 * htap.cqlite.it.ddl-file}. {@code htap.cqlite.it.rows} is what CQL counts in that table,
 * and setting it turns the count into an agreement with the CQL path. {@code
 * htap-cqlite/pom.xml} passes them through, and the container run recorded in the commit
 * message is what sets them.
 */
class CqliteNativeTest {

    private static CqliteLibrary library;

    private RootAllocator allocator;
    private CqliteSession session;

    @BeforeAll
    static void loadLibrary() {
        String path = property("htap.cqlite.library");
        assumeTrue(path != null, "htap.cqlite.library is unset, so there is no library to load");
        library = CqliteLibrary.load(Path.of(path));
    }

    @BeforeEach
    void openSession() {
        allocator = new RootAllocator();
        session = library.openSession(allocator);
    }

    @AfterEach
    void closeSession() {
        if (session != null) {
            session.close();
        }
        if (allocator != null) {
            // Fails on a buffer a statement imported and did not give back.
            allocator.close();
        }
    }

    @Test
    void theLoadedLibraryReportsItself() {
        assertEquals(Abi.VERSION, library.abiVersion());
        assertFalse(library.buildInfo().isBlank(), "cqlite_build_info said nothing");
    }

    /** No SSTable is a decline a flush ends, and the code says which of the two it is. */
    @Test
    void anEmptyDirectoryIsNotReady(@TempDir Path empty) {
        session.registerTable("nothing", empty, "CREATE TABLE nothing (k text PRIMARY KEY)", OpenOptions.DEFAULTS);
        CqliteException refusal = assertThrows(CqliteException.class, () -> session.discover("nothing"));
        assertEquals(CqliteStatus.NOT_READY, refusal.status());
    }

    @Test
    void aCreateStatementThatDoesNotParseIsRefused(@TempDir Path empty) {
        CqliteException refusal = assertThrows(
                CqliteException.class,
                () -> session.registerTable("broken", empty, "NOT A CREATE TABLE", OpenOptions.DEFAULTS));
        assertEquals(CqliteStatus.SCHEMA, refusal.status());
    }

    @Test
    void aTableIsDiscoveredAndCounted() {
        String table = registerLiveTable();

        Discovery discovery = session.discover(table);
        assertTrue(discovery.files() > 0, "the directory holds no SSTable file");
        assertTrue(discovery.bytes() > 0, "the SSTable files are empty");
        assertTrue(discovery.dataAge().isPresent(), "a directory holding files has a newest file");

        try (CqliteStatement statement = session.query("SELECT count(*) AS n FROM " + table)) {
            List<Map<String, Object>> rows = statement.rows();
            assertEquals(1, rows.size());
            long counted = (Long) rows.getFirst().get("n");
            String expected = property("htap.cqlite.it.rows");
            if (expected == null) {
                assertTrue(counted >= 0L, "count(*) is not a count");
            } else {
                assertEquals(Long.parseLong(expected), counted, "the count CQL gives for " + table);
            }

            ScanFigures figures = statement.scan();
            assertEquals(1L, figures.tables(), "one scan for one table");
            assertTrue(figures.files() > 0, "the scan opened no file");
            assertTrue(figures.bytes() > 0, "the scan opened nothing of any size");
            // Non-negative and finite rather than positive: an elapsed time is a timing
            // assertion, and one small generation can round to 0.0 with nothing wrong.
            // That a reader opened at all is what files() above establishes.
            assertTrue(
                    figures.readerOpenMillis() >= 0.0 && Double.isFinite(figures.readerOpenMillis()),
                    "reader_open_ms is not an elapsed time: " + figures.readerOpenMillis());
        }
    }

    /**
     * The figures outlive the statement, because a caller reports them from the branch that
     * handles a failure, outside the try-with-resources that closed it.
     */
    @Test
    void theFiguresAreReadableAfterTheStatementCloses() {
        String table = registerLiveTable();
        CqliteStatement statement = session.query("SELECT count(*) FROM " + table);
        ScanFigures drained;
        try (statement) {
            statement.rows();
            drained = statement.scan();
        }
        assertEquals(drained, statement.scan());
        assertThrows(IllegalStateException.class, statement::rows, "a closed statement drains");
    }

    /** A statement that does not plan fails where it is planned, before any file is read. */
    @Test
    void anUnknownColumnIsRefusedAtPlanning() {
        String table = registerLiveTable();
        CqliteException refusal = assertThrows(
                CqliteException.class, () -> session.query("SELECT no_such_column FROM " + table));
        assertTrue(
                refusal.getMessage().contains("no_such_column"),
                "the message names the column, and said: " + refusal.getMessage());
    }

    /** A cancelled scan and a failed one are one code, and only the intent tells them apart. */
    @Test
    void aCancelledScanReportsItselfAsCancelled() {
        String table = registerLiveTable();
        try (CqliteStatement statement = session.query("SELECT * FROM " + table)) {
            assertTrue(statement.cancel(), "cancelling an open statement");
            CqliteException stopped = assertThrows(CqliteException.class, statement::rows);
            assertTrue(stopped.cancelled(), "the drain reported: " + stopped.getMessage());
            assertEquals(CqliteStatement.CANCELLED_MESSAGE, stopped.getMessage());
        }
    }

    /** A sink that closes the statement it is reading is refused, where it would once hang. */
    @Test
    void aSinkThatClosesIsRefusedRatherThanDeadlocked() {
        String table = registerLiveTable();
        // Not try-with-resources: the sink's own close() is the mistake under test, and
        // -Xlint:try reports an explicit close of a declared resource.
        CqliteStatement statement = session.query("SELECT count(*) FROM " + table);
        try {
            IllegalStateException refusal = assertThrows(
                    IllegalStateException.class, () -> statement.forEachBatch(batch -> statement.close()));
            assertTrue(
                    refusal.getMessage().contains("cancel()"),
                    "the message says what to call instead, and said: " + refusal.getMessage());
        } finally {
            statement.close();
        }
    }

    @Test
    void aDeregisteredTableIsGone() {
        String table = registerLiveTable();
        session.deregisterTable(table);
        assertThrows(CqliteException.class, () -> session.query("SELECT count(*) FROM " + table));
    }

    /** The live table the data properties name, registered on this test's session. */
    private String registerLiveTable() {
        String table = property("htap.cqlite.it.table");
        String directory = property("htap.cqlite.it.directory");
        String ddlFile = property("htap.cqlite.it.ddl-file");
        assumeTrue(
                table != null && directory != null && ddlFile != null,
                "htap.cqlite.it.table, .directory and .ddl-file name the live files to read");
        session.registerTable(table, Path.of(directory), read(Path.of(ddlFile)), OpenOptions.DEFAULTS);
        return table;
    }

    /** Null for a property surefire passed through unset, which arrives as empty. */
    private static String property(String name) {
        String value = System.getProperty(name, "");
        return value.isBlank() ? null : value;
    }

    private static String read(Path path) {
        try {
            return Files.readString(path);
        } catch (IOException e) {
            throw new UncheckedIOException(path + " holds the CREATE TABLE statement", e);
        }
    }
}
