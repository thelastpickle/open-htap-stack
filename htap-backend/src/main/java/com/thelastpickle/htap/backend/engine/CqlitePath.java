package com.thelastpickle.htap.backend.engine;

import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.thelastpickle.htap.backend.config.CassandraSettings;
import com.thelastpickle.htap.backend.config.CqliteSettings;
import com.thelastpickle.htap.backend.query.Dialects;
import com.thelastpickle.htap.backend.support.Messages;
import com.thelastpickle.htap.backend.support.Round;
import com.thelastpickle.htap.cqlite.CqliteException;
import com.thelastpickle.htap.cqlite.CqliteLibrary;
import com.thelastpickle.htap.cqlite.CqliteSession;
import com.thelastpickle.htap.cqlite.CqliteStatement;
import com.thelastpickle.htap.cqlite.Discovery;
import com.thelastpickle.htap.cqlite.OpenOptions;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.jboss.logging.Logger;

/**
 * SQL over the live SSTable files, in this process: the fifth access path.
 *
 * <p>The only path that reads Cassandra's data files where they lie. There is no snapshot, no
 * Sidecar and no second JVM: the reader opens the {@code Data.db} files a flush or a compaction has
 * already written, merges the generations so each row is resolved once, and DataFusion plans and
 * executes the SQL over them.
 *
 * <p>Two consequences the dashboard states rather than hides. An answer is as of the last flush, so
 * rows still in a memtable are invisible and {@code dataAgeS} says how stale the answer was. And a
 * table Cassandra has never flushed has no file to read, so the path declines instead of returning
 * nothing, which is the state a stack that started minutes ago is in.
 *
 * <p>It cannot contend with the request path, because it never enters it: no coordinator, no read
 * repair and none of Cassandra's own page cache is involved. That is the bulk reader's claim
 * without the snapshot.
 */
@ApplicationScoped
public class CqlitePath implements QueryPath {

    private static final Logger LOG = Logger.getLogger(CqlitePath.class);

    /**
     * The tables the path offers.
     *
     * <p>Three of the demo's seven. {@code drone_text_embeddings} is left out on purpose: it holds
     * a {@code vector<float, n>} column, which this reader has no Arrow type for, so registering it
     * would fail rather than answer.
     */
    public static final List<String> CQLITE_TABLES =
            List.of("events", "drone_latest_status", "drone_events_by_entity");

    /** The registered names are the table names, so nothing is prefixed. */
    static final String TABLE_PREFIX = "";

    /**
     * How often registration is looked at again.
     *
     * <p>Longer than the other paths' ten seconds, because an attempt here reads three table
     * definitions from Cassandra rather than opening a socket, and the Health page polls.
     */
    static final Duration RETRY_INTERVAL = Duration.ofSeconds(30);

    /**
     * What a stopped scan reports.
     *
     * <p>Cancellation is cooperative: the merge gives up at its next partition and nothing has to be
     * rebuilt, so unlike the two JDBC paths this one needs no reconnect afterwards.
     */
    static final String CANCELLED_MESSAGE =
            "Cancelled: the scan was stopped, so the reader abandoned the merge. The next query "
                    + "starts a new one.";

    private final CassandraSettings cassandraSettings;
    private final CqliteSettings settings;
    private final CassandraPath cassandra;
    private final ConnectionGate gate = new ConnectionGate("cqlite", RETRY_INTERVAL);

    /**
     * Registration and execution are one operation, as they are for the bulk reader: a scan lists
     * its directory again as it starts, and re-registering a name replaces the table under it.
     */
    private final ReentrantLock queryLock = new ReentrantLock();

    /** The directory registered per table, sorted for the Health page that shows them. */
    private final SortedMap<String, Path> registered = new ConcurrentSkipListMap<>();

    /**
     * Why each table that is not registered is not.
     *
     * <p>Worth reporting separately: an unflushed table and a missing mount are different problems
     * and only one of them will pass on its own.
     */
    private final Map<String, String> declined = new ConcurrentHashMap<>();

    private volatile CqliteLibrary library;
    private volatile BufferAllocator allocator;
    private volatile CqliteSession session;

    /** The statement in flight, for {@link #abort()} to cancel from another thread. */
    private volatile CqliteStatement running;

    private volatile boolean aborted;

    @Inject
    CqlitePath(CassandraSettings cassandraSettings, CqliteSettings settings, CassandraPath cassandra) {
        this.cassandraSettings = cassandraSettings;
        this.settings = settings;
        this.cassandra = cassandra;
    }

    @Override
    public String name() {
        return "cqlite";
    }

    @Override
    public void connect(boolean force) {
        gate.connect(force, this::registerMissing);
    }

    /** True once any table is registered: one unflushed table must not take the path away. */
    @Override
    public boolean connected() {
        return gate.connected();
    }

    @Override
    public boolean busy() {
        return queryLock.isLocked();
    }

    @Override
    public String dialect(String sql, int limit) {
        return Dialects.sql(sql, TABLE_PREFIX, limit);
    }

    /** The directory registered for each table. */
    public Map<String, Path> tables() {
        return Map.copyOf(registered);
    }

    /** Why each table that could not be registered could not be. */
    public Map<String, String> declined() {
        return Map.copyOf(declined);
    }

    /** What the library reports about itself, absent until a session has opened through it. */
    public Optional<String> buildInfo() {
        CqliteLibrary loaded = library;
        return Optional.ofNullable(loaded).map(CqliteLibrary::buildInfo);
    }

    /** What the registered directories hold at this moment. */
    public record FileCount(long tables, long files, long bytes) {}

    /**
     * What the registered directories hold now, read without opening an SSTable.
     *
     * <p>Cheap enough for a health probe to call. A table Cassandra has not flushed counts as
     * nothing rather than as a failure: it is a state a young stack passes through, and a probe that
     * raised there would report the whole path down.
     */
    public FileCount filesNow() {
        CqliteSession current = session;
        if (current == null) {
            return new FileCount(0, 0, 0);
        }
        long files = 0;
        long bytes = 0;
        for (String table : registered.keySet()) {
            try {
                Discovery found = current.discover(table);
                files += found.files();
                bytes += found.bytes();
            } catch (RuntimeException e) {
                LOG.debugf("cqlite could not discover %s: %s", table, e);
            }
        }
        return new FileCount(registered.size(), files, bytes);
    }

    /**
     * Stop the scan that is running.
     *
     * <p>False when there was nothing to stop. The reader polls the flag this sets and gives up at
     * its next partition, so a scan stops in a fraction of a second with no connection torn down.
     *
     * <p>{@link #busy()} and not the statement is what says a scan is running, because a statement
     * exists only once {@code query} has planned the scan and opened its files, which on a large
     * table is seconds. Reading the statement alone made a stop pressed in that window answer false
     * and then be discarded, so the scan ran to completion.
     */
    @Override
    public boolean abort() {
        if (!busy()) {
            return false;
        }
        aborted = true;
        CqliteStatement current = running;
        if (current == null) {
            // Nothing to cancel through the binding yet; read() tests the flag as soon as it holds
            // a statement, which is the same stop arriving a moment later.
            LOG.info("cqlite scan cancelled before it had a statement to cancel");
            return true;
        }
        boolean stopped = current.cancel();
        if (stopped) {
            LOG.info("cqlite scan cancelled");
        }
        return stopped;
    }

    @Override
    public QueryRows query(String sql) {
        ensureRegistered();
        queryLock.lock();
        try {
            // Cleared first, so the window in which abort() reports a stop it cannot deliver is the
            // one instruction between taking the lock and this line. Before the lock the path is
            // not busy and abort() answers false.
            aborted = false;
            CqliteSession current = session;
            if (current == null) {
                throw new EngineUnavailable("cqlite reader not connected");
            }
            return read(current, sql);
        } finally {
            queryLock.unlock();
        }
    }

    private QueryRows read(CqliteSession current, String sql) {
        try (CqliteStatement statement = current.query(sql)) {
            running = statement;
            try {
                if (aborted) {
                    statement.cancel();
                }
                List<String> columns = statement.columns();
                return new QueryRows(columns, drain(statement, columns), figures(statement));
            } catch (CqliteException e) {
                // The figures are still worth having: a scan that failed or was stopped had opened
                // its files, and how many it opened is part of why it did not finish.
                throw new EngineFailed(failureMessage(e), e, figures(statement));
            } finally {
                running = null;
            }
        }
    }

    /**
     * The rows as values in the projected order.
     *
     * <p>The binding spells a row as a map and the pages read values by position, so the projection
     * happens here rather than the binding growing a second row shape for one caller.
     */
    private static List<List<Object>> drain(CqliteStatement statement, List<String> columns) {
        List<List<Object>> rows = new ArrayList<>();
        statement.forEachBatch(batch -> {
            for (Map<String, Object> row : batch) {
                List<Object> values = new ArrayList<>(columns.size());
                for (String column : columns) {
                    values.add(row.get(column));
                }
                rows.add(values);
            }
        });
        return rows;
    }

    /**
     * What the statement read, or nothing where the figures themselves could not be read.
     *
     * <p>Lenient because this is called from the branch that reports a failure: a second failure
     * here would replace the one a viewer needs to see.
     */
    private static ReadFigures figures(CqliteStatement statement) {
        try {
            var scan = statement.scan();
            return ReadFigures.sstables(
                    scan.files(),
                    scan.bytes(),
                    Round.tenth(scan.readerOpenMillis()),
                    scan.dataAge().map(Duration::toSeconds).orElse(null));
        } catch (RuntimeException e) {
            LOG.debugf("cqlite figures unreadable: %s", e);
            return ReadFigures.NONE;
        }
    }

    /**
     * A cancelled scan and a failed one, told apart.
     *
     * <p>Both arrive as an error from the reader, and only the operator's intent distinguishes them:
     * someone who pressed stop should read that they stopped it.
     */
    private String failureMessage(CqliteException failure) {
        if (aborted || failure.cancelled()) {
            return CANCELLED_MESSAGE;
        }
        return Messages.oneLine(failure);
    }

    /**
     * Register whatever is not registered yet, at most once per {@link #RETRY_INTERVAL}.
     *
     * <p>The other four paths are probed by opening a socket, so a service that arrives late is
     * noticed on the next poll. This path has no socket and registration needs the keyspace to
     * exist, so a backend that started before the sink created the schema would register nothing and
     * never look again.
     *
     * <p>The partial case is looked at as well, and it is the one a clean start actually produces:
     * the sink creates the three tables one statement at a time, so a backend registering between
     * two of them gets {@code events} and neither of the others. The Python at first retried only
     * the all-or-nothing case, and that left the other two missing for the life of the process; it
     * failed the CI dashboard step at its first comparison, where cqlite reported "table
     * 'drone_latest_status' not found".
     *
     * <p>Public because the health probe calls it: the probe is what runs every few seconds whether
     * or not anyone queries this path, so it is where a late-arriving table is picked up.
     */
    public void ensureRegistered() {
        if (registered.size() == CQLITE_TABLES.size()) {
            return;
        }
        if (registered.isEmpty()) {
            connect();
            return;
        }
        gate.topUp(this::registerMissing);
    }

    /**
     * Open the session if it is not open, then register the tables it lacks.
     *
     * <p>Cassandra is asked for each table's {@code CREATE TABLE}, so the schema the files are
     * parsed with cannot drift from the schema that wrote them. That makes the path depend on the
     * CQL path being up once, to register; a query afterwards needs nothing but the files.
     *
     * <p>A table's own failure is reported and the rest carry on: an unflushed or dropped table must
     * not take away the tables that are readable. Registering nothing at all is the failure, and it
     * is what leaves the gate disconnected.
     */
    private void registerMissing() throws IOException {
        CqliteSession current = open();
        var keyspace = cassandra.session()
                .getMetadata()
                .getKeyspace(cassandraSettings.keyspace())
                .orElseThrow(() -> new EngineUnavailable(
                        "keyspace " + cassandraSettings.keyspace() + " does not exist yet"));
        OpenOptions options = new OpenOptions(
                settings.splits(), settings.batchRows(), settings.keyChunk());
        for (String table : CQLITE_TABLES) {
            if (registered.containsKey(table)) {
                continue;
            }
            Optional<TableMetadata> metadata = keyspace.getTable(table);
            if (metadata.isEmpty()) {
                declined.put(table, "the keyspace has no table of that name yet");
                continue;
            }
            try {
                Path directory = tableDirectory(table, metadata.get().getId());
                // Registering a name that is already held replaces the table under it, and a scan
                // already running keeps the reader it started with, so this is safe to repeat.
                current.registerTable(table, directory, createTableCql(metadata.get()), options);
                registered.put(table, directory);
                declined.remove(table);
                LOG.infof("cqlite table registered: %s at %s", table, directory);
            } catch (IOException | RuntimeException e) {
                declined.put(table, Messages.oneLine(e));
                LOG.warnf("cqlite table %s not registered: %s", table, Messages.oneLine(e));
            }
        }
        if (registered.isEmpty()) {
            throw new EngineUnavailable("no cqlite table could be registered: " + declined);
        }
    }

    /** The session, opening the library and one on the first call. */
    private CqliteSession open() {
        CqliteSession current = session;
        if (current != null) {
            return current;
        }
        CqliteLibrary loaded = CqliteLibrary.load(settings.library());
        LOG.infof("cqlite library loaded: %s, %s", settings.library(), loaded.buildInfo());
        BufferAllocator opened = new RootAllocator();
        CqliteSession started;
        try {
            started = loaded.openSession(opened);
        } catch (RuntimeException | Error e) {
            // The allocator is closed here rather than left to the next attempt: registration is
            // retried every RETRY_INTERVAL, so an openSession that keeps failing would abandon one
            // root allocator per attempt, each holding its own accounting for the life of the
            // process.
            opened.close();
            throw e;
        }
        allocator = opened;
        session = started;
        // Last of the three, so buildInfo() answers for a session that opened rather than for a
        // library that merely loaded: an openSession that fails leaves nothing behind but the log
        // line above, where a library set before it would have the Health page reporting a build
        // string for a path that cannot read a file.
        library = loaded;
        return started;
    }

    /**
     * The directory Cassandra keeps this table's files in.
     *
     * <p>The name is {@code <table>-<id>} with the id's dashes removed, and the id changes when a
     * table is dropped and recreated, so it is taken from the cluster rather than remembered: the
     * directory an earlier incarnation left behind is still on disk, and reading it would answer
     * from data the cluster has forgotten.
     */
    Path tableDirectory(String table, Optional<UUID> id) throws IOException {
        Path root = settings.dataDir().resolve(cassandraSettings.keyspace());
        Optional<Path> named = id.map(value -> root.resolve(
                table + "-" + value.toString().replace("-", "")));
        if (named.isPresent() && Files.isDirectory(named.get())) {
            return named.get();
        }
        // No id in the metadata, or none matching: take the one directory that matches, and refuse
        // to guess between several.
        List<Path> matches;
        try (Stream<Path> entries = Files.list(root)) {
            matches = entries.filter(Files::isDirectory)
                    .filter(entry -> entry.getFileName().toString().startsWith(table + "-"))
                    .sorted()
                    .toList();
        }
        if (matches.size() == 1) {
            return matches.getFirst();
        }
        if (matches.isEmpty()) {
            throw new NoSuchFileException(root.toString(), null,
                    "no directory for %s.%s; the data directory is mounted read-only from"
                            .formatted(cassandraSettings.keyspace(), table)
                            + " cqlite.data-dir");
        }
        throw new IOException("%d directories match %s.%s under %s and the cluster did not say which"
                .formatted(matches.size(), cassandraSettings.keyspace(), table, root)
                + " is current");
    }

    /**
     * This table's own {@code CREATE TABLE}, from the cluster's schema.
     *
     * <p>Without the trailing semicolon the driver appends and unformatted: the reader parses one
     * statement, and the semicolon makes it two.
     */
    static String createTableCql(TableMetadata metadata) {
        String described = metadata.describe(false).strip();
        return described.endsWith(";") ? described.substring(0, described.length() - 1) : described;
    }

    @PreDestroy
    void shutdown() {
        gate.invalidate();
        registered.clear();
        CqliteSession openSession = session;
        BufferAllocator openAllocator = allocator;
        session = null;
        allocator = null;
        if (openSession != null) {
            openSession.close();
        }
        if (openAllocator != null) {
            // After the session, and it reports a leak by throwing: a statement closes the child
            // allocator it was given, so anything outstanding here is this class having lost one.
            openAllocator.close();
        }
    }
}
