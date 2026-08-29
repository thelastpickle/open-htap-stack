package com.thelastpickle.htap.backend.engine;

import com.thelastpickle.htap.backend.config.CassandraSettings;
import com.thelastpickle.htap.backend.config.SparkSettings;
import com.thelastpickle.htap.backend.query.Dialects;
import com.thelastpickle.htap.backend.support.Round;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.jboss.logging.Logger;

/**
 * The Cassandra Analytics bulk reader, over its own Thrift Server session.
 *
 * <p>It reads SSTable files straight from a coordinated snapshot through the Sidecar, so a scan
 * here never touches the CQL request path and cannot contend with the OLTP traffic. The price is
 * in the answer's currency: the rows are as of the snapshot rather than as of now.
 *
 * <p>Each read takes a fresh snapshot of only the tables the statement names, so the timing
 * includes what the mechanism costs. A snapshot lives until its TTL expires rather than until the
 * read finishes, so several are present at once; measured harmless, since a snapshot hard-links
 * files that were already live.
 */
@ApplicationScoped
public class SparkBulkPath implements QueryPath {

    private static final Logger LOG = Logger.getLogger(SparkBulkPath.class);

    /** As the Python's {@code RECONNECT_INTERVAL_S}. */
    static final Duration RETRY_INTERVAL = Duration.ofSeconds(10);

    /** Both Spark paths register the same three tables, this one under a prefix of its own. */
    static final String TABLE_PREFIX = "bulk_";

    /**
     * The cores the reader is given per query.
     *
     * <p>Four, against the Thrift Server's own {@code spark.cores.max} cap, so a bulk read cannot
     * take every executor and starve the jobs run beside it in the same container.
     */
    static final int NUM_CORES = 4;

    /** The clock in a snapshot's name, which is how its age is known without asking Cassandra. */
    private static final Pattern SNAPSHOT_STAMP = Pattern.compile("_(\\d{10,})_(\\d+)$");

    private static final AtomicLong SNAPSHOT_COUNTER = new AtomicLong(1);

    private final CassandraSettings cassandra;
    private final SparkSettings spark;
    private final Sidecar sidecar;
    private final ConnectionGate gate = new ConnectionGate("Spark bulk", RETRY_INTERVAL);
    private final ThriftConnection thrift;

    /**
     * Registering the view and running the statement have to be one operation.
     *
     * <p>The view name is fixed per table, so two bulk reads interleaving here would have the
     * second replace the first's view, and with it the snapshot the first was about to read,
     * between its registration and its {@code SELECT}.
     */
    private final ReentrantLock queryLock = new ReentrantLock();

    /**
     * The last snapshot taken per table, so a later read can be asked to read it again.
     *
     * <p>Held per path rather than per caller, because the point of reuse is that a snapshot
     * outlives the query that took it.
     */
    private final Map<String, String> lastSnapshot = new ConcurrentHashMap<>();

    @Inject
    SparkBulkPath(CassandraSettings cassandra, SparkSettings spark, Sidecar sidecar) {
        this.cassandra = cassandra;
        this.spark = spark;
        this.sidecar = sidecar;
        this.thrift = new ThriftConnection("bulk-reader", spark);
    }

    @Override
    public String name() {
        return "spark_bulk";
    }

    @Override
    public void connect(boolean force) {
        // No connector views on this session: it registers its own per query, and a session
        // carrying views it never reads would take three table definitions through the CQL path
        // for nothing, which is the path this one exists to avoid.
        gate.connect(force, thrift::open);
    }

    @Override
    public boolean connected() {
        return gate.connected();
    }

    @Override
    public boolean busy() {
        return thrift.busy();
    }

    @Override
    public boolean abort() {
        return thrift.abort();
    }

    @Override
    public boolean supportsSnapshotReuse() {
        return true;
    }

    @Override
    public String dialect(String sql, int limit) {
        return Dialects.sql(sql, TABLE_PREFIX, limit);
    }

    @Override
    public QueryRows query(String sql) {
        return query(sql, false);
    }

    /**
     * Prepare a snapshot of what the statement reads, size it, then run it.
     *
     * <p>{@code reusePrepared} reads the snapshot the last query took rather than taking another,
     * which skips the hard-link pass at the cost of answering as of when that snapshot was taken.
     * It falls back to a fresh snapshot whenever reuse would be wrong.
     */
    @Override
    public QueryRows query(String sql, boolean reusePrepared) {
        ready();
        queryLock.lock();
        try {
            double prepareMs = 0;
            boolean allReused = true;
            List<String> tables = tablesIn(sql);
            List<Double> ages = new ArrayList<>(tables.size());
            List<OptionalLong> measured = new ArrayList<>(tables.size());
            for (String table : tables) {
                long started = System.nanoTime();
                Prepared prepared = registerBulkView(table, reusePrepared);
                prepareMs += (System.nanoTime() - started) / 1_000_000.0;
                allReused &= prepared.reused();
                ageOf(prepared.snapshot()).ifPresent(ages::add);
                measured.add(sidecar.snapshotBytes(table, prepared.snapshot()));
            }
            Long total = totalBytes(measured);
            QueryRows rows = run(sql);
            // The oldest age, because that is the age of the least current thing read.
            Double oldest = ages.stream().max(Double::compare).map(Round::tenth).orElse(null);
            return rows.withFigures(ReadFigures.snapshot(
                    total,
                    Round.tenth(prepareMs),
                    // Reused only if every table's snapshot was: a statement reading two tables
                    // where one had to be re-taken paid the cost anyway.
                    !tables.isEmpty() && allReused,
                    oldest));
        } finally {
            queryLock.unlock();
        }
    }

    private QueryRows run(String sql) {
        try {
            return thrift.query(sql);
        } catch (EngineFailed e) {
            if (ThriftConnection.connectionIsGone(e) && !thrift.wasAborted()) {
                thrift.discard();
                gate.invalidate();
                throw new EngineFailed(
                        ("The bulk reader stopped answering within %ds. Reading the whole history"
                                        + " off SSTables is minutes of work, and beside the other"
                                        + " paths the job outlasts this guard. Spark carries on"
                                        + " with it, so cancel it from the Health page rather than"
                                        + " leaving it to compete with the next run; for a figure,"
                                        + " run the paths one at a time.")
                                .formatted(spark.queryTimeout().toSeconds()),
                        e);
            }
            throw e;
        }
    }

    private void ready() {
        if (!gate.connected()) {
            connect();
        }
    }

    /** A snapshot the view now points at, and whether it was already there. */
    private record Prepared(String snapshot, boolean reused) {}

    /**
     * How many bytes of SSTable files the snapshot holds, over the tables the statement reads.
     *
     * <p>Absent, and not the sum of what could be measured, when one table's size did not come back:
     * a short sum still looks whole, and the dashboard divides this figure by the read's duration to
     * quote a MB/s rate. Absent for a statement that reads no table, since zero bytes would read as
     * a snapshot with nothing in it.
     */
    static Long totalBytes(List<OptionalLong> measured) {
        if (measured.isEmpty()) {
            return null;
        }
        long summed = 0;
        for (OptionalLong one : measured) {
            if (one.isEmpty()) {
                return null;
            }
            summed += one.getAsLong();
        }
        return summed;
    }

    /**
     * Which tables this statement reads.
     *
     * <p>The statement arrives with its names already rewritten to the bulk views, so matching
     * them needs no SQL parsing, and only the tables actually read are snapshotted.
     */
    static List<String> tablesIn(String sql) {
        String lowered = sql.toLowerCase(Locale.ROOT);
        return SparkPath.REGISTERED_TABLES.stream()
                .filter(table -> lowered.contains(TABLE_PREFIX + table))
                .toList();
    }

    /**
     * The TTL a snapshot is created with.
     *
     * <p>A snapshot hard-links live SSTables, so until it expires it keeps them from being
     * compacted away: the TTL wants to be short. But it must outlast the read, and Cassandra
     * clears it mid-read otherwise: the components vanish and the read fails with "Required 1
     * replicas but only 0 responded", which is what a fixed fifteen minutes did to a
     * sixteen-minute contended run. So it is derived from the query timeout, which bounds any read
     * anybody is still waiting on, and doubled for the snapshot this query takes before that clock
     * starts.
     */
    Duration snapshotTtl() {
        long minutes = Math.max(15, Math.ceilDiv(spark.queryTimeout().toSeconds(), 60) * 2);
        return Duration.ofMinutes(minutes);
    }

    /**
     * How long ago this snapshot was taken, from the clock in its name.
     *
     * <p>Absent when the name carries no clock, which is a snapshot this path did not create.
     */
    private static Optional<Double> ageOf(String snapshot) {
        Matcher stamp = SNAPSHOT_STAMP.matcher(snapshot);
        if (!stamp.find()) {
            return Optional.empty();
        }
        double taken = Long.parseLong(stamp.group(1));
        return Optional.of(Math.max(0, Instant.now().getEpochSecond() - taken));
    }

    /**
     * The last snapshot of this table, if reading it again would be sound.
     *
     * <p>Three ways it would not be. Nothing has been taken yet. What was taken has since gone,
     * which the Sidecar answers by failing to list it, since Cassandra expires these on a TTL and
     * an operator can clear them by hand. Or too little of that TTL is left: Cassandra removes a
     * snapshot when its time is up regardless of who is reading, so one with less life left than a
     * read may take is no use.
     */
    private Optional<String> reusable(String table) {
        String snapshot = lastSnapshot.get(table);
        if (snapshot == null) {
            return Optional.empty();
        }
        Optional<Double> age = ageOf(snapshot);
        if (age.isEmpty()) {
            return Optional.empty();
        }
        double remaining = snapshotTtl().toSeconds() - age.get();
        if (remaining <= spark.queryTimeout().toSeconds()) {
            LOG.infof(
                    "snapshot %s has %ds of its TTL left, which is too little to read under;"
                            + " taking a fresh one",
                    snapshot, (long) remaining);
            return Optional.empty();
        }
        if (sidecar.snapshotBytes(table, snapshot).isEmpty()) {
            LOG.infof("snapshot %s is gone; taking a fresh one", snapshot);
            return Optional.empty();
        }
        return Optional.of(snapshot);
    }

    /**
     * Point the view at a snapshot, taking one unless an old one will serve.
     *
     * <p>A name carries the clock as well as a counter, so a restarted backend cannot ask for a
     * name an earlier run took and has not yet released, and the clock is what later tells the
     * snapshot's age.
     */
    private Prepared registerBulkView(String table, boolean reuse) {
        Optional<String> existing = reuse ? reusable(table) : Optional.empty();
        String snapshot = existing.orElseGet(() -> "htap_dashboard_%s_%d_%d"
                .formatted(table, Instant.now().getEpochSecond(), SNAPSHOT_COUNTER.getAndIncrement()));
        // Nothing to create and nothing to clear when reusing: the snapshot already carries the
        // TTL it was created with, and Cassandra enforces that whatever this reader says of it.
        //
        // The options are the analytics jar's own, which expects "{strategy [ttl]}" with the
        // strategy spelled exactly as its enum: an unrecognised value is not an error, it keeps
        // the snapshot for ever. OnCompletionOrTTL rather than OnCompletion, because the
        // completion hook does not fire for a statement issued through the Thrift Server, so the
        // TTL is what releases the snapshot and Cassandra enforces it whatever this process does.
        String strategy = existing.isPresent()
                ? "NoOp"
                : "OnCompletionOrTTL " + snapshotTtl().toMinutes() + "m";
        String ddl = """
                CREATE OR REPLACE TEMP VIEW %s%s \
                USING org.apache.cassandra.spark.sparksql.CassandraDataSource \
                OPTIONS (sidecar_contact_points '%s', keyspace '%s', table '%s', DC '%s', \
                createSnapshot '%s', snapshotName '%s', clearSnapshotStrategy '%s', numCores '%d')"""
                .formatted(
                        TABLE_PREFIX,
                        table,
                        cassandra.host(),
                        cassandra.keyspace(),
                        table,
                        cassandra.datacenter(),
                        existing.isPresent() ? "false" : "true",
                        snapshot,
                        strategy,
                        NUM_CORES);
        thrift.ddl(ddl);
        lastSnapshot.put(table, snapshot);
        return new Prepared(snapshot, existing.isPresent());
    }

    @PreDestroy
    void shutdown() {
        gate.invalidate();
        thrift.discard();
    }
}
