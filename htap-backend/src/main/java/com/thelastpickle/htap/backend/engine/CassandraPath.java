package com.thelastpickle.htap.backend.engine;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.thelastpickle.htap.backend.config.CassandraSettings;
import com.thelastpickle.htap.backend.query.Dialects;
import com.thelastpickle.htap.backend.support.Messages;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.function.Supplier;

/**
 * The CQL request path: the OLTP side of the demo, and the only path any other path is
 * compared against.
 */
@ApplicationScoped
public class CassandraPath implements QueryPath {

    /** As the Python's {@code RECONNECT_INTERVAL_S}. */
    static final Duration RETRY_INTERVAL = Duration.ofSeconds(10);

    /** As the Python's single execution profile, whose {@code request_timeout} was 15. */
    static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(15);

    private final CassandraSettings settings;
    private final ConnectionGate gate = new ConnectionGate("Cassandra", RETRY_INTERVAL);
    private final ConnectionGate.Attempt connectAction;
    private volatile CqlSession session;

    @Inject
    CassandraPath(CassandraSettings settings) {
        this(settings, null);
    }

    /**
     * The seam {@link #guarded} is tested through: an attempt that does nothing leaves the
     * gate connected with no cluster behind it, which is the state where a failed request
     * either invalidates or does not.
     */
    CassandraPath(CassandraSettings settings, ConnectionGate.Attempt connectAction) {
        this.settings = settings;
        this.connectAction = connectAction == null ? this::open : connectAction;
    }

    @Override
    public String name() {
        return "cassandra";
    }

    @Override
    public void connect(boolean force) {
        gate.connect(force, connectAction);
    }

    @Override
    public boolean connected() {
        return gate.connected();
    }

    @Override
    public String dialect(String sql, int limit) {
        return Dialects.cql(sql, limit);
    }

    /**
     * Runs one console statement.
     *
     * <p>What CQL cannot express is refused here, and the refusal is the finding the compare page
     * reports rather than a fault: a {@code GROUP BY} on a non-key column is the demo's own
     * example. So the driver's own message is passed on, and only a session that has gone takes
     * the path down.
     */
    @Override
    public QueryRows query(String sql) {
        try {
            return CqlRows.read(execute(SimpleStatement.newInstance(sql)));
        } catch (AllNodesFailedException e) {
            throw new EngineUnavailable("Cassandra not connected: " + Messages.oneLine(e), e);
        } catch (DriverException e) {
            throw new EngineFailed(Messages.oneLine(e), e);
        }
    }

    /**
     * Runs a statement, and marks the path disconnected if the session is what failed.
     *
     * <p>Every read goes through here so that a session which has stopped being usable is
     * noticed. Without it {@code connected()} would stay true on the strength of one
     * successful connect: a driver that has run out of hosts throws on every request, each
     * failure is answered with the empty shape, and the pages then show an empty fleet
     * beside a platform probe reporting Cassandra up, with no reconnect ever attempted.
     * That is the ten-hour failure the Python sink had, and this is where it is refused.
     *
     * <p>Reconnecting is then the gate's business, throttled to one attempt per {@link
     * #RETRY_INTERVAL}, so a cluster that is down costs one connect attempt every ten
     * seconds rather than one per read.
     */
    public ResultSet execute(Statement<?> statement) {
        return guarded(() -> session().execute(statement));
    }

    /**
     * Prepares a statement, through the same guard every request goes through.
     *
     * <p>The Python held its own prepared statements per statement text, under the lock the
     * connection used, and cleared them on a reconnect. None of that is needed here: this driver's
     * session keeps prepared statements itself, keyed by the statement, and {@code
     * CqlPrepareAsyncProcessor.process} consults them before going to the node, so preparing a text
     * it has already seen makes no round trip. Read out of the 4.19.3 bytecode rather than assumed,
     * because a per-call round trip would be invisible and would double the cost of every
     * transaction the demo times. The clearing comes for free too, since a reconnect builds a new
     * session and the old one's statements go with it.
     */
    public PreparedStatement prepare(String cql) {
        return guarded(() -> session().prepare(cql));
    }

    /**
     * Runs {@code request}, invalidating the gate if the session rather than the statement is
     * what failed.
     *
     * <p>Package-private and generic so that the invalidation itself is testable: reaching
     * this catch through {@link #execute} needs a driver call, and against a closed port
     * {@link #session()} refuses before one is made.
     */
    <T> T guarded(Supplier<T> request) {
        try {
            return request.get();
        } catch (RuntimeException e) {
            if (sessionIsGone(e)) {
                gate.invalidate();
            }
            throw e;
        }
    }

    /**
     * Whether a failed request means the session rather than the statement.
     *
     * <p>One failure does: {@link AllNodesFailedException}, the driver reporting that it has
     * no host left to try, with {@code NoNodeAvailableException} its subclass for the case
     * where the error map is empty. Anything else is the statement's own business, and
     * invalidating on a query error would drop a working session because one column name was
     * wrong.
     *
     * <p>{@code ClosedConnectionException} is deliberately not here, although it was at
     * first. It names one connection and not the session: the driver aborts the request, the
     * pool re-establishes the connection and the session stays usable. Invalidating on it
     * would rebuild the session on a blip the driver had already handled, fail every read
     * still paging through the old one, and report Cassandra down to the pages for up to
     * {@link #RETRY_INTERVAL}. Nor does dropping it lose a genuinely dead session: {@code
     * DefaultRetryPolicy.onRequestAborted} answers {@code RETRY_NEXT} only for a request the
     * caller declared idempotent and {@code RETHROW} otherwise, and nothing here declares it,
     * neither a statement nor an execution profile, the driver's own {@code
     * basic.request.default-idempotence} being false. So the aborted request is rethrown rather
     * than tried on another host, and the following request finds an empty pool and raises
     * {@code NoNodeAvailableException}, which closes the gate one request later.
     */
    static boolean sessionIsGone(RuntimeException failure) {
        return failure instanceof AllNodesFailedException;
    }

    /**
     * The session, connecting first if a previous attempt has not left one.
     *
     * @throws EngineUnavailable when there is no session to give
     */
    public CqlSession session() {
        if (!gate.connected()) {
            connect();
        }
        CqlSession current = session;
        if (current == null) {
            throw new EngineUnavailable("Cassandra not connected");
        }
        return current;
    }

    private void open() {
        ProgrammaticDriverConfigLoaderBuilder config = DriverConfigLoader.programmaticBuilder()
                .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, REQUEST_TIMEOUT);
        String advertised = settings.translateAddressesTo().filter(host -> !host.isBlank()).orElse("");
        if (!advertised.isEmpty()) {
            // The driver's own equivalent of the Python's FixedAddressTranslator: it returns
            // every discovered address as this host with the discovered port. Named without
            // its package, which the driver resolves against its internal translator
            // package. SubnetAddressTranslator is the other built-in and does not fit: it
            // reads subnet-addresses through getStringMap with no default, so an unset map
            // fails the session build rather than translating nothing.
            config.withString(DefaultDriverOption.ADDRESS_TRANSLATOR_CLASS,
                            "FixedHostNameAddressTranslator")
                    .withString(DefaultDriverOption.ADDRESS_TRANSLATOR_ADVERTISED_HOSTNAME, advertised);
        }
        CqlSession opened = CqlSession.builder()
                // Unresolved on purpose: the driver resolves a programmatic contact point
                // itself and re-resolves it per connection, which is what a container name
                // whose address can change needs. Resolving here would pin the first answer.
                .addContactPoint(InetSocketAddress.createUnresolved(settings.host(), settings.port()))
                .withLocalDatacenter(settings.datacenter())
                .withKeyspace(settings.keyspace())
                .withConfigLoader(config.build())
                .build();
        CqlSession previous = session;
        session = opened;
        if (previous != null) {
            // The Python dropped its old Cluster on a reconnect and left its threads and
            // sockets to the garbage collector; a driver-4 session holds a Netty event loop
            // group, so the same omission here would leak a pool per reconnect. Async
            // because a close waits for in-flight requests and this runs under the gate's
            // lock, where a reader is waiting.
            previous.closeAsync();
        }
    }

    @PreDestroy
    void close() {
        CqlSession current = session;
        session = null;
        gate.invalidate();
        if (current != null) {
            current.close();
        }
    }
}
