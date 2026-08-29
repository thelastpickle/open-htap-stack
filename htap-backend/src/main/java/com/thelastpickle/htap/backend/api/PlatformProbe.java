package com.thelastpickle.htap.backend.api;

import com.thelastpickle.htap.backend.api.dto.ServiceHealth;
import com.thelastpickle.htap.backend.config.AccordSqlSettings;
import com.thelastpickle.htap.backend.config.CassandraSettings;
import com.thelastpickle.htap.backend.config.CqliteSettings;
import com.thelastpickle.htap.backend.config.KafkaSettings;
import com.thelastpickle.htap.backend.config.PrestoSettings;
import com.thelastpickle.htap.backend.config.SparkSettings;
import com.thelastpickle.htap.backend.engine.CqlitePath;
import com.thelastpickle.htap.backend.support.Round;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

/**
 * Whether each service in the stack is accepting connections.
 *
 * <p>A Transmission Control Protocol (TCP) connect and nothing more: it says the port is open,
 * not that the engine behind it would answer a query. That is the honest thing a dashboard
 * beside the services can say cheaply, and the paths themselves report their own failures
 * when asked to read.
 */
@ApplicationScoped
public class PlatformProbe {

    private static final int PROBE_TIMEOUT_MS = 2_000;

    /**
     * The Overview key performance indicators embed the score and are polled every few
     * seconds. Probing every socket on every poll would add seconds of latency whenever a
     * service is down, so the score is reused for a little longer than a poll cycle.
     */
    private static final long SCORE_TTL_NANOS = 10_000_000_000L;

    private record Target(String name, String host, int port) {}

    private record Reading(long expiresAtNanos, double score) {}

    private final List<Target> targets;
    private final CqliteSettings cqliteSettings;
    private final CqlitePath cqlite;
    private final LongSupplier nanoClock;
    private final AtomicReference<Reading> reading = new AtomicReference<>();

    @Inject
    PlatformProbe(
            CassandraSettings cassandra,
            KafkaSettings kafka,
            PrestoSettings presto,
            SparkSettings spark,
            AccordSqlSettings accordSql,
            CqliteSettings cqliteSettings,
            CqlitePath cqlite) {
        this(cassandra, kafka, presto, spark, accordSql, cqliteSettings, cqlite, System::nanoTime);
    }

    PlatformProbe(
            CassandraSettings cassandra,
            KafkaSettings kafka,
            PrestoSettings presto,
            SparkSettings spark,
            AccordSqlSettings accordSql,
            CqliteSettings cqliteSettings,
            CqlitePath cqlite,
            LongSupplier nanoClock) {
        this.cqliteSettings = cqliteSettings;
        this.cqlite = cqlite;
        // Every host and port comes from configuration, so the probe follows the backend
        // whether it runs inside the compose network or on the host. cassandra-sql is a
        // reachability row and nothing more: it is not one of the five paths.
        this.targets = List.of(
                new Target("Cassandra", cassandra.host(), cassandra.port()),
                new Target("Kafka", kafka.host(), kafka.port()),
                new Target("Presto", presto.host(), presto.port()),
                new Target("Spark", spark.uiHost(), spark.uiPort()),
                new Target("cassandra-sql", accordSql.host(), accordSql.port()));
        this.nanoClock = nanoClock;
    }

    /** Every service, probed now. */
    public List<ServiceHealth> services() {
        List<ServiceHealth> services = new ArrayList<>(targets.size() + 1);
        for (Target target : targets) {
            services.add(new ServiceHealth(
                    target.name(),
                    probe(target.host(), target.port()),
                    target.host() + ":" + target.port()));
        }
        services.add(cqliteReader());
        return services;
    }

    /**
     * The cqlite path, which has no socket to probe.
     *
     * <p>It is a library in this process reading a directory, so what stands in for reachability is
     * whether it found the SSTable files: a backend without the data directory mounted reports the
     * path down, and says which directory it looked in. The file count is what says the files are
     * there rather than only the directory.
     *
     * <p>Registration is retried here, because there is no socket whose opening would otherwise
     * show that the keyspace has since been created.
     */
    private ServiceHealth cqliteReader() {
        cqlite.ensureRegistered();
        CqlitePath.FileCount found = cqlite.filesNow();
        return new ServiceHealth(
                "cqlite reader",
                found.files() > 0 ? ServiceHealth.UP : ServiceHealth.DOWN,
                cqliteSettings.dataDir() + " — " + found.tables() + " table(s), "
                        + found.files() + " SSTable(s)");
    }

    /** Fraction of the services reachable, to a thousandth. */
    public double score(List<ServiceHealth> services) {
        if (services.isEmpty()) {
            return 0.0;
        }
        long up = services.stream().filter(ServiceHealth::up).count();
        return Round.thousandth((double) up / services.size());
    }

    /** The score, probing only when the last reading has expired. */
    public double score() {
        Reading held = reading.get();
        if (held != null && held.expiresAtNanos() - nanoClock.getAsLong() > 0) {
            return held.score();
        }
        return remember(score(services()));
    }

    /** Hold a score the caller has just measured, so a poll behind it need not probe. */
    public double remember(double score) {
        reading.set(new Reading(nanoClock.getAsLong() + SCORE_TTL_NANOS, score));
        return score;
    }

    private static String probe(String host, int port) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), PROBE_TIMEOUT_MS);
            return ServiceHealth.UP;
        } catch (IOException e) {
            // An unresolvable host arrives here too, which is a service that is not
            // running rather than a fault in this probe.
            return ServiceHealth.DOWN;
        } catch (RuntimeException e) {
            return ServiceHealth.UNKNOWN;
        }
    }
}
