package com.thelastpickle.htap.backend.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.thelastpickle.htap.backend.api.dto.ServiceHealth;
import com.thelastpickle.htap.backend.config.AccordSqlSettings;
import com.thelastpickle.htap.backend.config.CassandraSettings;
import com.thelastpickle.htap.backend.config.KafkaSettings;
import com.thelastpickle.htap.backend.config.PrestoSettings;
import com.thelastpickle.htap.backend.config.SparkSettings;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * A Quarkus test because the five service addresses come from configuration, and taking them
 * from the container is what makes this cover the mapping as well as the probe. Every one of
 * them points at a closed port under {@code %test}, so the probe is deterministic.
 */
@QuarkusTest
class PlatformProbeTest {

    @Inject
    CassandraSettings cassandra;

    @Inject
    KafkaSettings kafka;

    @Inject
    PrestoSettings presto;

    @Inject
    SparkSettings spark;

    @Inject
    AccordSqlSettings accordSql;

    private final AtomicLong clock = new AtomicLong();

    private PlatformProbe probe() {
        return new PlatformProbe(cassandra, kafka, presto, spark, accordSql, clock::get);
    }

    @Test
    void theFiveServicesAreNamedAndAddressedFromConfiguration() {
        List<ServiceHealth> services = probe().services();

        assertEquals(
                List.of("Cassandra", "Kafka", "Presto", "Spark", "cassandra-sql"),
                services.stream().map(ServiceHealth::name).toList());
        assertEquals(
                List.of("127.0.0.1:1", "127.0.0.1:1", "127.0.0.1:1", "127.0.0.1:1", "127.0.0.1:1"),
                services.stream().map(ServiceHealth::endpoint).toList());
    }

    /** A refused connection is a service that is not running, not a fault in the probe. */
    @Test
    void aClosedPortReadsAsDown() {
        List<ServiceHealth> services = probe().services();

        assertEquals(List.of("down", "down", "down", "down", "down"),
                services.stream().map(ServiceHealth::status).toList());
        services.forEach(service -> assertFalse(service.up()));
    }

    @Test
    void theScoreIsTheReachableFractionToAThousandth() {
        PlatformProbe probe = probe();

        assertEquals(0.0, probe.score(List.of()));
        assertEquals(0.0, probe.score(probe.services()));
        assertEquals(0.833, probe.score(mixed(1, 5)));
        assertEquals(0.667, probe.score(mixed(1, 2)));
        assertEquals(1.0, probe.score(mixed(0, 4)));
    }

    /**
     * The key performance indicator (KPI) poll asks for the score every few seconds, and
     * probing five sockets each time would add the connect timeout to a page load whenever a
     * service is down.
     */
    @Test
    void aRememberedScoreIsReusedUntilItExpires() {
        PlatformProbe probe = probe();
        probe.remember(0.75);

        assertEquals(0.75, probe.score());

        clock.addAndGet(9_999_999_999L);

        assertEquals(0.75, probe.score());

        clock.addAndGet(1L);

        // Expired, so this one probes: five closed ports, and 0.0 rather than the held 0.75.
        assertEquals(0.0, probe.score());
    }

    private static List<ServiceHealth> mixed(int down, int up) {
        List<ServiceHealth> services = new java.util.ArrayList<>();
        for (int i = 0; i < down; i++) {
            services.add(new ServiceHealth("down-" + i, ServiceHealth.DOWN, ""));
        }
        for (int i = 0; i < up; i++) {
            services.add(new ServiceHealth("up-" + i, ServiceHealth.UP, ""));
        }
        return services;
    }
}
