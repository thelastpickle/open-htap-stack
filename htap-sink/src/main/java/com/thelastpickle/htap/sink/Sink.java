package com.thelastpickle.htap.sink;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thelastpickle.htap.common.BucketKeys;
import com.thelastpickle.htap.sink.Alerts.Proximity;
import com.thelastpickle.htap.sink.DroneTracker.Derived;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletionStage;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/**
 * Consumes the event topic into Cassandra and derives what the dashboard reads.
 *
 * <p>Per reading it appends to the raw event table, which Presto and the Spark bulk reader query;
 * appends to the per-asset history behind the flight trails; upserts the one-row-per-asset table
 * behind the live map; derives speed, heading and flight state from the previous reading for that
 * asset; and scores the position against the restricted zones. It also owns the demo schema: see
 * {@link DemoSchema}.
 *
 * <p><b>A plain JVM application and not a Quarkus one.</b> It serves nothing: compose declares no
 * port and no healthcheck for this service, so an HTTP stack would be a dependency tree and a
 * startup cost for a process nobody calls. What that gives up is configuration validation and
 * dependency injection, and neither is missed at this size: {@link SinkSettings} is one record read
 * once, and the wiring below is a dozen lines in one place.
 *
 * <p><b>The offsets are committed after the batch is acknowledged, never before.</b> Every write is
 * an idempotent upsert, so a redelivered batch costs duplicate work and no duplicate data; a commit
 * before the acknowledgement would silently drop a failed batch, because the offsets would already
 * say it had been handled.
 */
public final class Sink {

    /** How long to wait before trying a service that is not up yet again. */
    static final Duration RETRY_DELAY = Duration.ofSeconds(5);

    /** One poll's wait, as the Python's {@code timeout_ms=1000}. */
    static final Duration POLL_TIMEOUT = Duration.ofSeconds(1);

    /**
     * The bound on one request to the node.
     *
     * <p>The Python driver's own default, which the Python sink took by not naming an execution
     * profile; this driver's default is 2 seconds, which a batch of 600 requests against a node
     * that is compacting does not reliably meet.
     */
    static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(10);

    private final SinkSettings settings;
    private final Writes writes;
    private final DroneTracker tracker;
    private final Alerts alerts;
    private final ObjectMapper json;
    private final Supplier<Instant> clock;
    private final LongSupplier nanoClock;

    private long totalInserted;
    private long windowInserted;

    Sink(SinkSettings settings, Writes writes, Alerts alerts) {
        this(settings, writes, alerts, Instant::now, System::nanoTime);
    }

    Sink(
            SinkSettings settings,
            Writes writes,
            Alerts alerts,
            Supplier<Instant> clock,
            LongSupplier nanoClock) {
        this.settings = settings;
        this.writes = writes;
        this.alerts = alerts;
        this.tracker = new DroneTracker();
        this.json = new ObjectMapper();
        this.clock = clock;
        this.nanoClock = nanoClock;
    }

    public static void main(String[] args) {
        SinkSettings settings = SinkSettings.fromEnvironment();
        Log.sink("%s", settings);

        CqlSession session = connectAndEnsureSchema(settings);
        Alerts alerts = new Alerts();
        Sink sink = new Sink(settings, new CassandraWrites(session, settings), alerts);
        try (Consumer<byte[], byte[]> consumer = subscribe(settings)) {
            sink.run(consumer, new Zones(session, settings.keyspace()));
        } finally {
            session.close();
        }
    }

    /**
     * The loop: poll, write, commit.
     *
     * <p>A failure in the poll or the commit is left to leave the process, as it was in the Python:
     * {@code restart: unless-stopped} restarts a sink that cannot talk to its broker, and a loop
     * that swallowed it would spin against a service that is gone.
     */
    void run(Consumer<byte[], byte[]> consumer, Zones zones) {
        reloadZones(zones);
        long lastZoneReload = nanoClock.getAsLong();
        long lastReport = nanoClock.getAsLong();

        while (true) {
            ConsumerRecords<byte[], byte[]> polled = consumer.poll(POLL_TIMEOUT);
            if (polled.isEmpty()) {
                continue;
            }
            if (elapsedSeconds(lastZoneReload) > settings.zoneReload().toSeconds()) {
                reloadZones(zones);
                lastZoneReload = nanoClock.getAsLong();
            }

            List<byte[]> values = new ArrayList<>(polled.count());
            for (ConsumerRecord<byte[], byte[]> record : polled) {
                values.add(record.value());
            }
            Batch batch = write(values);
            if (!batch.acknowledged()) {
                continue;
            }

            consumer.commitSync();
            if (batch.buffered() > 0) {
                writes.count(batch.buffered(), BucketKeys.thirtyMinute(clock.get()));
            }
            totalInserted += batch.buffered();
            windowInserted += batch.buffered();

            double elapsed = elapsedSeconds(lastReport);
            if (elapsed >= settings.reportEvery().toSeconds()) {
                Log.sink("total_inserted=%d (~%.0f/s)", totalInserted, windowInserted / elapsed);
                windowInserted = 0;
                lastReport = nanoClock.getAsLong();
            }
        }
    }

    /**
     * Derives and writes one batch, and says whether the offsets may be committed.
     *
     * <p>Every write of the batch is put in flight before any is awaited, so the batch overlaps in
     * the cluster; they are then awaited together, and one failure leaves the offsets where they
     * are so the batch is redelivered.
     */
    Batch write(List<byte[]> values) {
        List<CompletionStage<?>> pending = new ArrayList<>(values.size() * 3);
        int buffered = 0;
        for (byte[] value : values) {
            JsonNode parsed;
            try {
                parsed = json.readTree(value);
            } catch (Exception unreadable) {
                // Skipped rather than raised, which is a change from the Python: there the JSON
                // deserializer ran inside the poll, so one malformed record failed the process and
                // was redelivered to the next one for ever, since its offset was never committed.
                Log.sink("skipping a record that is not readable JSON: %s", unreadable);
                continue;
            }
            Instant now = clock.get();
            Event event = Event.from(parsed, now);
            Derived derived = tracker.update(
                    event.entityId(), event.latitude(), event.longitude(), event.altitudeM(),
                    event.eventTime());
            Proximity proximity = alerts.score(
                    event.entityId(), event.latitude(), event.longitude(), event.altitudeM(),
                    event.eventTime());
            proximity.alerts().forEach(writes::alert);
            pending.addAll(writes.event(event, derived, proximity, now));
            buffered++;
        }

        try {
            for (CompletionStage<?> stage : pending) {
                stage.toCompletableFuture().join();
            }
        } catch (RuntimeException e) {
            Log.sink("batch write failed, will retry from the last commit: %s", e);
            return new Batch(false, buffered);
        }
        return new Batch(true, buffered);
    }

    /** What one batch did: whether every write was acknowledged, and how many readings it held. */
    record Batch(boolean acknowledged, int buffered) {}

    /**
     * Reads the zones, keeping the ones already loaded if the read fails.
     *
     * <p>An unreadable table must not turn the alerting off silently, and the next reload is a
     * minute away.
     */
    private void reloadZones(Zones zones) {
        try {
            alerts.reload(zones.enabled());
        } catch (RuntimeException e) {
            Log.alert("could not load zones: %s", e);
        }
    }

    private double elapsedSeconds(long sinceNanos) {
        return (nanoClock.getAsLong() - sinceNanos) / 1e9;
    }

    /**
     * Connects and applies the schema, retrying until both work.
     *
     * <p>Both in the same loop and in this order, because a node that answers a connection is not
     * yet a node that will accept a schema change: a fresh stack has Cassandra opening its port
     * while it is still replaying, and the schema step is what has to succeed before a single row
     * can be written.
     */
    private static CqlSession connectAndEnsureSchema(SinkSettings settings) {
        while (true) {
            CqlSession session = null;
            try {
                session = CqlSession.builder()
                        // Unresolved, so the driver re-resolves the container name per connection
                        // rather than pinning the first answer.
                        .addContactPoint(InetSocketAddress.createUnresolved(
                                settings.cassandraHost(), settings.cassandraPort()))
                        .withLocalDatacenter(settings.datacenter())
                        // No keyspace: this session is what creates it, and every statement here
                        // names the keyspace anyway.
                        .withConfigLoader(DriverConfigLoader.programmaticBuilder()
                                .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, REQUEST_TIMEOUT)
                                .build())
                        .build();
                new SchemaOwner(session, settings).ensure();
                Log.sink("cassandra connected and schema ensured");
                return session;
            } catch (RuntimeException e) {
                if (session != null) {
                    session.close();
                }
                Log.sink("cassandra not ready yet: %s", e);
                sleep(RETRY_DELAY);
            }
        }
    }

    /** Subscribes to the topic, retrying until the broker answers. */
    private static Consumer<byte[], byte[]> subscribe(SinkSettings settings) {
        while (true) {
            try {
                KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties(settings));
                consumer.subscribe(List.of(settings.topic()));
                Log.sink("kafka consumer started");
                return consumer;
            } catch (RuntimeException e) {
                Log.sink("kafka not ready yet: %s", e);
                sleep(RETRY_DELAY);
            }
        }
    }

    private static Properties properties(SinkSettings settings) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, settings.bootstrap());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, settings.groupId());
        // The commit is the sink's own, after the batch is acknowledged; see the class comment.
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, settings.batchSize());
        properties.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return properties;
    }

    private static void sleep(Duration delay) {
        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while waiting for a service", e);
        }
    }
}
