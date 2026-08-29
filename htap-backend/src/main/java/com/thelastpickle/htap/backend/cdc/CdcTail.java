package com.thelastpickle.htap.backend.cdc;

import com.thelastpickle.htap.backend.api.dto.CdcRecord;
import com.thelastpickle.htap.backend.api.dto.CdcStreamStatus;
import com.thelastpickle.htap.backend.config.CdcSettings;
import com.thelastpickle.htap.backend.support.Messages;
import com.thelastpickle.htap.backend.support.Round;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;
import org.jboss.logging.Logger;

/**
 * The live tail behind the Streaming page: one poll loop, one ring buffer.
 *
 * <p>Cassandra hard-links each commit log segment into {@code cdc_raw} as it is discarded; the
 * Sidecar beside the node reads those segments, deserialises the mutations of a CDC-enabled table and
 * publishes them to Kafka as Confluent-framed Avro. So this is an ordinary Kafka consumer with a
 * schema lookup, and nothing here touches Cassandra: the mutations arrive from the commit log, not
 * from a query.
 *
 * <p>Two things bound what the page can cost. The buffer has a fixed size, so a page left open
 * overnight holds no more memory than one just opened; and the loop consumes whether or not anybody
 * is watching, so what the page shows is what the topic did rather than what it did since somebody
 * looked.
 */
@ApplicationScoped
public class CdcTail {

    private static final Logger LOG = Logger.getLogger(CdcTail.class);

    /** How long the loop waits before trying again after a failure or an absent topic. */
    static final Duration RETRY_DELAY = Duration.ofSeconds(5);

    /** Latency samples kept for the p50, over live records only. */
    static final int LATENCY_SAMPLES = 200;

    static final String STARTING = "starting";
    static final String WAITING_FOR_TOPIC = "waiting_for_topic";
    static final String TAILING = "tailing";
    static final String ERRORED = "error";

    private final CdcSource source;
    private final CdcDecoder decoder;
    private final SchemaRegistry registry;
    private final CdcSettings settings;
    private final LongSupplier nanoClock;

    private final Deque<CdcRecord> buffer = new ArrayDeque<>();
    private final Deque<Double> latencies = new ArrayDeque<>();

    /** Guards the buffer, the samples and the counters, which one thread writes and requests read. */
    private final Object lock = new Object();

    private long seq;
    private long consumed;
    private long decodeFailures;
    private double rate;
    private long rateMarkerAtNanos;
    private long rateMarkerCount;

    private volatile String state = STARTING;
    private volatile String error;
    private volatile boolean attached;
    private volatile List<Integer> partitions = List.of();
    private volatile Map<Integer, Long> backfillUntil = Map.of();
    private volatile Long lastRecordAtMs;
    private volatile boolean running = true;
    private volatile Thread loop;

    @Inject
    CdcTail(CdcSource source, CdcDecoder decoder, SchemaRegistry registry, CdcSettings settings) {
        this(source, decoder, registry, settings, System::nanoTime);
    }

    CdcTail(
            CdcSource source,
            CdcDecoder decoder,
            SchemaRegistry registry,
            CdcSettings settings,
            LongSupplier nanoClock) {
        this.source = source;
        this.decoder = decoder;
        this.registry = registry;
        this.settings = settings;
        this.nanoClock = nanoClock;
        this.rateMarkerAtNanos = nanoClock.getAsLong();
    }

    void onStart(@Observes StartupEvent event) {
        loop = Thread.ofVirtual().name("cdc-tail").start(this::run);
    }

    void onStop(@Observes ShutdownEvent event) {
        running = false;
        Thread current = loop;
        if (current != null) {
            current.interrupt();
        }
        source.close();
    }

    /** What the tail is doing, in the terms the page shows. */
    public CdcStreamStatus status() {
        synchronized (lock) {
            List<Double> live = latencies.stream().sorted().toList();
            return new CdcStreamStatus(
                    state,
                    settings.topic(),
                    source.bootstrap(),
                    settings.registry(),
                    partitions,
                    settings.bufferSize(),
                    buffer.size(),
                    consumed,
                    decodeFailures,
                    Round.tenth(rate),
                    live.isEmpty() ? null : Round.tenth(live.get(live.size() / 2)),
                    live.isEmpty() ? null : Round.tenth(live.getLast()),
                    registry.ids(),
                    lastRecordAtMs,
                    error);
        }
    }

    /**
     * The latest records, newest first.
     *
     * @param since only records minted after this sequence number, for a page polling for what it has
     *     not seen; null for the latest window whatever was seen before
     */
    public List<CdcRecord> records(int limit, Long since) {
        synchronized (lock) {
            List<CdcRecord> newestFirst = new ArrayList<>(Math.min(limit, buffer.size()));
            var oldestLast = buffer.descendingIterator();
            while (oldestLast.hasNext() && newestFirst.size() < limit) {
                CdcRecord record = oldestLast.next();
                if (since == null || record.seq() > since) {
                    newestFirst.add(record);
                }
            }
            return newestFirst;
        }
    }

    /**
     * The loop itself, started once and stopped at shutdown.
     *
     * <p>A broker that is not up yet, a topic that does not exist yet and a registry that is not
     * answering are all ordinary states on a stack that is minutes old, so each is reported and
     * retried rather than raised.
     */
    private void run() {
        while (running) {
            if (!tick() && !sleepRetryDelay()) {
                return;
            }
        }
    }

    /** One attach-and-poll, answering whether it got through without a failure. */
    boolean tick() {
        try {
            if (!attached) {
                CdcSource.Attachment attachment = source.attach();
                partitions = attachment.partitions();
                backfillUntil = attachment.backfillUntil();
                attached = true;
                state = TAILING;
            }
            ingest(source.poll());
            error = null;
            return true;
        } catch (CdcSource.TopicAbsent e) {
            state = WAITING_FOR_TOPIC;
            error = e.getMessage();
            detach();
            return false;
        } catch (RuntimeException e) {
            state = ERRORED;
            error = e.getClass().getSimpleName() + ": " + Messages.oneLine(e.getMessage());
            LOG.warnf("cdc tail failed: %s", error);
            detach();
            return false;
        }
    }

    /** Decodes what one poll returned and lets the same number of records leave the buffer. */
    private void ingest(List<Arrival> arrivals) {
        for (Arrival arrival : arrivals) {
            long at;
            boolean backfill;
            synchronized (lock) {
                at = ++seq;
                consumed++;
                backfill = arrival.offset() < backfillUntil.getOrDefault(arrival.partition(), 0L);
            }
            // Decoded outside the lock: it may fetch a schema, and a request asking for the status
            // should not wait on the registry.
            CdcRecord record = decoder.decode(at, arrival, backfill);
            synchronized (lock) {
                keep(record);
            }
        }
        if (!arrivals.isEmpty()) {
            markRate();
        }
    }

    /** Adds one record, drops the oldest if the buffer is full, and takes its figures. */
    private void keep(CdcRecord record) {
        buffer.addLast(record);
        while (buffer.size() > settings.bufferSize()) {
            buffer.removeFirst();
        }
        if (record.decodeError() != null) {
            decodeFailures++;
        }
        if (record.ageMs() != null) {
            latencies.addLast(record.ageMs());
            while (latencies.size() > LATENCY_SAMPLES) {
                latencies.removeFirst();
            }
        }
        lastRecordAtMs = record.kafkaAtMs() > 0 ? record.kafkaAtMs() : record.mutationAtMs();
    }

    /**
     * Records a second over the interval between two polls that both saw records.
     *
     * <p>Not an average since startup: the write rate is changed from the Settings page, and an
     * average since startup would hide the change.
     */
    private void markRate() {
        synchronized (lock) {
            double elapsed = (nanoClock.getAsLong() - rateMarkerAtNanos) / 1e9;
            if (elapsed >= 1.0) {
                rate = (consumed - rateMarkerCount) / elapsed;
                rateMarkerAtNanos = nanoClock.getAsLong();
                rateMarkerCount = consumed;
            }
        }
    }

    private void detach() {
        attached = false;
        source.close();
    }

    /** Sleeps the retry delay, answering whether the loop should go round again. */
    private boolean sleepRetryDelay() {
        try {
            Thread.sleep(RETRY_DELAY);
            return running;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
