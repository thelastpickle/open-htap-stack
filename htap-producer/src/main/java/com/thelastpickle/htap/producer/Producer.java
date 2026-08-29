package com.thelastpickle.htap.producer;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Locale;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * The fleet, sending telemetry to Kafka at whatever rate the dashboard asks for.
 *
 * <p>One loop, one thread, a batch per period: the period decides how smooth the motion looks and
 * how many wakeups the process costs, and the rate decides how large a batch is. A second thread
 * polls the dashboard's controls, and nothing on the send path waits for it.
 *
 * <p>Nothing here is durable and that is deliberate; {@link Topics#producerConfig} says why beside
 * {@code acks}.
 */
public final class Producer {

    /**
     * How long {@code flush} is given at shutdown before the process leaves anyway.
     *
     * <p>Reached through the shutdown hook below, which is what makes it more than a constant:
     * {@code podman stop} sends SIGTERM and runs no {@code finally}, so without the hook the client
     * would drop whatever it had buffered.
     */
    private static final Duration FLUSH_TIMEOUT = Duration.ofSeconds(10);

    /** Where a batch goes. An interface so the loop can be driven without a broker. */
    interface Sender {

        void send(String topic, byte[] key, byte[] value);
    }

    private final ProducerSettings settings;
    private final Log log;

    Producer(ProducerSettings settings, Log log) {
        this.settings = settings;
        this.log = log;
    }

    public static void main(String[] args) {
        new Producer(ProducerSettings.fromEnvironment(), Log.STDOUT).run();
    }

    void run() {
        double startedAt = System.currentTimeMillis() / 1000.0;
        LiveSettings live = new LiveSettings(
                settings.eventsPerSec(), settings.nEntities(), settings.outlierPercent());
        Fleet fleet = new Fleet(new FleetState(settings.fleet(), startedAt), settings.maxEntities());

        SettingsPoller poller = startPolling(live);
        Thread sending = Thread.currentThread();
        // SIGTERM runs no finally block, so the stop has to reach the loop through a hook: it
        // interrupts the send thread and waits for it, which is what lets the flush below happen.
        Runtime.getRuntime().addShutdownHook(Thread.ofPlatform().unstarted(() -> {
            sending.interrupt();
            try {
                sending.join(FLUSH_TIMEOUT);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));
        // The producer is closed with a bound rather than in a try-with-resources, because
        // close() with no argument waits for every outstanding send however long that takes, and
        // a shutdown of a lossy telemetry producer should not.
        KafkaProducer<byte[], byte[]> producer = null;
        try (TextSource text = corpus()) {
            producer = new KafkaProducer<>(Topics.producerConfig(settings));
            Topics.ensure(
                    settings.bootstrap(),
                    Topics.wanted(
                            settings.topic(), settings.topicPartitions(), settings.topicReplication()),
                    log);
            // Locale.ROOT on every line carrying a number, for the reason EventPartitions records
            // beside its own formatter: under fa-IR the default locale prints Persian digits, and a
            // log line nobody can grep is worse than no line.
            log.say(String.format(
                    Locale.ROOT,
                    "started bootstrap=%s topic=%s eps=%d n_entities=%d max_entities=%d"
                            + " batch_period_ms=%d",
                    settings.bootstrap(),
                    settings.topic(),
                    settings.eventsPerSec(),
                    settings.nEntities(),
                    settings.maxEntities(),
                    settings.batchPeriod().toMillis()));
            KafkaProducer<byte[], byte[]> client = producer;
            sendUntilInterrupted(
                    (topic, key, value) -> client.send(new ProducerRecord<>(topic, key, value)),
                    fleet,
                    live,
                    text);
        } catch (IOException e) {
            log.say("could not open the text corpus: " + e.getMessage());
        } finally {
            poller.stop();
            if (producer != null) {
                producer.close(FLUSH_TIMEOUT);
            }
        }
    }

    /**
     * The send loop, which ends only on an interrupt.
     *
     * <p>Every turn re-reads the controls, so a rate or fleet change from the Settings page takes
     * effect on the next batch rather than on a restart.
     */
    void sendUntilInterrupted(Sender sender, Fleet fleet, LiveSettings live, TextSource text) {
        double periodSeconds = settings.batchPeriod().toMillis() / 1000.0;
        long reportEveryNanos = settings.reportEvery().toNanos();
        long lastReport = System.nanoTime();
        long totalSent = 0;
        long windowSent = 0;

        while (!Thread.currentThread().isInterrupted()) {
            long loopStartNanos = System.nanoTime();
            double nowSeconds = System.currentTimeMillis() / 1000.0;
            LiveSettings.Snapshot now = live.snapshot();

            if (!now.paused()) {
                // The fleet's arrays are sized for maxEntities, so a request for more is capped
                // rather than allowed to index past the end.
                int liveEntities = Math.min(now.nEntities(), settings.maxEntities());
                int[] ids = fleet.next(settings.eventsPerBatch(now.eventsPerSec()), liveEntities);
                for (Fleet.Event event : fleet.batch(
                        ids,
                        nowSeconds,
                        periodSeconds,
                        text,
                        settings.textRefreshMinS(),
                        settings.textRefreshMaxS(),
                        now.outlierPercent() / 100.0)) {
                    sender.send(settings.topic(), event.key(), event.value());
                }
                windowSent += ids.length;
            }

            if (!sleepRestOfPeriod(loopStartNanos)) {
                return;
            }
            long elapsedSinceReport = System.nanoTime() - lastReport;
            if (elapsedSinceReport >= reportEveryNanos) {
                totalSent += windowSent;
                log.say(String.format(
                        Locale.ROOT,
                        "sent_total=%d (~%.0f/s)",
                        totalSent,
                        windowSent / (elapsedSinceReport / 1e9)));
                windowSent = 0;
                lastReport = System.nanoTime();
            }
        }
    }

    private SettingsPoller startPolling(LiveSettings live) {
        SettingsPoller poller = SettingsPoller.overHttp(
                settings.settingsUrl(), live, settings.settingsPollInterval(), log);
        if (settings.settingsUrl().isEmpty()) {
            // Nothing to poll, and the poller is still returned so that the caller's shutdown has
            // one thing to stop rather than a branch.
            return poller;
        }
        Thread.ofPlatform().name("settings-poller").daemon().start(poller);
        log.say("polling " + settings.settingsUrl() + " for live settings");
        return poller;
    }

    private TextSource corpus() throws IOException {
        if (settings.textFile().isEmpty()) {
            return TextSource.NONE;
        }
        CorpusSampler sampler = new CorpusSampler(Path.of(settings.textFile()));
        log.say("text payloads sampled from " + settings.textFile());
        return sampler;
    }

    /** Sleeps out the rest of the period, answering whether the loop should go round again. */
    private boolean sleepRestOfPeriod(long loopStartNanos) {
        long remaining = settings.batchPeriod().toNanos() - (System.nanoTime() - loopStartNanos);
        if (remaining <= 0) {
            // Behind the cadence: the next turn starts at once, which is the Python's behaviour and
            // is what lets a slow batch be absorbed rather than compounding into a drift.
            return true;
        }
        try {
            Thread.sleep(Duration.ofNanos(remaining));
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
