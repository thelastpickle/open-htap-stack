package com.thelastpickle.htap.producer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The send loop: what one turn sends, and what the dashboard's controls do to the next one.
 *
 * <p>Over a recording sender rather than a broker, which is what the interface on the loop is for.
 * One turn is made observable by interrupting from inside the first send: the batch already in
 * flight finishes, the sleep that follows it answers the interrupt, and the loop returns. The
 * container's stop reaches the loop the same way, through the shutdown hook.
 */
class ProducerTest {

    /** A rate and a period that divide: 2,000 a second over 5 ms is ten events a turn. */
    private static final Map<String, String> FAST = Map.of(
            "BATCH_PERIOD_MS", "5",
            "EVENTS_PER_SEC", "2000",
            "N_ENTITIES", "10",
            "MAX_ENTITIES", "20",
            "SETTINGS_URL", "");

    private final ProducerSettings settings = ProducerSettings.from(env(FAST));
    private final List<String> logged = new ArrayList<>();
    private final Producer producer = new Producer(settings, logged::add);

    /** The batch is sized from the rate and the period, and it carries the configured topic. */
    @Test
    @Timeout(20)
    void oneTurnSendsTheBatchTheRateAsksFor() {
        Recorder sent = oneTurn(new LiveSettings(2000, 10, 5.0));

        assertEquals(10, settings.eventsPerBatch(2000));
        assertEquals(10, sent.keys.size());
        assertEquals(List.of("demo-events"), sent.topics.stream().distinct().toList());
    }

    /**
     * A paused fleet sends nothing, and the loop keeps turning.
     *
     * <p>Pause is a control on the Settings page, so it is worth pinning: nothing else asserts it,
     * and a pause that sent anyway would look like a producer ignoring the dashboard.  Ended by
     * interrupting from another thread, because a paused turn never reaches the sender.
     */
    @Test
    @Timeout(20)
    void aPausedFleetSendsNothing() throws InterruptedException {
        LiveSettings live = new LiveSettings(2000, 10, 5.0);
        live.apply(reported(OptionalInt.empty(), OptionalInt.empty(), Optional.of(true)));
        Recorder sent = new Recorder();

        Thread loop = Thread.ofPlatform().start(() -> run(sent, live));
        Thread.sleep(60);
        loop.interrupt();
        loop.join();

        assertEquals(List.of(), sent.keys, "a paused fleet sent records");
    }

    /** The rate is re-read every turn, so a change from the dashboard sizes the next batch. */
    @Test
    @Timeout(20)
    void aRateChangeSizesTheNextBatch() {
        LiveSettings live = new LiveSettings(2000, 10, 5.0);
        assertEquals(10, oneTurn(live).keys.size());

        live.apply(reported(OptionalInt.of(400), OptionalInt.empty(), Optional.empty()));

        assertEquals(2, oneTurn(live).keys.size(), "400 a second over a 5 ms period is two events");
    }

    /**
     * The cursor moves on within a turn, so the fleet is reported on in rotation.
     *
     * <p>Ten assets and a batch of ten is one pass over the fleet, so a repeat inside one turn would
     * mean the cursor had not advanced.
     */
    @Test
    @Timeout(20)
    void everyAssetIsReportedOnceInATurnOverTheWholeFleet() {
        Recorder sent = oneTurn(new LiveSettings(2000, 10, 5.0));

        assertEquals(10, sent.keys.stream().distinct().count(), sent.keys.toString());
    }

    /** A fleet larger than the ceiling is capped rather than indexing past the fleet's arrays. */
    @Test
    @Timeout(20)
    void aFleetLargerThanTheCeilingIsCapped() {
        LiveSettings live = new LiveSettings(2000, 10, 5.0);
        live.apply(reported(OptionalInt.of(8000), OptionalInt.of(9999), Optional.empty()));

        Recorder sent = oneTurn(live);

        assertEquals(40, sent.keys.size(), "8,000 a second over a 5 ms period is forty events");
        assertTrue(sent.keys.stream().distinct().count() <= 20,
                "more assets than MAX_ENTITIES were reported on");
    }

    /** One turn, ended from inside the first send so the batch that is running finishes. */
    private Recorder oneTurn(LiveSettings live) {
        Recorder sent = new Recorder();
        sent.stopAfterTheFirstSend = true;
        run(sent, live);
        // The loop returns with the flag set, since it answers the interrupt in its own sleep.
        Thread.interrupted();
        return sent;
    }

    private void run(Recorder sent, LiveSettings live) {
        producer.sendUntilInterrupted(
                sent,
                new Fleet(new FleetState(settings.fleet(), 1_787_846_133.0), settings.maxEntities()),
                live,
                TextSource.NONE);
    }

    private static LiveSettings.Reported reported(
            OptionalInt eventsPerSec, OptionalInt drones, Optional<Boolean> paused) {
        return new LiveSettings.Reported(eventsPerSec, drones, OptionalDouble.empty(), paused);
    }

    /** A sender that keeps what it was given, and can end the loop after the first record. */
    private static final class Recorder implements Producer.Sender {

        private final List<String> topics = new ArrayList<>();
        private final List<String> keys = new ArrayList<>();

        private boolean stopAfterTheFirstSend;

        @Override
        public synchronized void send(String topic, byte[] key, byte[] value) {
            topics.add(topic);
            keys.add(new String(key, StandardCharsets.UTF_8));
            if (stopAfterTheFirstSend) {
                // The loop finishes the batch it is in and then answers this in its sleep, so a
                // turn is exactly one batch however long the batch is.
                Thread.currentThread().interrupt();
            }
        }
    }

    private static UnaryOperator<String> env(Map<String, String> values) {
        return values::get;
    }
}
