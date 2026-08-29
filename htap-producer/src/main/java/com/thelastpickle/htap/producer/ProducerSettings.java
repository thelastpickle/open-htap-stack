package com.thelastpickle.htap.producer;

import java.time.Duration;
import java.util.function.UnaryOperator;

/**
 * Everything this process reads from its environment, in one value.
 *
 * <p>A bad value falls back to the default rather than failing the start, which is the Python's
 * behaviour and is right for a generator: a mistyped rate should not stop the demo, and the
 * startup line prints what was actually adopted.
 *
 * <p>{@code TEXT_SEED} is deliberately absent. The Python passed it to its sampler and then never
 * used it: every snippet came from {@code sample_stable}, which seeds a generator of its own from
 * the asset and its refresh count, so the variable moved nothing.
 *
 * @param nEntities the starting fleet size, which the Settings page may change at run time
 * @param maxEntities the ceiling, which fixes how much state this process allocates up front
 * @param batchPeriod the send loop's cadence: shorter is smoother motion and more wakeups
 * @param settingsUrl empty to ignore the dashboard and stay on the values here
 */
record ProducerSettings(
        String bootstrap,
        String topic,
        String clientId,
        int topicPartitions,
        short topicReplication,
        int eventsPerSec,
        int nEntities,
        int maxEntities,
        double outlierPercent,
        Duration batchPeriod,
        String acks,
        int lingerMs,
        int kafkaBatchSize,
        String compression,
        String textFile,
        double textRefreshMinS,
        double textRefreshMaxS,
        String settingsUrl,
        Duration settingsPollInterval,
        Duration reportEvery,
        long fleetSeed,
        double centerLat,
        double centerLon,
        double latSpreadDeg,
        double lonSpreadDeg) {

    /** The shortest cadence the loop will run at, from the Python's own {@code max(5, ...)}. */
    private static final int MIN_BATCH_PERIOD_MS = 5;

    static ProducerSettings from(UnaryOperator<String> env) {
        int nEntities = Math.max(1, integer(env, "N_ENTITIES", 100));
        return new ProducerSettings(
                text(env, "KAFKA_BOOTSTRAP", "kafka:19092"),
                text(env, "TOPIC", "demo-events"),
                // Compose declares PRODUCER_CLIENT_ID and the Python never read it, so the broker
                // saw a generated id. Read here, because a named client is what makes this
                // producer identifiable in the broker's own metrics.
                text(env, "PRODUCER_CLIENT_ID", "demo-producer"),
                integer(env, "TOPIC_PARTITIONS", 12),
                (short) integer(env, "TOPIC_RF", 1),
                Math.max(1, integer(env, "EVENTS_PER_SEC", 2000)),
                nEntities,
                // The fleet's arrays are allocated once for the largest size the dashboard may
                // ask for, so the ceiling can never be below the starting size.
                Math.max(nEntities, integer(env, "MAX_ENTITIES", 2000)),
                number(env, "OUTLIER_PERCENT", 5.0),
                Duration.ofMillis(Math.max(MIN_BATCH_PERIOD_MS, integer(env, "BATCH_PERIOD_MS", 50))),
                // acks as text, because the client takes "0", "1" or "all" and the Python's
                // integer default of 0 is the same value spelled differently.
                acks(env),
                integer(env, "KAFKA_LINGER_MS", 20),
                integer(env, "KAFKA_BATCH_SIZE", 32768),
                // Empty means none, as an unset KAFKA_COMPRESSION did in the Python.
                text(env, "KAFKA_COMPRESSION", ""),
                text(env, "TEXT_FILE", ""),
                number(env, "TEXT_REFRESH_MIN_S", 5.0),
                number(env, "TEXT_REFRESH_MAX_S", 30.0),
                text(env, "SETTINGS_URL", ""),
                seconds(env, "SETTINGS_POLL_INTERVAL_S", 10.0),
                seconds(env, "REPORT_EVERY_S", 5.0),
                integer(env, "FLEET_SEED", 42),
                number(env, "FLEET_CENTER_LAT", 59.91),
                number(env, "FLEET_CENTER_LON", 10.75),
                number(env, "FLEET_LAT_SPREAD_DEG", 0.06),
                number(env, "FLEET_LON_SPREAD_DEG", 0.12));
    }

    static ProducerSettings fromEnvironment() {
        return from(System::getenv);
    }

    FleetConfig fleet() {
        return FleetConfig.of(
                maxEntities(), fleetSeed(), centerLat(), centerLon(), latSpreadDeg(), lonSpreadDeg());
    }

    /**
     * How many events one turn of the loop sends, at the rate asked for.
     *
     * <p>Named apart from {@link #kafkaBatchSize}, which is a count of bytes the client buffers:
     * the two are unrelated and sharing a name would be the kind of collision that reads as
     * correct.
     */
    int eventsPerBatch(int eventsPerSec) {
        return Math.max(1, (int) (eventsPerSec * batchPeriod.toMillis() / 1000.0));
    }

    /**
     * The acknowledgement setting, held to the four values the client takes.
     *
     * <p>Validated here because {@code ProducerConfig} raises on anything else, and that raise is
     * inside the constructor: {@code KAFKA_ACKS=true} would have killed the process at start and
     * left the restart policy looping it, where the Python read the same variable as an integer and
     * fell back to 0.  {@code KAFKA_COMPRESSION} is not validated the same way, because the client
     * accepts any codec name it knows and a wrong one is a start-up failure that says so.
     */
    private static String acks(UnaryOperator<String> env) {
        String asked = text(env, "KAFKA_ACKS", "0");
        return switch (asked) {
            case "all", "-1", "0", "1" -> asked;
            default -> "0";
        };
    }

    private static String text(UnaryOperator<String> env, String name, String fallback) {
        String value = env.apply(name);
        return value == null || value.isEmpty() ? fallback : value;
    }

    private static int integer(UnaryOperator<String> env, String name, int fallback) {
        try {
            return Integer.parseInt(text(env, name, String.valueOf(fallback)).strip());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    private static double number(UnaryOperator<String> env, String name, double fallback) {
        try {
            return Double.parseDouble(text(env, name, String.valueOf(fallback)).strip());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    private static Duration seconds(UnaryOperator<String> env, String name, double fallback) {
        return Duration.ofMillis(Math.round(number(env, name, fallback) * 1000.0));
    }
}
