package com.thelastpickle.htap.sink;

import java.time.Duration;
import java.util.function.UnaryOperator;

/**
 * What the sink was started with, read from the environment.
 *
 * <p>The variable names are the ones {@code podman-compose.yml} declares for this service, which
 * are not the backend's spellings of the same values: the sink reads {@code TOPIC} and
 * {@code GROUP_ID} where the backend reads {@code KAFKA_EVENTS_TOPIC} and
 * {@code KAFKA_SINK_GROUP_ID}. Kept rather than renamed, because the compose file gives the
 * running stack both and a rename would silently move one service onto a default.
 *
 * <p>{@code EVENT_BUCKET_MINUTES} and {@code EVENT_SHARDS} are part of the data model rather
 * than tuning: rows keep the buckets and shards they were written with, so a stack whose sink
 * and backend disagree produces queries that match nothing. Compose declares both once and
 * gives them to both services.
 *
 * @param datacenter the datacenter the driver addresses nodes in. This driver refuses to build a
 *     session without it where the Python driver inferred one, so it is read here with the same
 *     default the backend uses and the same name; compose need not declare it, and the keyspace the
 *     schema creates names {@code datacenter1} in its replication either way
 * @param accordEnabled whether the three session tables and the three clearance tables are born
 *     transactional. Read from the same declaration the node reads, because the node refuses
 *     {@code transactional_mode='full'} at {@code CREATE TABLE} when Accord is off, and the
 *     refusal would stop the sink at its schema step and with it the whole demo
 * @param cdcEnabled whether {@code drone_latest_status} is followed by the Sidecar's publisher
 * @param batchSize how many records one poll may return. A batch fans out to three writes per
 *     event, all in flight at once, so it has to be small enough that the driver's request queue
 *     holds them: 200 events is 600 requests
 */
record SinkSettings(
        String bootstrap,
        String topic,
        String groupId,
        String cassandraHost,
        int cassandraPort,
        String datacenter,
        String keyspace,
        String table,
        int batchSize,
        Duration reportEvery,
        Duration zoneReload,
        int bucketMinutes,
        int shards,
        boolean accordEnabled,
        boolean cdcEnabled) {

    static SinkSettings fromEnvironment() {
        return from(System::getenv);
    }

    /** The same reading, over a lookup a test supplies. */
    static SinkSettings from(UnaryOperator<String> environment) {
        return new SinkSettings(
                text(environment, "KAFKA_BOOTSTRAP", "kafka:19092"),
                text(environment, "TOPIC", "demo-events"),
                text(environment, "GROUP_ID", "demo-cassandra-sink"),
                text(environment, "CASSANDRA_HOST", "cassandra"),
                integer(environment, "CASSANDRA_PORT", 9042),
                text(environment, "CASSANDRA_DATACENTER", "datacenter1"),
                text(environment, "KEYSPACE", "demo"),
                text(environment, "TABLE", "events"),
                Math.max(1, integer(environment, "BATCH_SIZE", 200)),
                seconds(environment, "REPORT_EVERY_S", 5.0),
                seconds(environment, "ZONE_RELOAD_S", 60.0),
                integer(environment, "EVENT_BUCKET_MINUTES", 15),
                integer(environment, "EVENT_SHARDS", 16),
                flag(environment, "CASSANDRA_ACCORD_ENABLED", true),
                flag(environment, "CASSANDRA_CDC_ENABLED", true));
    }

    /** What the startup line says, and the whole of what a reader needs to place a run. */
    @Override
    public String toString() {
        return "kafka=" + bootstrap + " topic=" + topic + " group_id=" + groupId
                + " cassandra=" + cassandraHost + ":" + cassandraPort + " " + keyspace + "." + table
                + " batch_size=" + batchSize;
    }

    private static String text(UnaryOperator<String> environment, String name, String fallback) {
        String value = environment.apply(name);
        return value == null || value.isEmpty() ? fallback : value;
    }

    /**
     * A number, or the default where the value is absent or unreadable.
     *
     * <p>Lenient because the Python was: {@code env_int} swallowed the failure and carried on, so
     * a typo in compose gave a running sink on defaults rather than a container that would not
     * start. Kept, since the two values that could not be defaulted safely are refused further in
     * by {@code EventPartitions}, which raises on a bucket width that does not divide 60 and on a
     * shard count below one.
     */
    private static int integer(UnaryOperator<String> environment, String name, int fallback) {
        try {
            return Integer.parseInt(text(environment, name, Integer.toString(fallback)).strip());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    private static Duration seconds(
            UnaryOperator<String> environment, String name, double fallback) {
        double value;
        try {
            value = Double.parseDouble(text(environment, name, Double.toString(fallback)).strip());
        } catch (NumberFormatException e) {
            value = fallback;
        }
        return Duration.ofMillis(Math.round(value * 1000));
    }

    /** As the Python read it: any spelling of "true" is true and everything else is false. */
    private static boolean flag(UnaryOperator<String> environment, String name, boolean fallback) {
        return text(environment, name, Boolean.toString(fallback)).equalsIgnoreCase("true");
    }
}
