package com.thelastpickle.htap.backend.query;

import com.thelastpickle.htap.backend.config.KafkaSettings;
import com.thelastpickle.htap.backend.support.Messages;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

/**
 * How far the sink has consumed, which is what says whether a closed window can still grow.
 *
 * <p>The compare page needs a claim about the writer, and Kafka is where the writer's progress is
 * recorded. The sink derives {@code event_bucket} from the event's own timestamp, so a sink behind
 * the topic keeps inserting into windows the clock has passed. CI established that with three
 * paths reading one closed window and returning 80,810, 81,697 and 82,869 rows, while the sink ran
 * some 645,900 records behind a producer at 1,899/s against its own 712/s.
 */
@ApplicationScoped
public class SinkProgress {

    /**
     * Seconds added to a window's end before asking which record first lies past it.
     *
     * <p>A Kafka record's timestamp is the producer's clock at send; the sink's event time comes
     * from the {@code event_id} timeuuid the same producer stamped a moment earlier in the same
     * loop. One clock, so the two differ only by the construction of one batch, and a minute is
     * far more margin than that needs. It costs a fifteen-minute window nothing: the flag turns
     * true a minute later than it strictly could.
     */
    static final Duration MARGIN = Duration.ofSeconds(60);

    /** What {@link OffsetSpec#forTimestamp} answers when no record on the partition is that late. */
    private static final long NO_RECORD_THAT_LATE = -1L;

    private static final DateTimeFormatter TARGET_FORMAT =
            DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneOffset.UTC);

    private final KafkaSettings settings;
    private final Supplier<Admin> admins;

    @Inject
    SinkProgress(KafkaSettings settings) {
        this(settings, null);
    }

    /** The seam the decision is tested through, so no test needs a broker. */
    SinkProgress(KafkaSettings settings, Supplier<Admin> admins) {
        this.settings = settings;
        this.admins = admins == null ? this::open : admins;
    }

    /** The verdict and the evidence for it, so a page and a CI failure can each say why. */
    public record Verdict(boolean settled, String detail) {}

    /**
     * Whether the sink has consumed every record that could still land in a window.
     *
     * <p>For each partition of the events topic, take the offset of the first record stamped at or
     * after the window's end and compare it with the offset the sink has committed: the sink
     * commits a batch only once every write in it is acknowledged, so a committed offset at or
     * past that one means nothing left to consume on that partition can be filed under this
     * window. A partition holding no record that late is settled when the sink has consumed all
     * of it.
     *
     * <p>Kafka not answering is reported as not settled, since an unknown writer position licenses
     * no claim.
     *
     * <p>One admin client does all three lookups, where the Python needed a consumer beside it:
     * its admin client could not ask for an offset by timestamp, and a consumer carrying the
     * sink's group could have committed over the sink's own progress. Java's {@code Admin} reads
     * committed offsets and offsets by timestamp alike and can commit neither, so the hazard and
     * the second client are both gone. The client is opened per check and closed after it, which
     * is the greater part of what this costs; the Python measured 520 ms of its 550 ms in opening
     * clients, against comparisons that take 8 to 40 s.
     */
    public Verdict consumedPast(Instant windowEnd) {
        Instant target = windowEnd.plus(MARGIN);
        String topic = settings.eventsTopic();
        Admin admin = null;
        try {
            admin = admins.get();
            List<TopicPartition> partitions = partitions(admin, topic);
            if (partitions.isEmpty()) {
                return new Verdict(false, "Kafka reported no partitions for " + topic);
            }
            return decide(
                    topic,
                    target,
                    partitions,
                    committed(admin, partitions),
                    offsets(admin, partitions, OffsetSpec.forTimestamp(target.toEpochMilli())),
                    offsets(admin, partitions, OffsetSpec.latest()));
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return new Verdict(false, "Kafka could not say where the sink is: " + Messages.oneLine(e));
        } finally {
            if (admin != null) {
                // Bounded, because close() otherwise waits out every request still in flight, and
                // a broker that has just timed out one lookup is the case this runs in.
                admin.close(Duration.ofSeconds(1));
            }
        }
    }

    /**
     * The verdict, given what the broker answered.
     *
     * <p>Every partition must pass. Testing one is what failed twice on CI: the sink polls with
     * {@code max_poll_records} across the topic's twelve partitions, so under a backlog their
     * positions diverge widely and one reaching the current window says nothing about the other
     * eleven. Measured on a stack four minutes old, the lag ran from 560 records on partition 0 to
     * 29,718 on partition 5, 130,173 in all, and the old test called that window settled.
     *
     * @param firstBeyond the first offset stamped at or after the target, {@code -1} where the
     *     partition holds no record that late
     * @param ends the offset each partition currently ends at
     */
    static Verdict decide(
            String topic,
            Instant target,
            List<TopicPartition> partitions,
            Map<TopicPartition, Long> committed,
            Map<TopicPartition, Long> firstBeyond,
            Map<TopicPartition, Long> ends) {
        SortedMap<Integer, Long> shortBy = new TreeMap<>();
        for (TopicPartition partition : partitions) {
            Long at = committed.get(partition);
            if (at == null || at < 0) {
                return new Verdict(false, "the sink has committed no offset on partition "
                        + partition.partition() + " of " + topic);
            }
            Long beyond = firstBeyond.get(partition);
            long needed;
            if (beyond != null && beyond != NO_RECORD_THAT_LATE) {
                needed = beyond;
            } else if (ends.containsKey(partition)) {
                // Nothing on this partition is stamped that late, so all of it belongs to this
                // window or an earlier one and all of it must be read.
                needed = ends.get(partition);
            } else {
                // Defaulting to the committed offset here would pass the partition on the
                // strength of an answer Kafka did not give.
                return new Verdict(false, "Kafka reported no end offset for partition "
                        + partition.partition() + " of " + topic);
            }
            if (at < needed) {
                shortBy.put(partition.partition(), needed - at);
            }
        }
        if (shortBy.isEmpty()) {
            return new Verdict(true, "all " + partitions.size() + " partitions of " + topic
                    + " are consumed past " + TARGET_FORMAT.format(target));
        }
        return new Verdict(false, shortfall(topic, target, partitions.size(), shortBy));
    }

    /**
     * How the short partitions are reported.
     *
     * <p>Summarised rather than listed: twelve partitions each named is a wall of text in a CI
     * failure, and the count, the total and the worst one are what a reader does anything with.
     */
    private static String shortfall(
            String topic, Instant target, int partitions, SortedMap<Integer, Long> shortBy) {
        Map.Entry<Integer, Long> worst = shortBy.entrySet().stream()
                .max(Map.Entry.comparingByValue())
                .orElseThrow();
        long total = shortBy.values().stream().mapToLong(Long::longValue).sum();
        return String.format(
                Locale.ROOT,
                "the sink has not consumed past %s: %d of %d partitions are short, %,d records in"
                        + " all, the worst partition %d by %,d",
                TARGET_FORMAT.format(target),
                shortBy.size(),
                partitions,
                total,
                worst.getKey(),
                worst.getValue());
    }

    private List<TopicPartition> partitions(Admin admin, String topic) throws Exception {
        TopicDescription description = admin.describeTopics(List.of(topic))
                .allTopicNames()
                .get(timeoutMs(), TimeUnit.MILLISECONDS)
                .get(topic);
        if (description == null) {
            return List.of();
        }
        return description.partitions().stream()
                .map(TopicPartitionInfo::partition)
                .sorted()
                .map(partition -> new TopicPartition(topic, partition))
                .toList();
    }

    private Map<TopicPartition, Long> committed(Admin admin, List<TopicPartition> partitions)
            throws Exception {
        String group = settings.sinkGroupId();
        Map<TopicPartition, OffsetAndMetadata> answered = admin
                .listConsumerGroupOffsets(Map.of(
                        group, new ListConsumerGroupOffsetsSpec().topicPartitions(partitions)))
                .partitionsToOffsetAndMetadata(group)
                .get(timeoutMs(), TimeUnit.MILLISECONDS);
        Map<TopicPartition, Long> offsets = new HashMap<>();
        answered.forEach((partition, offset) -> {
            // A group that has committed nothing on a partition is reported as a null value
            // rather than as an absent key, and the two mean the same thing here.
            if (offset != null) {
                offsets.put(partition, offset.offset());
            }
        });
        return offsets;
    }

    private Map<TopicPartition, Long> offsets(
            Admin admin, List<TopicPartition> partitions, OffsetSpec spec) throws Exception {
        Map<TopicPartition, OffsetSpec> asked = new HashMap<>();
        partitions.forEach(partition -> asked.put(partition, spec));
        Map<TopicPartition, Long> offsets = new HashMap<>();
        admin.listOffsets(asked)
                .all()
                .get(timeoutMs(), TimeUnit.MILLISECONDS)
                .forEach((partition, info) -> {
                    // An unanswered partition is left absent rather than recorded as -1, so that
                    // decide can tell "no record that late" from "no answer" and refuse the second.
                    if (info != null) {
                        offsets.put(partition, info.offset());
                    }
                });
        return offsets;
    }

    private long timeoutMs() {
        return settings.offsetsTimeout().toMillis();
    }

    private Admin open() {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                settings.host() + ":" + settings.port());
        properties.put(AdminClientConfig.CLIENT_ID_CONFIG, "htap-backend-sink-progress");
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) timeoutMs());
        // Both, because request.timeout.ms bounds one attempt and the client retries within
        // default.api.timeout.ms; the futures below are waited on for the same span, so leaving
        // the API timeout at its two-minute default would have the wait, not the client, give up.
        properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, (int) timeoutMs());
        return Admin.create(properties);
    }
}
