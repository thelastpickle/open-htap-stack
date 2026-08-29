package com.thelastpickle.htap.backend.cdc;

import com.thelastpickle.htap.backend.config.CdcSettings;
import com.thelastpickle.htap.backend.config.KafkaSettings;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/**
 * The tail's consumer: no group, no committed offsets, and a seek to near the end of each partition.
 *
 * <p>A tail rather than a subscriber, and that is the whole of the design: a restarted backend should
 * show what is arriving now, not replay from where the last one stopped. It reads back one buffer's
 * worth so a page opened after the fact still has something to show, and those records are flagged,
 * because their age measures the backlog rather than the publisher.
 */
@ApplicationScoped
public class KafkaCdcSource implements CdcSource {

    /** How long {@code close} waits for the consumer to leave; nothing here needs a graceful exit. */
    private static final CloseOptions CLOSING = CloseOptions.timeout(Duration.ofSeconds(5));

    private final CdcSettings settings;
    private final KafkaSettings kafka;

    private KafkaConsumer<byte[], byte[]> consumer;

    KafkaCdcSource(CdcSettings settings, KafkaSettings kafka) {
        this.settings = settings;
        this.kafka = kafka;
    }

    @Override
    public String bootstrap() {
        return kafka.host() + ":" + kafka.port();
    }

    @Override
    public Attachment attach() {
        KafkaConsumer<byte[], byte[]> opened = new KafkaConsumer<>(properties());
        try {
            List<PartitionInfo> known = opened.partitionsFor(settings.topic());
            if (known == null || known.isEmpty()) {
                throw new TopicAbsent("topic " + settings.topic() + " does not exist yet;"
                        + " the Sidecar creates it with its first published mutation");
            }
            List<TopicPartition> assignment = known.stream()
                    .map(info -> new TopicPartition(info.topic(), info.partition()))
                    .sorted(Comparator.comparingInt(TopicPartition::partition))
                    .toList();
            opened.assign(assignment);

            Map<TopicPartition, Long> ends = opened.endOffsets(assignment);
            Map<TopicPartition, Long> begins = opened.beginningOffsets(assignment);
            int backfillEach = backfillEach(settings.bufferSize(), assignment.size());
            Map<Integer, Long> backfillUntil = new LinkedHashMap<>();
            for (TopicPartition partition : assignment) {
                long end = ends.getOrDefault(partition, 0L);
                opened.seek(partition, startOffset(begins.getOrDefault(partition, 0L), end, backfillEach));
                backfillUntil.put(partition.partition(), end);
            }
            consumer = opened;
            return new Attachment(List.copyOf(backfillUntil.keySet()), backfillUntil);
        } catch (RuntimeException e) {
            opened.close(CLOSING);
            throw e;
        }
    }

    @Override
    public List<Arrival> poll() {
        List<Arrival> arrivals = new ArrayList<>();
        for (ConsumerRecord<byte[], byte[]> record : consumer.poll(settings.pollTimeout())) {
            arrivals.add(new Arrival(
                    record.partition(), record.offset(), record.key(), record.timestamp(), record.value()));
        }
        return arrivals;
    }

    @Override
    public void close() {
        KafkaConsumer<byte[], byte[]> open = consumer;
        consumer = null;
        if (open != null) {
            try {
                open.close(CLOSING);
            } catch (RuntimeException e) {
                // A close that fails leaves nothing to do: the next attach opens its own consumer.
            }
        }
    }

    /** How far back each partition is read, so that one bufferful is shared between them. */
    static int backfillEach(int bufferSize, int partitions) {
        return Math.max(1, bufferSize / Math.max(1, partitions));
    }

    /** Where a partition is read from: a bufferful back, or its beginning where it is shorter. */
    static long startOffset(long begin, long end, int backfillEach) {
        return Math.max(begin, end - backfillEach);
    }

    private Properties properties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "htap-cdc-tail");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        // Bounded so one poll cannot hand the decode more than the buffer holds.
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, settings.bufferSize());
        // Off, because a consumer that created the topic would hide the state the page reports: the
        // publisher creates it, with the partition count it wants, on its first mutation.
        properties.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
        // Both bounded well under the client's own defaults of 30 and 60 seconds: the metadata
        // lookup is what an unreachable broker blocks in, and the loop reports and retries, so a
        // minute inside one attach would only delay the report.
        properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5_000);
        properties.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 10_000);
        properties.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return properties;
    }
}
