package com.thelastpickle.htap.backend.cdc;

import java.util.List;
import java.util.Map;

/** Where the tail's records come from: one attach, then polls until something fails. */
interface CdcSource {

    /**
     * Opens a consumer and places it near the end of every partition.
     *
     * @throws TopicAbsent where the topic does not exist yet, which is an ordinary state
     */
    Attachment attach();

    /** Whatever has arrived within one poll timeout, which may be nothing. */
    List<Arrival> poll();

    /** Closes whatever {@link #attach} opened. A source with nothing open does nothing. */
    void close();

    /**
     * Asks the loop's poll to return now, from another thread.
     *
     * <p>The one call a Kafka consumer documents as safe from a second thread: it permits one thread
     * at a time and raises {@code ConcurrentModificationException} from its own guard, so a shutdown
     * that closed the consumer under a poll in flight would leave it open.
     */
    void wakeup();

    /** The broker the page names, whether or not anything is attached to it. */
    String bootstrap();

    /**
     * @param backfillUntil per partition, the offset that was the end of the log at attach.
     *     Anything below it was read to fill the buffer rather than seen arrive
     */
    record Attachment(List<Integer> partitions, Map<Integer, Long> backfillUntil) {}

    /** The topic appears when the publisher writes its first mutation, so this is not a failure. */
    final class TopicAbsent extends RuntimeException {

        private static final long serialVersionUID = 1L;

        TopicAbsent(String message) {
            super(message);
        }
    }
}
