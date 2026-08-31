package com.thelastpickle.htap.producer;

import java.time.Duration;

/**
 * How many events one turn of the send loop sends, at any rate the Settings page offers.
 *
 * <p>The rate over the cadence is rarely a whole number, and flooring each turn on its own put a
 * floor under the achieved rate. The loop runs every 50 ms, so 5 events a second is a quarter of an
 * event a turn; {@code Math.max(1, (int) 0.25)} sent one, and the producer then ran at 20 a second
 * while reporting the 5 it had been asked for. The remainder is carried instead, so four turns of a
 * quarter send one event between them and the achieved rate is the asked-for rate.
 *
 * <p>A turn may therefore send nothing, which the loop has to skip rather than pass on: a batch of
 * no events divides the stamp window by zero.
 *
 * <p>What is carried is a fraction below one, so a rate the dashboard changes takes effect on the
 * next turn and no history of the old rate survives it. Nothing accumulates across a pause either,
 * because a paused loop does not ask.
 *
 * <p>Not named for a batch size: {@code ProducerSettings.kafkaBatchSize} is a count of bytes the
 * Kafka client buffers, and the two sharing a name would be the kind of collision that reads as
 * correct.
 */
final class BatchPacer {

    private final double periodSeconds;

    /** The fraction of an event left over from the turns so far, always in [0, 1). */
    private double owed;

    BatchPacer(Duration batchPeriod) {
        this.periodSeconds = batchPeriod.toMillis() / 1000.0;
    }

    /** How many events this turn sends, which may be none. */
    int next(int eventsPerSec) {
        owed += eventsPerSec * periodSeconds;
        int batch = (int) owed;
        owed -= batch;
        return batch;
    }
}
