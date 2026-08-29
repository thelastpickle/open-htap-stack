package com.thelastpickle.htap.sink;

import com.thelastpickle.htap.sink.Alerts.Proximity;
import com.thelastpickle.htap.sink.DroneTracker.Derived;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * What the sink writes, behind an interface so the batch rule can be tested without a cluster.
 *
 * <p>The three writes of one reading are returned rather than awaited, because the whole batch is
 * put in flight and awaited together: that is what makes the sink fast enough to keep up, and it is
 * also what makes the offset commit correct, since a commit before the acknowledgement would drop a
 * failed batch silently.
 */
interface Writes {

    /** The raw row, the per-asset history row and the live-status upsert, all in flight at once. */
    List<CompletionStage<?>> event(Event event, Derived derived, Proximity proximity, Instant now);

    /**
     * Adds to the ingestion counter for a 30-minute bucket.
     *
     * <p>Not awaited: the chart it feeds is an indicator rather than the data, and a counter write
     * that fails must not hold up the offsets of a batch already acknowledged.
     */
    void count(int records, String bucket);

    /** One alert row, not awaited for the same reason. */
    void alert(AlertRow alert);
}
