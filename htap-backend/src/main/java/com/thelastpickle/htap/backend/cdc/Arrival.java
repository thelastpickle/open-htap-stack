package com.thelastpickle.htap.backend.cdc;

/**
 * One record as the broker handed it over, before anything has been read of its value.
 *
 * <p>The tail's own type rather than Kafka's, so the buffer, the counters and the decode can be
 * tested without a broker, and so nothing above this line holds a Kafka class.
 *
 * @param timestampMs when the broker appended the record, which with the mutation's own write time
 *     gives the publisher's delay
 */
record Arrival(int partition, long offset, byte[] key, long timestampMs, byte[] value) {}
