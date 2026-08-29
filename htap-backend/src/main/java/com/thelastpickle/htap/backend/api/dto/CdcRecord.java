package com.thelastpickle.htap.backend.api.dto;

import java.util.List;
import java.util.Map;

/**
 * One mutation, as it arrived on the CDC topic.
 *
 * <p>{@code columns} is what the publisher's Avro record carries beside its own envelope fields, so
 * it is the table's own columns for the mutation that touched them. An {@code UPDATE} names only
 * what it wrote, which is why a row here can be sparse.
 *
 * @param seq monotonic within this backend's run, so the page can ask for what it has not seen
 *     without trusting Kafka offsets across a partition
 * @param key {@code keyspace:table:hash}, written by the publisher as a plain UTF-8 string
 * @param mutationAtMs the mutation's own write time, from the record
 * @param kafkaAtMs the broker's timestamp, from the message
 * @param ageMs how old the mutation was when it reached Kafka: {@code kafkaAtMs} less
 *     {@code mutationAtMs}, which is the publisher's own delay and stays true while this backend's
 *     consumer is behind. Absent on a record read from before the tail attached, whose age would
 *     measure the backlog instead
 * @param backfill a record the tail read to fill its buffer on attach rather than one it saw arrive
 * @param updateFields the columns the mutation itself named, from the envelope's
 *     {@code updateFields}. The publisher's record has a field per column of the table and fills the
 *     rest with null, so this is what tells a column written as null from one never touched
 * @param decodeError why a record could not be read, kept in the buffer rather than dropped: a
 *     record the dashboard cannot read is a finding
 */
public record CdcRecord(
        long seq,
        int partition,
        long offset,
        String key,
        String keyspace,
        String table,
        String operation,
        long mutationAtMs,
        long kafkaAtMs,
        Double ageMs,
        boolean backfill,
        boolean partial,
        Map<String, Object> columns,
        List<String> updateFields,
        Integer schemaId,
        String decodeError) {}
