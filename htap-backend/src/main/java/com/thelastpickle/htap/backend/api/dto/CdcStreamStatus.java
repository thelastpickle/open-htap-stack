package com.thelastpickle.htap.backend.api.dto;

import java.util.List;

/**
 * What the tail is doing, in the terms the Streaming page shows.
 *
 * @param state {@code starting}, {@code waiting_for_topic}, {@code tailing} or {@code error}
 * @param consumed records consumed since this backend attached, which is a count and not an offset
 * @param decodeFailures how many of those could not be read
 * @param ratePerSec records a second over the interval between two polls that both saw records,
 *     rather than since startup: the write rate is changed from the Settings page, and an average
 *     since startup would hide that. It measures this consumer, so the publish rate is quoted from
 *     the topic's end offsets instead
 * @param latencyP50Ms the age at Kafka append over the records seen live, which measures the
 *     publisher rather than this tail
 */
public record CdcStreamStatus(
        String state,
        String topic,
        String bootstrap,
        String registry,
        List<Integer> partitions,
        int bufferSize,
        int buffered,
        long consumed,
        long decodeFailures,
        double ratePerSec,
        Double latencyP50Ms,
        Double latencyMaxMs,
        List<Integer> schemaIds,
        Long lastRecordAtMs,
        String error) {}
