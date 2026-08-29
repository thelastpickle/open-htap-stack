package com.thelastpickle.htap.backend.api.dto;

import java.util.List;

/** The tail's counters and a window of its records, newest first. */
public record CdcStreamResponse(CdcStreamStatus status, List<CdcRecord> records) {}
