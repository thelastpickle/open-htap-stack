package com.thelastpickle.htap.backend.api.dto;

import java.util.List;

/** One session's projection, as the transactions left it. */
public record SessionTimelineView(
        String userId, String sessionId, List<TransactionTimelineRow> timeline) {}
