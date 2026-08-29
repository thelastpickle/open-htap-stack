package com.thelastpickle.htap.backend.api.dto;

import com.thelastpickle.htap.backend.query.ComparisonRun;
import java.util.List;
import java.util.Map;

/**
 * Everything in flight: the comparison holding the gate, and what the engines are working on.
 *
 * @param comparison null when no comparison is running, which is the ordinary state
 * @param unreadable why a path contributed no rows above, keyed by path. An engine that could not be
 *     reached belongs here rather than failing the page, and so do the two paths that keep no list to
 *     read: a page that silently omits three of the five paths reads as though they were idle.
 */
public record RunningWork(
        ComparisonRun comparison, List<QueryInFlight> queries, Map<String, String> unreadable) {}
