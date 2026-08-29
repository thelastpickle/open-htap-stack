package com.thelastpickle.htap.backend.api.dto;

import java.util.Map;

/**
 * Which access paths are connected, for the engine selector.
 *
 * <p>Ordered as the dashboard shows them, which a {@code Map} literal would not be; the map handed
 * in keeps its insertion order through serialisation.
 */
public record EnginesResponse(Map<String, Boolean> engines) {}
