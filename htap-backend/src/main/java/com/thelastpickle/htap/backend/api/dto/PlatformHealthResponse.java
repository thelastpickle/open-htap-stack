package com.thelastpickle.htap.backend.api.dto;

import java.util.List;

/**
 * @param containerCli the container command this stack runs under, so the page's copyable
 *     commands name one the operator has
 */
public record PlatformHealthResponse(
        List<ServiceHealth> services,
        double overallHealthScore,
        long totalDrones,
        String containerCli) {}
