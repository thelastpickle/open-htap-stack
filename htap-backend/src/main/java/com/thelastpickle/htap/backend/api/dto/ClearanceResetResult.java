package com.thelastpickle.htap.backend.api.dto;

import java.util.List;

/** Every clearance given back, one line each, and the ledger the next run starts from. */
public record ClearanceResetResult(List<String> actions, ClearanceState state) {}
