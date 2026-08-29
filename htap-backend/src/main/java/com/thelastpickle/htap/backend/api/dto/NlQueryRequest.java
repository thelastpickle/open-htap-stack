package com.thelastpickle.htap.backend.api.dto;

/** A question in words, as the natural-language page sends it. */
public record NlQueryRequest(String prompt) {}
