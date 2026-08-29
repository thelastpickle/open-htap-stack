package com.thelastpickle.htap.backend.api.dto;

/** The process answering, and nothing about what it can reach. */
public record Liveness(String status, String timestamp) {}
