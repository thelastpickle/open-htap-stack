package com.thelastpickle.htap.backend.api.dto;

/** One statement the console offers, with what it is meant to show. */
public record SqlPreset(String id, String title, String description, String sql) {}
