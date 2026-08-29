package com.thelastpickle.htap.backend.api.dto;

/** The session a caller steps through by hand, which every step's first guard reads. */
public record SessionOpened(String userId, String sessionId) {}
