package com.thelastpickle.htap.backend.api.dto;

/** Turn the live embedder on or off. Boxed, so a body that says nothing is refused. */
public record LiveEmbeddingRequest(Boolean enabled) {}
