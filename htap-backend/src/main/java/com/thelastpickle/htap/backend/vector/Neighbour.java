package com.thelastpickle.htap.backend.vector;

/** One row the index returned, with the cosine similarity Cassandra scored it at. */
public record Neighbour(String entityId, String text, Double similarity) {}
