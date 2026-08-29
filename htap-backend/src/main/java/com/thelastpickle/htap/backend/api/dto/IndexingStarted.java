package com.thelastpickle.htap.backend.api.dto;

/** The answer to a bulk index, which runs on after the response. */
public record IndexingStarted(String status, String embedder, String message) {

    public static IndexingStarted of(String embedder) {
        return new IndexingStarted(
                "started", embedder, "Indexing started; results appear as rows are embedded.");
    }
}
