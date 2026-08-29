package com.thelastpickle.htap.backend.vector;

/**
 * An embedding that must not be silently replaced by a local one.
 *
 * <p>Raised only where falling back would corrupt the index rather than merely degrade it: an
 * endpoint answering the wrong number of dimensions is a misconfiguration, and mixing its vectors
 * with locally embedded ones would leave one table holding two embedding spaces and every
 * similarity between them meaningless. Every other remote failure falls back and is logged.
 */
public class EmbeddingFailed extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public EmbeddingFailed(String message) {
        super(message);
    }
}
