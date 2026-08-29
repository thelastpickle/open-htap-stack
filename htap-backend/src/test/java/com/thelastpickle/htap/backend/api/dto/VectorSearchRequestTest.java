package com.thelastpickle.htap.backend.api.dto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.junit.jupiter.api.Test;

class VectorSearchRequestTest {

    @Test
    void aBodyWithNoLimitTakesTheDefault() {
        VectorSearchRequest asked = new VectorSearchRequest("restricted airspace", null);

        assertEquals(VectorSearchRequest.DEFAULT_HITS, asked.hits());
        assertEquals(Optional.empty(), asked.outOfRange());
    }

    @Test
    void aLimitInsideItsRangeIsWhatIsAsked() {
        assertEquals(20, new VectorSearchRequest("q", 20).hits());
        assertEquals(Optional.empty(), new VectorSearchRequest("q", 1).outOfRange());
        assertEquals(
                Optional.empty(),
                new VectorSearchRequest("q", VectorSearchRequest.MOST_HITS).outOfRange());
    }

    /** The ceiling is what keeps the row count out of the statement's own limit clause. */
    @Test
    void aLimitOutsideItsRangeIsRefusedWithTheFigureNamed() {
        assertEquals(
                Optional.of("limit must be between 1 and 50, got 0"),
                new VectorSearchRequest("q", 0).outOfRange());
        assertEquals(
                Optional.of("limit must be between 1 and 50, got 51"),
                new VectorSearchRequest("q", 51).outOfRange());
    }

    /** An empty query is legal and embeds to the probe vector; a missing one is not. */
    @Test
    void aBodyWithNoQueryIsRefusedAndAnEmptyOneIsNot() {
        assertTrue(new VectorSearchRequest(null, 5).outOfRange().isPresent());
        assertEquals(Optional.empty(), new VectorSearchRequest("", 5).outOfRange());
    }
}
