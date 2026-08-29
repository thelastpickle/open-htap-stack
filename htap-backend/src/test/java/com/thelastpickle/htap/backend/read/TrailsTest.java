package com.thelastpickle.htap.backend.read;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class TrailsTest {

    private static List<Integer> rows(int count) {
        return IntStream.range(0, count).boxed().toList();
    }

    /**
     * The integer stride is the whole of the port: 2000 / 60 is 33, and every 33rd row of 2000
     * is 61 rows, one more than was asked for. Python's slice truncated that last row and so
     * does this, which is why the count is 60 and the last kept index is 1947 rather than 1980.
     */
    @Test
    void theStrideIsIntegerDivisionAndTheTailIsTruncated() {
        List<Integer> kept = Trails.thin(rows(2000), 60);

        assertEquals(60, kept.size());
        assertEquals(0, kept.get(0));
        assertEquals(33, kept.get(1));
        assertEquals(33 * 59, kept.get(59));
    }

    @Test
    void aReadShorterThanTheRequestIsKeptWhole() {
        assertEquals(rows(12), Trails.thin(rows(12), 60));
    }

    @Test
    void anEmptyReadThinsToNothing() {
        assertTrue(Trails.thin(List.of(), 60).isEmpty());
    }

    @Test
    void aStrideOfOneKeepsTheFirstPoints() {
        assertEquals(List.of(0, 1, 2, 3), Trails.thin(rows(7), 4));
    }
}
