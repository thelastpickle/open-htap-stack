package com.thelastpickle.htap.cqlite;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** The sentinel the two figure records share: the boundary's {@code -1} is no age. */
class FiguresTest {

    @Test
    void discoveryCarriesTheAgeItWasGiven() {
        assertEquals(Optional.of(Duration.ofSeconds(42)), Discovery.of(4, 1024, 42).dataAge());
    }

    @Test
    void aDirectoryWithNoNewestFileHasNoAge() {
        assertTrue(Discovery.of(0, 0, Abi.NO_AGE).dataAge().isEmpty());
    }

    @Test
    void scanFiguresCarryTheSameSentinel() {
        ScanFigures figures = ScanFigures.of(1, 4, 1024, 12.5, Abi.NO_AGE);
        assertTrue(figures.dataAge().isEmpty());
        assertEquals(12.5, figures.readerOpenMillis(), "the figure is kept unrounded");
    }
}
