package com.thelastpickle.htap.backend.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.junit.jupiter.api.Test;

/** What a refusal carries about the read that did not finish. */
class EngineFailedTest {

    @Test
    void aRefusalWithNoFiguresReportsThatItMeasuredNothing() {
        assertSame(ReadFigures.NONE, new EngineFailed("refused").figures());
        assertSame(
                ReadFigures.NONE,
                new EngineFailed("refused", new IllegalStateException()).figures());
    }

    @Test
    void theFiguresAReadHadTakenTravelWithTheRefusal() {
        ReadFigures measured = ReadFigures.sstables(4, 488_777_346L, 12.5, 30L);

        assertSame(measured, new EngineFailed("cancelled", null, measured).figures());
    }

    /**
     * Never null, although the field is transient: the record it holds is not serialisable, and a
     * deserialised failure would otherwise answer null to a caller that reports the figures.
     */
    @Test
    void figuresAreNeverNullEvenWhenNoneWereGiven() {
        assertSame(ReadFigures.NONE, new EngineFailed("refused", null, null).figures());
    }

    @Test
    void theCauseAndTheMessageAreTheOnesGiven() {
        IllegalStateException cause = new IllegalStateException("no route");
        EngineFailed failed = new EngineFailed("Presto query failed", cause);

        assertEquals("Presto query failed", failed.getMessage());
        assertSame(cause, failed.getCause());
    }
}
