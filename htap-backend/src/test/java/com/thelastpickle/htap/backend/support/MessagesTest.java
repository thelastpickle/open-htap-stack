package com.thelastpickle.htap.backend.support;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/** What a dashboard column can show of a failure whose engine answers in frames. */
class MessagesTest {

    @Test
    void everyRunOfWhitespaceBecomesOneSpaceAndTheEndsAreTrimmed() {
        assertEquals("one two three", Messages.oneLine("  one\n\ttwo   three \n"));
    }

    @Test
    void nothingIsAnEmptyStringRatherThanNull() {
        assertEquals("", Messages.oneLine((String) null));
        assertEquals("", Messages.oneLine("   \n  "));
    }

    @Test
    void aLongMessageIsCutAtTheLimit() {
        String flattened = Messages.oneLine("x".repeat(Messages.LIMIT + 50));

        assertEquals(Messages.LIMIT, flattened.length());
        assertEquals("x".repeat(Messages.LIMIT), flattened);
    }

    /** Cut after flattening, so a wall of indented frames is not spent on its own whitespace. */
    @Test
    void theLimitIsMeasuredAfterFlatteningNotBefore() {
        String indented = ("at com.example.Frame\n" + " ".repeat(80)).repeat(20);

        assertEquals(Messages.LIMIT, Messages.oneLine(indented).length());
        assertEquals("at com.example.Frame at", Messages.oneLine(indented).substring(0, 23));
    }

    /** A failure carrying no message would otherwise report nothing at all. */
    @Test
    void aFailureWithNoMessageIsNamedByItsType() {
        assertEquals("IllegalStateException", Messages.oneLine(new IllegalStateException()));
        assertEquals("IllegalStateException", Messages.oneLine(new IllegalStateException("  ")));
        assertEquals("no route", Messages.oneLine(new IllegalStateException("no route")));
    }
}
