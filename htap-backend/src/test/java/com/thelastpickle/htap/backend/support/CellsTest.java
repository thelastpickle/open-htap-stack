package com.thelastpickle.htap.backend.support;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * One spelling per value, whichever engine read the row.
 *
 * <p>Each of the five paths hands back a different Java type for the same Cassandra column, so
 * every case here is a type one path returns for a column another path returns differently. Left
 * alone they would serialise three ways and the comparison would report a formatting difference as
 * a disagreement about data.
 */
class CellsTest {

    private static final Instant AT = Instant.parse("2026-08-29T12:34:56.789Z");

    /**
     * The CQL driver's type against the two JDBC drivers', on the same UTC wall clock.
     *
     * <p>A JDBC timestamp holds no zone, and both servers here run in UTC, so the value the driver
     * hands over carries the server's wall clock and {@code Cells} reads it as UTC. That is what
     * keeps this test's answer the same in any test JVM's zone, and it is the same reason a backend
     * container with {@code TZ} set does not shift the analytical paths against the CQL one.
     */
    @Test
    void theThreeTimestampTypesEveryPathReturnsAgreeOnOneSpelling() {
        String expected = "2026-08-29T12:34:56.789000";

        assertEquals(expected, Cells.plain(AT));
        assertEquals(expected, Cells.plain(java.sql.Timestamp.valueOf("2026-08-29 12:34:56.789")));
        assertEquals(expected, Cells.plain(LocalDateTime.ofInstant(AT, java.time.ZoneOffset.UTC)));
    }

    @Test
    void aDateAndATimeAreSpelledTheSameFromEitherLibrary() {
        assertEquals("2026-08-29", Cells.plain(java.sql.Date.valueOf("2026-08-29")));
        assertEquals("2026-08-29", Cells.plain(LocalDate.parse("2026-08-29")));
        assertEquals("12:34:56", Cells.plain(java.sql.Time.valueOf("12:34:56")));
        assertEquals("12:34:56", Cells.plain(LocalTime.parse("12:34:56")));
    }

    @Test
    void aUuidAndAnAddressAreTheirOwnText() {
        UUID id = UUID.fromString("6f1e7a52-0a1d-11f0-9e5f-0242ac140002");

        assertEquals("6f1e7a52-0a1d-11f0-9e5f-0242ac140002", Cells.plain(id));
        assertEquals("172.20.0.10", Cells.plain(address("172.20.0.10")));
    }

    /** {@code 0x…}, which is how cqlsh and the cqlite reader both print a blob. */
    @Test
    void aBlobIsHexWhicheverContainerItArrivesIn() {
        byte[] bytes = "hi".getBytes(StandardCharsets.UTF_8);

        assertEquals("0x6869", Cells.plain(bytes));
        assertEquals("0x6869", Cells.plain(ByteBuffer.wrap(bytes)));
    }

    /**
     * The driver hands out the buffer it holds, so reading the bytes must not consume it: a second
     * read of the same row would otherwise find it empty and report a blob of no bytes.
     */
    @Test
    void readingABufferTwiceGivesTheSameHex() {
        ByteBuffer buffer = ByteBuffer.wrap("hi".getBytes(StandardCharsets.UTF_8));

        assertEquals("0x6869", Cells.plain(buffer));
        assertEquals("0x6869", Cells.plain(buffer));
        assertEquals(2, buffer.remaining());
    }

    /** A decimal stays a decimal: a double here would lose digits the engines do agree on. */
    @Test
    void aDecimalIsNotTurnedIntoADouble() {
        BigDecimal decimal = new BigDecimal("12345678901234567890.12345");

        assertSame(decimal, Cells.plain(decimal));
    }

    @Test
    void aValueNoPathSpellsDifferentlyIsPassedThrough() {
        assertNull(Cells.plain(null));
        assertEquals(7, Cells.plain(7));
        assertEquals("grounded", Cells.plain("grounded"));
        assertEquals(true, Cells.plain(true));
        assertEquals(1.5, Cells.plain(1.5));
    }

    private static InetAddress address(String text) {
        try {
            return InetAddress.getByName(text);
        } catch (UnknownHostException e) {
            throw new IllegalStateException(e);
        }
    }
}
