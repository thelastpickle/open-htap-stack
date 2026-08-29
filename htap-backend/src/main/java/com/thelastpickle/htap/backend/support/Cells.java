package com.thelastpickle.htap.backend.support;

import com.thelastpickle.htap.common.Timestamps;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.HexFormat;
import java.util.UUID;

/**
 * One spelling per value type, shared by every path that returns ad-hoc rows.
 *
 * <p>The comparison checks the five paths against each other row for row, so a value has to
 * arrive spelled the same way whichever engine read it. Each of the five hands back a
 * different Java type for the same Cassandra column: the CQL driver an {@link Instant}, the
 * two JDBC drivers a {@link java.sql.Timestamp}, and the cqlite reader an Arrow value already
 * converted by {@code ArrowRows}. Left alone, Jackson would render those three differently and
 * a comparison would report a disagreement about formatting as a disagreement about data.
 *
 * <p>The spelling to match is the Python's, since the pages were written against it; see
 * {@link Timestamps}.
 */
public final class Cells {

    private Cells() {}

    /** The value as every path reports it. */
    public static Object plain(Object value) {
        return switch (value) {
            case null -> null;
            case Instant instant -> Timestamps.iso(instant);
            // A JDBC timestamp holds no zone, and both servers here are configured for UTC,
            // so it is read as UTC rather than through the default zone: a container whose
            // TZ was set would otherwise shift every analytical timestamp against the CQL
            // path's, which is exactly the disagreement this class exists to prevent.
            case java.sql.Timestamp stamp ->
                    Timestamps.iso(stamp.toLocalDateTime().toInstant(ZoneOffset.UTC));
            case java.sql.Date date -> date.toLocalDate().toString();
            case java.sql.Time time -> time.toLocalTime().toString();
            case LocalDateTime at -> Timestamps.iso(at.toInstant(ZoneOffset.UTC));
            case LocalDate date -> date.toString();
            case LocalTime time -> time.toString();
            case UUID uuid -> uuid.toString();
            case InetAddress address -> address.getHostAddress();
            case byte[] bytes -> hex(bytes);
            case ByteBuffer buffer -> hex(remaining(buffer));
            // A decimal stays a decimal: Jackson writes it without an exponent, and turning
            // it into a double here would lose digits the engines do agree on.
            case BigDecimal decimal -> decimal;
            default -> value;
        };
    }

    /** {@code 0x…}, which is how {@code cqlsh} and the cqlite reader both print a blob. */
    private static String hex(byte[] bytes) {
        return "0x" + HexFormat.of().formatHex(bytes);
    }

    private static byte[] remaining(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        // A duplicate, so reading the bytes does not consume the caller's buffer: the driver
        // hands out the buffer it holds, and a second read of the same row would find it empty.
        buffer.duplicate().get(bytes);
        return bytes;
    }
}
