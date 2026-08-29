package com.thelastpickle.htap.cqlite;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_DOUBLE;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemoryLayout.PathElement;
import java.lang.foreign.StructLayout;
import java.lang.invoke.VarHandle;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.SequencedMap;

/**
 * The boundary as Java declares it: one entry per export, and one layout per struct.
 *
 * <p>This file is the whole of the coupling to {@code dist/cqlite_datafusion.h}, and
 * {@code CqliteAbiTest} reads that header and checks this file against it: the application
 * binary interface (ABI) version, the six codes, the thirteen names and the three struct
 * sizes and their fields. A C caller gets the struct check from the header's own {@code
 * _Static_assert} lines at compile time; a Panama caller gets it from that test.
 *
 * <p>The counts are {@code uint64_t} in C and {@code long} here, because Java has no
 * unsigned 64-bit integer. What that costs is stated where it matters: {@link
 * OpenOptions} refuses a negative count, since a negative {@code long} would cross as
 * a value near {@code u64::MAX} and the library would see an enormous length rather
 * than a mistake.
 */
final class Abi {

    /** The boundary version this binding is written against. */
    static final int VERSION = 1;

    /** How a table is opened. A zero field takes the library's own default. */
    static final StructLayout OPEN_OPTIONS = MemoryLayout.structLayout(
            JAVA_LONG.withName("splits"),
            JAVA_LONG.withName("batch_rows"),
            JAVA_LONG.withName("key_chunk"));

    /** What a table's directory holds now. */
    static final StructLayout DISCOVERY = MemoryLayout.structLayout(
            JAVA_LONG.withName("files"),
            JAVA_LONG.withName("bytes"),
            JAVA_LONG.withName("data_age_secs"));

    /** What one statement read. */
    static final StructLayout SCAN = MemoryLayout.structLayout(
            JAVA_LONG.withName("tables"),
            JAVA_LONG.withName("files"),
            JAVA_LONG.withName("bytes"),
            JAVA_DOUBLE.withName("reader_open_ms"),
            JAVA_LONG.withName("data_age_secs"));

    /*
     * A handle per field rather than an offset per field, so the carrier comes from the
     * layout above and not from the call site. An offset leaves the two free to disagree: a
     * header that respelled `double reader_open_ms` as `uint64_t` would be caught by the
     * layout test, and `out.get(JAVA_DOUBLE, offset)` would still compile and read the
     * integer's bit pattern as a double. A handle derived from a JAVA_LONG field refuses a
     * `(double)` call outright, at the first read rather than in a reported figure. The
     * refusal is what `withInvokeExactBehavior` buys: a handle left in its default mode
     * adapts the call site as `MethodHandle.asType` would, so a `long` field read as a
     * `double` widens silently and only the narrowing direction throws.
     */
    static final VarHandle OPTIONS_SPLITS = field(OPEN_OPTIONS, "splits");
    static final VarHandle OPTIONS_BATCH_ROWS = field(OPEN_OPTIONS, "batch_rows");
    static final VarHandle OPTIONS_KEY_CHUNK = field(OPEN_OPTIONS, "key_chunk");

    static final VarHandle DISCOVERY_FILES = field(DISCOVERY, "files");
    static final VarHandle DISCOVERY_BYTES = field(DISCOVERY, "bytes");
    static final VarHandle DISCOVERY_AGE = field(DISCOVERY, "data_age_secs");

    static final VarHandle SCAN_TABLES = field(SCAN, "tables");
    static final VarHandle SCAN_FILES = field(SCAN, "files");
    static final VarHandle SCAN_BYTES = field(SCAN, "bytes");
    static final VarHandle SCAN_READER_OPEN_MS = field(SCAN, "reader_open_ms");
    static final VarHandle SCAN_AGE = field(SCAN, "data_age_secs");

    /**
     * The seconds-since figure the boundary writes when there is no age to report.
     *
     * <p>Both {@code cqlite_discovery} and {@code cqlite_scan} use it, and the case is
     * ordinary rather than exceptional: a directory holding no file has no newest
     * file.
     */
    static final long NO_AGE = -1L;

    /**
     * Every export, in the header's order, with the descriptor Java calls it by.
     *
     * <p>Declared once and resolved in full at load, so a name the library does not
     * export fails there rather than at the first call that needs it. Thirteen, and
     * the test that counts them counts the header's declarations rather than this map.
     */
    static final SequencedMap<String, FunctionDescriptor> SIGNATURES = signatures();

    private Abi() {
    }

    private static SequencedMap<String, FunctionDescriptor> signatures() {
        SequencedMap<String, FunctionDescriptor> map = new LinkedHashMap<>();
        map.put("cqlite_abi_version", FunctionDescriptor.of(JAVA_INT));
        map.put("cqlite_build_info", FunctionDescriptor.of(ADDRESS));
        map.put("cqlite_session_open", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS));
        map.put("cqlite_session_close", FunctionDescriptor.ofVoid(ADDRESS));
        map.put("cqlite_register_table", FunctionDescriptor.of(
                JAVA_INT, ADDRESS, ADDRESS, ADDRESS, ADDRESS, ADDRESS, ADDRESS));
        map.put("cqlite_deregister_table",
                FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, ADDRESS));
        map.put("cqlite_discover",
                FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, ADDRESS, ADDRESS));
        map.put("cqlite_query", FunctionDescriptor.of(
                JAVA_INT, ADDRESS, ADDRESS, ADDRESS, ADDRESS, ADDRESS));
        map.put("cqlite_cancel", FunctionDescriptor.of(JAVA_INT, ADDRESS));
        map.put("cqlite_stmt_scan", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS));
        map.put("cqlite_stmt_close", FunctionDescriptor.ofVoid(ADDRESS));
        map.put("cqlite_error_message", FunctionDescriptor.of(ADDRESS, ADDRESS));
        map.put("cqlite_error_free", FunctionDescriptor.ofVoid(ADDRESS));
        // A sequenced map rather than Map.copyOf, which is unordered: the order is the
        // header's, so a reader can put the two files side by side.
        return Collections.unmodifiableSequencedMap(map);
    }

    /**
     * A handle taking the segment and a base offset, both exactly as declared.
     *
     * <p>{@code withInvokeExactBehavior} is what forces the offset to be written {@code 0L} and
     * not {@code 0}: with the default behaviour the call site is adapted as {@code
     * MethodHandle.asType} would adapt it, so an {@code int} offset widens, and so does a {@code
     * long} field read as a {@code double}. The offset is {@code 0L} everywhere here, because
     * each segment holds exactly this one struct.
     */
    private static VarHandle field(StructLayout layout, String name) {
        return layout.varHandle(PathElement.groupElement(name)).withInvokeExactBehavior();
    }
}
