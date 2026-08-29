package com.thelastpickle.htap.cqlite;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.WrongMethodTypeException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/**
 * Checks {@link Abi} and {@link CqliteStatus} against the committed header.
 *
 * <p>The header is the declaration both sides are written to, and a C caller gets its
 * struct sizes checked by the {@code _Static_assert} lines at compile time. A Panama
 * caller has no such moment, so this test is it: the header is read and the two Java
 * files are compared with it, which is why the header travels beside the library rather
 * than being a copy left to drift.
 *
 * <p>Field names are compared as well as sizes, because a C caller and this binding are
 * not exposed to the same mistake. A C caller names each field and so survives a reorder;
 * {@link Abi} names each field too, but the offset behind the name comes from its own
 * layout, and every field of {@code cqlite_scan} is eight bytes wide, so swapping two of
 * them in the header keeps the size the header pins and leaves a handle reading the other
 * field. The size check stays beside the field check, since padding is what it catches and
 * a field list cannot.
 */
class CqliteAbiTest {

    private static final Path HEADER = Path.of("dist", "cqlite_datafusion.h");

    /** A {@code #define} of an integer, with the parentheses a negative one carries. */
    private static final Pattern DEFINE =
            Pattern.compile("^#define\\s+(CQLITE_\\w+)\\s+\\(?(-?\\d+)\\)?\\s*$");

    /** A declaration, which starts at column one where a comment and a parameter do not. */
    private static final Pattern DECLARATION = Pattern.compile("^[A-Za-z].*?\\b(cqlite_[a-z_]+)\\s*\\(");

    private static final Pattern STATIC_ASSERT =
            Pattern.compile("_Static_assert\\(sizeof\\((\\w+)\\)\\s*==\\s*(\\d+)");

    /** An anonymous struct and the name its typedef gives it, over a comment-free header. */
    private static final Pattern STRUCT =
            Pattern.compile("typedef\\s+struct\\s*\\{([^}]*)\\}\\s*(\\w+)\\s*;", Pattern.DOTALL);

    /** One field of such a struct: a type this boundary uses, then a name. */
    private static final Pattern FIELD =
            Pattern.compile("\\b(uint64_t|int64_t|double)\\s+(\\w+)\\s*;");

    /** What each C type crosses as. Java has no unsigned 64-bit integer, so both are long. */
    private static final Map<String, ValueLayout> CARRIERS = Map.of(
            "uint64_t", ValueLayout.JAVA_LONG,
            "int64_t", ValueLayout.JAVA_LONG,
            "double", ValueLayout.JAVA_DOUBLE);

    private static final Map<String, CqliteStatus> CODES = Map.of(
            "CQLITE_OK", CqliteStatus.OK,
            "CQLITE_ERROR", CqliteStatus.ERROR,
            "CQLITE_ERROR_NOT_READY", CqliteStatus.NOT_READY,
            "CQLITE_ERROR_SCHEMA", CqliteStatus.SCHEMA,
            "CQLITE_ERROR_BAD_ARGUMENT", CqliteStatus.BAD_ARGUMENT,
            "CQLITE_ERROR_PANIC", CqliteStatus.PANIC);

    private final List<String> lines = header();

    @Test
    void abiVersionMatchesTheHeader() {
        assertEquals(defines().get("CQLITE_ABI_VERSION").intValue(), Abi.VERSION, "CQLITE_ABI_VERSION");
    }

    @Test
    void everyStatusCarriesTheCodeTheHeaderDefines() {
        Map<String, Integer> defined = new LinkedHashMap<>(defines());
        defined.remove("CQLITE_ABI_VERSION");
        assertEquals(CODES.keySet(), defined.keySet(), "the header's codes");
        assertEquals(
                CqliteStatus.values().length,
                defined.size(),
                "one CqliteStatus per code the header defines");
        defined.forEach((name, code) -> assertEquals(code.intValue(), CODES.get(name).code(), name));
    }

    @Test
    void everyExportIsDeclaredInHeaderOrder() {
        Set<String> declared = new LinkedHashSet<>();
        for (String line : lines) {
            Matcher matcher = DECLARATION.matcher(line);
            if (matcher.find()) {
                declared.add(matcher.group(1));
            }
        }
        assertEquals(13, declared.size(), "exports declared in the header");
        assertEquals(
                List.copyOf(declared),
                List.copyOf(Abi.SIGNATURES.keySet()),
                "Abi.SIGNATURES, in the header's order");
    }

    @Test
    void everyStructMatchesThePinnedSize() {
        Map<String, Long> pinned = new LinkedHashMap<>();
        for (String line : lines) {
            Matcher matcher = STATIC_ASSERT.matcher(line);
            if (matcher.find()) {
                pinned.put(matcher.group(1), Long.parseLong(matcher.group(2)));
            }
        }
        Map<String, Long> declared = Map.of(
                "cqlite_open_options", Abi.OPEN_OPTIONS.byteSize(),
                "cqlite_discovery", Abi.DISCOVERY.byteSize(),
                "cqlite_scan", Abi.SCAN.byteSize());
        assertEquals(declared.keySet(), pinned.keySet(), "the structs the header pins");
        pinned.forEach((name, size) -> assertEquals(size, declared.get(name), name));
    }

    /** What the sizes cannot say: which field each offset in {@link Abi} reads. */
    @Test
    void everyStructCarriesTheHeadersFieldsInItsOrder() {
        Map<String, StructLayout> declared = Map.of(
                "cqlite_open_options", Abi.OPEN_OPTIONS,
                "cqlite_discovery", Abi.DISCOVERY,
                "cqlite_scan", Abi.SCAN);
        Map<String, List<MemoryLayout>> fromHeader = structs();
        assertEquals(declared.keySet(), fromHeader.keySet(), "the structs the header declares");
        fromHeader.forEach((name, fields) -> assertEquals(
                fields,
                declared.get(name).memberLayouts(),
                name + ", field for field in the header's order"));
    }

    @Test
    void theRefusedCountIsTheOneTheHeaderNames() {
        // Prose rather than a #define, so this checks the prose: a header that lowered the
        // bound would leave OpenOptions accepting a count the library refuses, and the
        // refusal names no field where OpenOptions.check does.
        assertTrue(
                prose().contains("refused above " + OpenOptions.MAX_COUNT),
                "the header still names " + OpenOptions.MAX_COUNT + " as the count it refuses above");
    }

    @Test
    void theAbsentAgeSentinelIsMinusOne() {
        // Documented in prose rather than as a #define, so this checks the prose is still
        // there: a header that stopped saying -1 would change what an empty age means.
        assertTrue(
                Pattern.compile("data_age_secs.{0,120}?or -1").matcher(prose()).find(),
                "the header still documents -1 as the absent age");
        assertEquals(-1L, Abi.NO_AGE);
    }

    /**
     * A field read as the wrong carrier throws rather than converting.
     *
     * <p>The reason {@link Abi} publishes handles instead of offsets is that a header which
     * respelled {@code double reader_open_ms} as {@code uint64_t} should fail at the read and
     * not in a reported figure. A handle left in its default mode would not do that: it
     * adapts the call site as {@code MethodHandle.asType} would, and a {@code long} widens to
     * a {@code double} without complaint. So the refusal rests on {@code
     * withInvokeExactBehavior}, and that one call is what this pins.
     */
    @Test
    void aFieldReadAsTheWrongTypeIsRefusedRatherThanWidened() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment scan = arena.allocate(Abi.SCAN);
            Abi.SCAN_TABLES.set(scan, 0L, 7L);

            assertEquals(7L, (long) Abi.SCAN_TABLES.get(scan, 0L));
            assertThrows(
                    WrongMethodTypeException.class,
                    () -> assertEquals(7.0, (double) Abi.SCAN_TABLES.get(scan, 0L)));
        }
    }

    /**
     * The header as one line with its whitespace collapsed.
     *
     * <p>The two prose checks read this rather than the lines, because the header is a build
     * artefact of the fork and a reflow that split one of the phrases over two lines would
     * otherwise report a bound as lowered where nothing about the bound had changed.
     */
    private String prose() {
        return String.join(" ", lines).replaceAll("\\s+", " ");
    }

    /**
     * Every typedef'd anonymous struct in the header, as the layout each field crosses as.
     *
     * <p>Comments are removed first, so a field name in prose cannot be read as a
     * declaration; the three opaque handles are typedefs of named structs and so match
     * neither pattern.
     */
    private Map<String, List<MemoryLayout>> structs() {
        String source = String.join("\n", lines).replaceAll("(?s)/\\*.*?\\*/", " ");
        Map<String, List<MemoryLayout>> found = new LinkedHashMap<>();
        Matcher struct = STRUCT.matcher(source);
        while (struct.find()) {
            List<MemoryLayout> fields = new ArrayList<>();
            Matcher field = FIELD.matcher(struct.group(1));
            while (field.find()) {
                ValueLayout carrier = CARRIERS.get(field.group(1));
                fields.add(carrier.withName(field.group(2)));
            }
            assertTrue(!fields.isEmpty(), struct.group(2) + " declares no field this boundary reads");
            found.put(struct.group(2), fields);
        }
        return found;
    }

    private Map<String, Integer> defines() {
        Map<String, Integer> defined = new LinkedHashMap<>();
        for (String line : lines) {
            Matcher matcher = DEFINE.matcher(line);
            if (matcher.matches()) {
                defined.put(matcher.group(1), Integer.parseInt(matcher.group(2)));
            }
        }
        return defined;
    }

    private static List<String> header() {
        try {
            return Files.readAllLines(HEADER);
        } catch (IOException e) {
            throw new UncheckedIOException(
                    HEADER.toAbsolutePath() + " is committed beside the library and must be readable", e);
        }
    }
}
