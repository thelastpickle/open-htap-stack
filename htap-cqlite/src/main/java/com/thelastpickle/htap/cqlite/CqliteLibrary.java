package com.thelastpickle.htap.cqlite;

import static java.lang.foreign.ValueLayout.ADDRESS;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.invoke.MethodHandle;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;

/**
 * A loaded cqlite reader: the shared library, its thirteen exports, and its version.
 *
 * <p>Load once per process. The library is looked up in {@link Arena#global()} and so is
 * never unloaded, which is deliberate: a statement's Arrow stream calls back into this
 * library as it is drained, and unloading while a stream is live is a segmentation fault
 * rather than an exception.
 *
 * <p>{@link #load} resolves every export before it returns, so a library missing one
 * fails at load and names it, rather than at the first query that needs it. The
 * application binary interface (ABI) version check is the same idea and is what the
 * header asks for: register nothing if {@code cqlite_abi_version()} and {@link
 * Abi#VERSION} differ.
 */
public final class CqliteLibrary {

    private final Path path;
    private final int abiVersion;
    private final String buildInfo;
    private final Map<String, MethodHandle> handles;

    private CqliteLibrary(Path path, Map<String, MethodHandle> handles) {
        this.path = path;
        this.handles = handles;
        this.abiVersion = callAbiVersion(handles);
        if (abiVersion != Abi.VERSION) {
            throw new CqliteException(
                    CqliteStatus.ERROR,
                    CqliteStatus.ERROR.code(),
                    false,
                    path + " reports ABI version " + abiVersion + ", and this binding is"
                            + " written against " + Abi.VERSION);
        }
        this.buildInfo = callBuildInfo(handles);
    }

    /**
     * Loads the library at {@code path} and checks that its boundary is the one this
     * binding declares.
     *
     * @throws CqliteException if the library cannot be loaded, does not export all
     *     thirteen names, or reports a different ABI version
     */
    @SuppressWarnings("restricted") // libraryLookup and downcallHandle: the boundary is this module's job
    public static CqliteLibrary load(Path path) {
        SymbolLookup lookup;
        try {
            lookup = SymbolLookup.libraryLookup(path, Arena.global());
        } catch (IllegalArgumentException e) {
            throw new CqliteException(CqliteStatus.ERROR, "cannot load " + path + ": " + e.getMessage(), e);
        }
        Linker linker = Linker.nativeLinker();
        Map<String, MethodHandle> handles = new LinkedHashMap<>();
        for (Map.Entry<String, FunctionDescriptor> export : Abi.SIGNATURES.entrySet()) {
            MemorySegment symbol = lookup.find(export.getKey())
                    .orElseThrow(() -> new CqliteException(
                            CqliteStatus.ERROR,
                            path + " does not export " + export.getKey(),
                            null));
            handles.put(export.getKey(), linker.downcallHandle(symbol, export.getValue()));
        }
        return new CqliteLibrary(path, handles);
    }

    /** Where this library was loaded from. */
    public Path path() {
        return path;
    }

    /** The boundary version the library reports, which equals {@link Abi#VERSION}. */
    public int abiVersion() {
        return abiVersion;
    }

    /**
     * The library's own version, its DataFusion version and the commit it was built
     * from, as one line for a viewer or a log.
     */
    public String buildInfo() {
        return buildInfo;
    }

    /** Opens a session, whose tables are registered on it and freed with it. */
    public CqliteSession openSession(BufferAllocator allocator) {
        return CqliteSession.open(this, allocator);
    }

    MethodHandle handle(String export) {
        MethodHandle handle = handles.get(export);
        if (handle == null) {
            throw new IllegalArgumentException(export + " is not a declared export");
        }
        return handle;
    }

    /**
     * Throws if the boundary refused, reading and freeing the error it wrote.
     *
     * @param errorSlot a one-pointer slot the call was given as its {@code err}
     *     out-parameter, zeroed before the call
     */
    void check(String export, int code, MemorySegment errorSlot) {
        if (code == CqliteStatus.OK.code()) {
            return;
        }
        throw refusal(export, code, errorSlot, false);
    }

    /** The exception a refused call becomes, with the boundary's own message in it. */
    CqliteException refusal(String export, int code, MemorySegment errorSlot, boolean cancelled) {
        CqliteStatus status = CqliteStatus.of(code);
        String message = errorSlot == null ? "" : takeMessage(errorSlot.get(ADDRESS, 0));
        if (message.isEmpty()) {
            // Rule 1 lets an export leave *err alone, and cqlite_cancel takes no err at
            // all, so name the call and the status rather than reporting nothing.
            message = export + " returned " + status + " (" + code + ")";
        }
        return new CqliteException(status, code, cancelled, message);
    }

    /** Reads an error's message and frees the error, which invalidates the message. */
    @SuppressWarnings("restricted") // reinterpret, to read a C string of unknown length
    private String takeMessage(MemorySegment error) {
        if (error.address() == 0L) {
            return "";
        }
        try {
            MemorySegment text = (MemorySegment) handle("cqlite_error_message").invokeExact(error);
            String raw = text.address() == 0L
                    ? ""
                    : text.reinterpret(Long.MAX_VALUE).getString(0);
            return CqliteException.readable(raw);
        } catch (Throwable t) {
            throw boundaryFailure("cqlite_error_message", t);
        } finally {
            try {
                handle("cqlite_error_free").invokeExact(error);
            } catch (Throwable t) {
                throw boundaryFailure("cqlite_error_free", t);
            }
        }
    }

    /**
     * What a downcall's declared {@code throws Throwable} becomes.
     *
     * <p>Only a mismatch between {@link Abi}'s descriptors and the call sites can reach
     * this, which is a defect here rather than a state the library can be in; an {@link
     * Error} is left alone because the caller cannot act on it either. Each call site
     * therefore wraps the invocation alone, and checks the returned code outside the
     * {@code catch}, so a refusal is never reported as a defect in this file.
     */
    static RuntimeException boundaryFailure(String export, Throwable cause) {
        if (cause instanceof Error error) {
            throw error;
        }
        return new CqliteException(
                CqliteStatus.ERROR, "calling " + export + " failed: " + cause, cause);
    }

    private static int callAbiVersion(Map<String, MethodHandle> handles) {
        try {
            return (int) handles.get("cqlite_abi_version").invokeExact();
        } catch (Throwable t) {
            throw boundaryFailure("cqlite_abi_version", t);
        }
    }

    @SuppressWarnings("restricted") // reinterpret, to read a C string of unknown length
    private static String callBuildInfo(Map<String, MethodHandle> handles) {
        try {
            MemorySegment text = (MemorySegment) handles.get("cqlite_build_info").invokeExact();
            // Lives for the process, so it is read rather than copied out and freed.
            return text.address() == 0L ? "" : text.reinterpret(Long.MAX_VALUE).getString(0);
        } catch (Throwable t) {
            throw boundaryFailure("cqlite_build_info", t);
        }
    }
}
