package com.thelastpickle.htap.cqlite;

import static java.lang.foreign.ValueLayout.ADDRESS;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.arrow.memory.BufferAllocator;

/**
 * One DataFusion session: the tables registered on it, and the statements it plans.
 *
 * <p>Usable from several threads at once, and every method here holds the session's read
 * lock while the boundary has the pointer, so {@link #close()} cannot free it under a
 * call in flight. That is the one thing the header forbids; a statement already
 * streaming is not affected, because closing a session while its stream is live is
 * documented as safe.
 *
 * <p>Registration reads no SSTable, so a table Cassandra has not flushed registers
 * without complaint and declines at query time instead.
 */
public final class CqliteSession implements AutoCloseable {

    private final CqliteLibrary library;
    private final BufferAllocator allocator;
    private final MemorySegment session;

    /** Read for a call that holds the pointer, write for the close that frees it. */
    private final ReentrantReadWriteLock lifetime = new ReentrantReadWriteLock();

    private boolean closed;

    private CqliteSession(CqliteLibrary library, BufferAllocator allocator, MemorySegment session) {
        this.library = library;
        this.allocator = allocator;
        this.session = session;
    }

    static CqliteSession open(CqliteLibrary library, BufferAllocator allocator) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment out = arena.allocate(ADDRESS);
            MemorySegment error = arena.allocate(ADDRESS);
            int code;
            try {
                code = (int) library.handle("cqlite_session_open").invokeExact(out, error);
            } catch (Throwable t) {
                throw CqliteLibrary.boundaryFailure("cqlite_session_open", t);
            }
            library.check("cqlite_session_open", code, error);
            return new CqliteSession(library, allocator, out.get(ADDRESS, 0));
        }
    }

    /** The library this session belongs to. */
    public CqliteLibrary library() {
        return library;
    }

    /**
     * Registers {@code directory} as the table {@code name}, whose shape {@code
     * createTableCql} gives. Registering a name twice replaces the first table.
     *
     * @throws CqliteException {@link CqliteStatus#SCHEMA} if the statement does not
     *     parse or names a CQL type the reader cannot map to Arrow
     */
    public void registerTable(
            String name, Path directory, String createTableCql, OpenOptions options) {
        lifetime.readLock().lock();
        try {
            requireOpen();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment error = arena.allocate(ADDRESS);
                int code;
                try {
                    code = (int) library.handle("cqlite_register_table").invokeExact(
                            session,
                            arena.allocateFrom(name),
                            arena.allocateFrom(directory.toString()),
                            arena.allocateFrom(createTableCql),
                            openOptions(arena, options),
                            error);
                } catch (Throwable t) {
                    throw CqliteLibrary.boundaryFailure("cqlite_register_table", t);
                }
                library.check("cqlite_register_table", code, error);
            }
        } finally {
            lifetime.readLock().unlock();
        }
    }

    /** Removes the table {@code name}, which must be registered. */
    public void deregisterTable(String name) {
        lifetime.readLock().lock();
        try {
            requireOpen();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment error = arena.allocate(ADDRESS);
                int code;
                try {
                    code = (int) library.handle("cqlite_deregister_table")
                            .invokeExact(session, arena.allocateFrom(name), error);
                } catch (Throwable t) {
                    throw CqliteLibrary.boundaryFailure("cqlite_deregister_table", t);
                }
                library.check("cqlite_deregister_table", code, error);
            }
        } finally {
            lifetime.readLock().unlock();
        }
    }

    /**
     * What the table {@code name} holds now, read without opening an SSTable.
     *
     * @throws CqliteException {@link CqliteStatus#NOT_READY} if the directory holds no
     *     SSTable, which is the state a flush ends
     */
    public Discovery discover(String name) {
        lifetime.readLock().lock();
        try {
            requireOpen();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment out = arena.allocate(Abi.DISCOVERY);
                MemorySegment error = arena.allocate(ADDRESS);
                int code;
                try {
                    code = (int) library.handle("cqlite_discover")
                            .invokeExact(session, arena.allocateFrom(name), out, error);
                } catch (Throwable t) {
                    throw CqliteLibrary.boundaryFailure("cqlite_discover", t);
                }
                library.check("cqlite_discover", code, error);
                return Discovery.of(
                        (long) Abi.DISCOVERY_FILES.get(out, 0L),
                        (long) Abi.DISCOVERY_BYTES.get(out, 0L),
                        (long) Abi.DISCOVERY_AGE.get(out, 0L));
            }
        } finally {
            lifetime.readLock().unlock();
        }
    }

    /**
     * Plans and starts {@code sql}.
     *
     * <p>Nothing has been read when this returns: the scan runs as the statement's rows
     * are drained, so a failure of the files themselves arrives from {@link
     * CqliteStatement#rows()} rather than from here.
     */
    public CqliteStatement query(String sql) {
        lifetime.readLock().lock();
        try {
            requireOpen();
            return CqliteStatement.start(library, allocator, session, sql);
        } finally {
            lifetime.readLock().unlock();
        }
    }

    /**
     * Frees the session and its registered tables. Idempotent.
     *
     * <p>Statements this session planned are separate and are closed separately; the
     * header allows this call while one of them is still streaming.
     */
    @Override
    public void close() {
        lifetime.writeLock().lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            try {
                library.handle("cqlite_session_close").invokeExact(session);
            } catch (Throwable t) {
                throw CqliteLibrary.boundaryFailure("cqlite_session_close", t);
            }
        } finally {
            lifetime.writeLock().unlock();
        }
    }

    private void requireOpen() {
        if (closed) {
            throw new IllegalStateException("this cqlite session is closed");
        }
    }

    /**
     * The options struct, or {@link MemorySegment#NULL} for the library's own defaults.
     *
     * <p>A null pointer and a zeroed struct mean the same thing to the library, and the
     * null says so at the call site.
     */
    private static MemorySegment openOptions(Arena arena, OpenOptions options) {
        if (options == null || options.equals(OpenOptions.DEFAULTS)) {
            return MemorySegment.NULL;
        }
        MemorySegment segment = arena.allocate(Abi.OPEN_OPTIONS);
        Abi.OPTIONS_SPLITS.set(segment, 0L, options.splits());
        Abi.OPTIONS_BATCH_ROWS.set(segment, 0L, options.batchRows());
        Abi.OPTIONS_KEY_CHUNK.set(segment, 0L, options.keyChunk());
        return segment;
    }
}
