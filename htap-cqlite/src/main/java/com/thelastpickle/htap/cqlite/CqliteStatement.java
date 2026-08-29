package com.thelastpickle.htap.cqlite;

import static java.lang.foreign.ValueLayout.ADDRESS;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * One running statement: its rows, what it read, and the way to stop it.
 *
 * <p>Nothing has been read when a statement starts. The scan runs as the rows are
 * drained, which is why a failure of the files themselves arrives from {@link #rows()}
 * and why {@link #scan()} is complete only after the drain has ended.
 *
 * <p>The rows cross as an Arrow C Data Interface stream, and the release callback is
 * called exactly once: {@link ArrowReader#close()} is that one call, so this class never
 * releases a stream itself and its own {@link #close()} closes the reader instead. Each
 * statement holds a child allocator, so an Arrow buffer this statement did not give back
 * is charged to the statement and refused at its close rather than accumulating on the
 * process; {@code AllocatorLeakCheckTest} is what says that check is live.
 *
 * <p>Every call takes a read lock and {@link #close()} takes the write lock, the drain
 * included: the C Data Interface forbids a release while a call on the stream is running,
 * so a close racing a drain would be a segmentation fault rather than an exception.
 * {@link #cancel()} is the exception, and it barges; see there.
 */
public final class CqliteStatement implements AutoCloseable {

    /** What a viewer is told when the scan was stopped rather than having failed. */
    public static final String CANCELLED_MESSAGE =
            "Cancelled: the scan was stopped, so the reader abandoned the merge.  "
                    + "The next query starts a new one.";

    private final CqliteLibrary library;
    private final MemorySegment statement;
    private final BufferAllocator allocator;
    private final ArrowReader reader;

    /** Read for a call that holds the pointer, write for the close that frees it. */
    private final ReentrantReadWriteLock lifetime = new ReentrantReadWriteLock();

    /** Written by {@link #cancel()} on any thread and read by the drain on another. */
    private volatile boolean cancelRequested;

    private boolean closed;

    /**
     * The figures as they stood when {@link #close()} freed the statement, so that they
     * outlive it. Written and read under {@link #lifetime}, which is what publishes it.
     */
    private ScanFigures lastScan;

    private CqliteStatement(
            CqliteLibrary library,
            MemorySegment statement,
            BufferAllocator allocator,
            ArrowReader reader) {
        this.library = library;
        this.statement = statement;
        this.allocator = allocator;
        this.reader = reader;
    }

    static CqliteStatement start(
            CqliteLibrary library, BufferAllocator parent, MemorySegment session, String sql) {
        BufferAllocator child = parent.newChildAllocator("cqlite-statement", 0, parent.getLimit());
        ArrowArrayStream stream = null;
        ArrowReader reader = null;
        MemorySegment statement = MemorySegment.NULL;
        boolean populated = false;
        try {
            // Inside the try, because it allocates from the child: a failure here leaves the
            // child registered on the parent, and the session's own close then reports
            // "closed with outstanding child allocators" in place of the real failure.
            stream = ArrowArrayStream.allocateNew(child);
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment outStatement = arena.allocate(ADDRESS);
                MemorySegment error = arena.allocate(ADDRESS);
                int code;
                try {
                    code = (int) library.handle("cqlite_query").invokeExact(
                            session,
                            arena.allocateFrom(sql),
                            MemorySegment.ofAddress(stream.memoryAddress()),
                            outStatement,
                            error);
                } catch (Throwable t) {
                    throw CqliteLibrary.boundaryFailure("cqlite_query", t);
                }
                library.check("cqlite_query", code, error);
                populated = true;
                statement = outStatement.get(ADDRESS, 0);
            }
            // Copies the struct and then closes this one, so nothing is left here to give
            // back.  Established by disassembling arrow-c-data 19.0.0:
            // `ArrowArrayStreamReader` snapshots this struct into a second
            // `ArrowArrayStream` of its own and calls `markReleased()` on this one; the
            // two-argument `importArrayStream` closes this one for the caller; and the
            // reader's `closeReadSource()` releases and closes only its own copy.  So from
            // here the release callback is the reader's and this stream must never be
            // released: its 40-byte buffer is already back with the allocator, and the
            // callback would be read out of memory Arrow has taken.
            reader = Data.importArrayStream(child, stream);
            return new CqliteStatement(library, statement, child, reader);
        } catch (RuntimeException | Error failure) {
            unwind(library, failure, stream, populated, reader, statement, child);
            throw failure;
        }
    }

    /**
     * Gives up what a failed start had already taken.
     *
     * <p>Which of the two copies of the struct holds the release callback is what decides
     * the first step. With a reader, the callback is the reader's and closing it is the one
     * release; without one, the callback is still on this stream, and it is released only
     * when {@code cqlite_query} populated it, since a call that refused leaves the struct
     * as {@code allocateNew} marked it and releasing a null callback is undefined rather
     * than harmless. The stream is closed either way, which is idempotent and so is safe
     * on the one Arrow's import has already closed.
     */
    private static void unwind(
            CqliteLibrary library,
            Throwable failure,
            ArrowArrayStream stream,
            boolean populated,
            ArrowReader reader,
            MemorySegment statement,
            BufferAllocator allocator) {
        if (reader != null) {
            suppressing(failure, () -> closeReader(reader));
        } else if (stream != null && populated) {
            suppressing(failure, stream::release);
        }
        if (stream != null) {
            suppressing(failure, stream::close);
        }
        if (statement.address() != 0L) {
            suppressing(failure, () -> closeStatement(library, statement));
        }
        suppressing(failure, allocator::close);
    }

    /** The columns the statement projects, in order. */
    public List<String> columns() {
        lifetime.readLock().lock();
        try {
            requireOpen();
            return reader.getVectorSchemaRoot().getSchema().getFields().stream()
                    .map(Field::getName)
                    .toList();
        } catch (IOException e) {
            throw drainFailure(e);
        } finally {
            lifetime.readLock().unlock();
        }
    }

    /**
     * Drains the statement, one row per map with the columns in the projected order.
     *
     * <p>Every row is held, so this is for a statement a viewer will see the whole of;
     * {@link #forEachBatch} is what a stream of a larger answer uses.
     */
    public List<Map<String, Object>> rows() {
        List<Map<String, Object>> rows = new ArrayList<>();
        forEachBatch(rows::addAll);
        return rows;
    }

    /**
     * Drains the statement, handing each batch to {@code sink} as it arrives.
     *
     * <p>A batch is what the library emitted, so its size follows {@link
     * OpenOptions#batchRows()} rather than anything decided here.
     *
     * <p>The drain holds the read lock, so {@code sink} must not close this statement:
     * {@link #close()} waits for the write lock and a reader does not upgrade, so a sink
     * that closes would deadlock itself and {@code close()} refuses it instead. {@link
     * #cancel()} from the sink or from another thread is what stops a drain early.
     */
    public void forEachBatch(Consumer<List<Map<String, Object>>> sink) {
        lifetime.readLock().lock();
        try {
            requireOpen();
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            while (reader.loadNextBatch()) {
                sink.accept(ArrowRows.of(root));
            }
        } catch (IOException e) {
            throw drainFailure(e);
        } finally {
            lifetime.readLock().unlock();
        }
    }

    /**
     * Stops the scan. From any thread, at any time, any number of times.
     *
     * <p>The drain then fails, and the failure reports itself as {@link
     * CqliteException#cancelled()} rather than as a fault.
     *
     * <p>{@code tryLock} and not {@code lock}, because this is the one call that must not
     * queue. A non-fair {@link ReentrantReadWriteLock} blocks a new reader once a writer is
     * queued, so a {@link #close()} parked behind a long drain would make this call wait for
     * the very drain it is stopping, and a 12 GB walk would then run to completion
     * uncancellable. {@code tryLock} barges past a queued writer and fails only while
     * {@code close()} holds the write lock, where there is nothing left to stop.
     *
     * @return false if the statement is closed or closing, and there is nothing to stop
     */
    public boolean cancel() {
        if (!lifetime.readLock().tryLock()) {
            return false;
        }
        try {
            if (closed) {
                return false;
            }
            cancelRequested = true;
            int code;
            try {
                code = (int) library.handle("cqlite_cancel").invokeExact(statement);
            } catch (Throwable t) {
                throw CqliteLibrary.boundaryFailure("cqlite_cancel", t);
            }
            // No err out-parameter: this export reports a null pointer and nothing else.
            library.check("cqlite_cancel", code, null);
            return true;
        } finally {
            lifetime.readLock().unlock();
        }
    }

    /**
     * What the statement has read so far, and what it read in total once the drain has
     * ended. Readable after a failed or cancelled drain, and after {@link #close()}, which
     * is where the figures are most worth having: a caller reports them from the branch
     * that handles the failure, outside the try-with-resources that closed the statement.
     */
    public ScanFigures scan() {
        lifetime.readLock().lock();
        try {
            if (!closed) {
                return readScan();
            }
            if (lastScan == null) {
                throw new IllegalStateException(
                        "this cqlite statement is closed and its figures could not be read");
            }
            return lastScan;
        } finally {
            lifetime.readLock().unlock();
        }
    }

    private ScanFigures readScan() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment out = arena.allocate(Abi.SCAN);
            int code;
            try {
                code = (int) library.handle("cqlite_stmt_scan").invokeExact(statement, out);
            } catch (Throwable t) {
                throw CqliteLibrary.boundaryFailure("cqlite_stmt_scan", t);
            }
            library.check("cqlite_stmt_scan", code, null);
            return ScanFigures.of(
                    (long) Abi.SCAN_TABLES.get(out, 0L),
                    (long) Abi.SCAN_FILES.get(out, 0L),
                    (long) Abi.SCAN_BYTES.get(out, 0L),
                    (double) Abi.SCAN_READER_OPEN_MS.get(out, 0L),
                    (long) Abi.SCAN_AGE.get(out, 0L));
        }
    }

    /**
     * Reads the figures, releases the stream, frees the statement and closes the child
     * allocator, in that order. Idempotent, and it waits for a drain in progress.
     *
     * @throws IllegalStateException if the calling thread is inside a drain, which cannot
     *     close: see {@link #forEachBatch}
     */
    @Override
    public void close() {
        refuseCloseInsideDrain(lifetime.getReadHoldCount());
        lifetime.writeLock().lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            List<Exception> failures = new ArrayList<>(4);
            try {
                // Read while the statement still exists, so scan() answers after it is gone.
                lastScan = readScan();
            } catch (RuntimeException e) {
                failures.add(e);
            }
            try {
                // One release of the stream, and the only one this class makes.
                reader.close();
            } catch (Exception e) {
                failures.add(e);
            }
            try {
                closeStatement(library, statement);
            } catch (RuntimeException e) {
                failures.add(e);
            }
            try {
                // Reports an Arrow buffer this statement imported and did not give back.
                allocator.close();
            } catch (RuntimeException e) {
                failures.add(e);
            }
            if (!failures.isEmpty()) {
                IllegalStateException failure =
                        new IllegalStateException("closing this cqlite statement failed");
                failures.forEach(failure::addSuppressed);
                throw failure;
            }
        } finally {
            lifetime.writeLock().unlock();
        }
    }

    private void requireOpen() {
        if (closed) {
            throw new IllegalStateException("this cqlite statement is closed");
        }
    }

    /**
     * Refuses a close from a thread that is inside this statement's drain.
     *
     * <p>The write lock would wait for a read lock the same thread holds and a reader does
     * not upgrade, so the wait is forever and reports nothing. Said here rather than left to
     * hang, because a request with no exception is the hardest failure to attribute; {@link
     * #cancel()} is what a sink calls to stop its own drain.
     *
     * <p>Static and package-private so that the refusal is testable without the library, as
     * {@link #drainFailure} is: reaching the branch through {@link #close()} needs a live
     * statement, which the suite has only where the library and a table are configured.
     */
    static void refuseCloseInsideDrain(int readHoldCount) {
        if (readHoldCount > 0) {
            throw new IllegalStateException(
                    "this thread is draining this cqlite statement and cannot close it:"
                            + " call cancel() from a sink, and close() outside the drain");
        }
    }

    private CqliteException drainFailure(IOException e) {
        return drainFailure(e.getMessage(), cancelRequested);
    }

    /**
     * What a failed drain becomes. Static and package-private so that the mapping is
     * testable without the library, which is a linux artefact.
     *
     * <p>A cancelled merge reports itself as an error like any other, so the intent is
     * what tells the two apart: this statement's own {@link #cancel()}, or a message
     * saying so for a cancel some other holder of the statement made. A genuine failure
     * whose text happens to say "cancelled" is therefore reported as a cancellation, which
     * is the price of the second test and is deliberate.
     */
    static CqliteException drainFailure(String rawMessage, boolean cancelRequested) {
        String message = CqliteException.readable(rawMessage);
        boolean cancelled =
                cancelRequested || message.toLowerCase(Locale.ROOT).contains("cancelled");
        return new CqliteException(
                CqliteStatus.ERROR,
                CqliteStatus.ERROR.code(),
                cancelled,
                cancelled ? CANCELLED_MESSAGE : message);
    }

    /** {@link ArrowReader#close()} as a {@link Runnable}, which is the one release. */
    private static void closeReader(ArrowReader reader) {
        try {
            reader.close();
        } catch (IOException e) {
            throw new UncheckedIOException("closing the Arrow reader of a cqlite statement", e);
        }
    }

    private static void closeStatement(CqliteLibrary library, MemorySegment statement) {
        try {
            library.handle("cqlite_stmt_close").invokeExact(statement);
        } catch (Throwable t) {
            throw CqliteLibrary.boundaryFailure("cqlite_stmt_close", t);
        }
    }

    /** Runs a step of an unwind, keeping its failure beside the one being reported. */
    private static void suppressing(Throwable failure, Runnable step) {
        try {
            step.run();
        } catch (RuntimeException e) {
            failure.addSuppressed(e);
        }
    }
}
