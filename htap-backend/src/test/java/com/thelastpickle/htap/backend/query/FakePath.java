package com.thelastpickle.htap.backend.query;

import com.thelastpickle.htap.backend.engine.EngineUnavailable;
import com.thelastpickle.htap.backend.engine.QueryPath;
import com.thelastpickle.htap.backend.engine.QueryRows;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * A path that answers whatever a test tells it to, and records what it was asked.
 *
 * <p>The five real paths each hold a driver, a JDBC connection or a native reader, so nothing about
 * the orchestration above them could be tested through one. What the orchestration is judged on is
 * the order it asks in, what it does with a refusal and which of the four cancel mechanisms it
 * reaches for, and all of that is visible here.
 */
final class FakePath implements QueryPath {

    private final String name;
    private final List<String> asked = new CopyOnWriteArrayList<>();
    private final AtomicInteger aborts = new AtomicInteger();
    private final AtomicInteger connects = new AtomicInteger();

    private volatile Supplier<QueryRows> answer = () -> new QueryRows(List.of("n"), List.of());
    private volatile boolean connected = true;
    private volatile boolean busy;
    private volatile boolean abortable = true;
    private volatile RuntimeException connectFails;

    FakePath(String name) {
        this.name = name;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String dialect(String sql, int limit) {
        return sql + " /* " + name + " */ LIMIT " + limit;
    }

    @Override
    public QueryRows query(String sql) {
        asked.add(sql);
        return answer.get();
    }

    @Override
    public void connect(boolean force) {
        connects.incrementAndGet();
        if (connectFails != null) {
            connected = false;
            throw connectFails;
        }
    }

    @Override
    public boolean connected() {
        return connected;
    }

    @Override
    public boolean busy() {
        return busy;
    }

    @Override
    public boolean abort() {
        aborts.incrementAndGet();
        return abortable;
    }

    List<String> asked() {
        return List.copyOf(asked);
    }

    int aborts() {
        return aborts.get();
    }

    int connects() {
        return connects.get();
    }

    FakePath answering(Supplier<QueryRows> rows) {
        this.answer = rows;
        return this;
    }

    FakePath refusing(RuntimeException failure) {
        this.answer = () -> {
            throw failure;
        };
        return this;
    }

    FakePath unreachable() {
        this.connected = false;
        this.connectFails = new EngineUnavailable(name + " connection failed");
        return this;
    }

    FakePath busy(boolean value) {
        this.busy = value;
        return this;
    }

    FakePath withNothingToAbort() {
        this.abortable = false;
        return this;
    }

    FakePath disconnected() {
        this.connected = false;
        return this;
    }
}
