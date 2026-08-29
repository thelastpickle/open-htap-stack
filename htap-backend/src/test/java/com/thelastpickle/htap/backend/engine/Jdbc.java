package com.thelastpickle.htap.backend.engine;

import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A JDBC result set over a proxy, shared by the tests of the two paths that read one.
 *
 * <p>It answers {@code getMetaData}, {@code next} and {@code getObject} and nothing else: anything
 * further raises rather than returning a default, so a test fails loudly if a reader starts to
 * depend on a method a driver might implement differently.
 */
final class Jdbc {

    private Jdbc() {}

    static ResultSet resultSet(List<String> labels, List<? extends List<?>> values) {
        ResultSetMetaData metadata = (ResultSetMetaData) Proxy.newProxyInstance(
                Jdbc.class.getClassLoader(),
                new Class<?>[] {ResultSetMetaData.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getColumnCount" -> labels.size();
                    case "getColumnLabel" -> labels.get((Integer) args[0] - 1);
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        AtomicInteger cursor = new AtomicInteger(-1);
        return (ResultSet) Proxy.newProxyInstance(
                Jdbc.class.getClassLoader(),
                new Class<?>[] {ResultSet.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getMetaData" -> metadata;
                    case "next" -> cursor.incrementAndGet() < values.size();
                    case "getObject" -> values.get(cursor.get()).get((Integer) args[0] - 1);
                    case "close" -> null;
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }
}
