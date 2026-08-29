package com.thelastpickle.htap.backend.sql;

import com.thelastpickle.htap.backend.config.AccordSqlSettings;
import jakarta.enterprise.inject.Vetoed;
import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/** The seams the console tests drive: settings, a client, and a JDBC statement over a proxy. */
final class ConsoleFakes {

    private ConsoleFakes() {}

    static AccordSqlSettings settings() {
        return settings("accord-sql", 5432);
    }

    static AccordSqlSettings settings(String host, int port) {
        return new AccordSqlSettings() {
            @Override
            public String host() {
                return host;
            }

            @Override
            public int port() {
                return port;
            }

            @Override
            public String database() {
                return "cassandra_sql";
            }

            @Override
            public String user() {
                return "htap-mission-control";
            }

            @Override
            public Duration connectTimeout() {
                return Duration.ofSeconds(5);
            }
        };
    }

    /**
     * A client that answers from a script rather than from a socket.
     *
     * <p>Subclasses the real one, so the console under test holds the type it holds in production and
     * nothing about the seam is visible to it.
     *
     * <p>{@code @Vetoed} because a subclass of a bean class is itself a bean here, and a container
     * that found both would refuse every {@code SqlConsoleClient} injection point as ambiguous.
     */
    @Vetoed
    static final class ScriptedClient extends SqlConsoleClient {

        final List<String> ran = new ArrayList<>();
        private final Deque<Object> answers = new ArrayDeque<>();
        private Function<String, Object> byStatement;
        private Object repeated = SqlAnswer.NOTHING;
        boolean ready = true;
        boolean reportsConnected = true;
        boolean reportsBusy;

        ScriptedClient() {
            super(settings());
        }

        /** Answer the next statement with these rows. A queued answer is used once. */
        ScriptedClient queue(Object answerOrFailure) {
            answers.add(answerOrFailure);
            return this;
        }

        /** Answer every statement that outlasts the queue with this. */
        ScriptedClient always(Object answerOrFailure) {
            repeated = answerOrFailure;
            return this;
        }

        /**
         * Answer each statement by what it says, falling through to the queue where this returns
         * nothing. What a caller issuing several different statements needs, since a queue would then
         * assert the order as well as the answers.
         */
        ScriptedClient answering(Function<String, Object> byStatement) {
            this.byStatement = byStatement;
            return this;
        }

        @Override
        public boolean ensureReady() {
            return ready;
        }

        @Override
        public boolean connected() {
            return reportsConnected;
        }

        @Override
        public boolean busy() {
            return reportsBusy;
        }

        @Override
        SqlAnswer execute(String sql) throws SQLException {
            ran.add(sql);
            Object answer = byStatement == null ? null : byStatement.apply(sql);
            if (answer == null) {
                answer = answers.isEmpty() ? repeated : answers.poll();
            }
            return switch (answer) {
                case SqlAnswer rows -> rows;
                case SQLException failure -> throw failure;
                case RuntimeException failure -> throw failure;
                default -> throw new IllegalStateException("not an answer: " + answer);
            };
        }
    }

    static SqlAnswer answer(List<String> columns, List<List<String>> rows) {
        return new SqlAnswer(columns, rows, 1.0);
    }

    /** One row of one column, which is what most of these statements return. */
    static SqlAnswer oneCell(String value) {
        return answer(List.of("n"), List.of(List.of(value)));
    }

    /**
     * A JDBC statement whose {@code execute} exposes the results given, in order.
     *
     * <p>A {@code List} is a result set and an {@code Integer} an update count, which is how a driver
     * reports a multi-statement string: the walk under test has to reach the last of them.
     */
    static Statement statement(List<String> columns, List<?> results) {
        AtomicInteger at = new AtomicInteger(-1);
        return (Statement) Proxy.newProxyInstance(
                ConsoleFakes.class.getClassLoader(),
                new Class<?>[] {Statement.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "execute" -> results.get(at.incrementAndGet()) instanceof List;
                    case "getResultSet" -> resultSet(columns, (List<?>) results.get(at.get()));
                    // Advances first, then says whether what it landed on is a result set, which is
                    // the contract the walk depends on.
                    case "getMoreResults" -> at.incrementAndGet() < results.size()
                            && results.get(at.get()) instanceof List;
                    case "getUpdateCount" -> at.get() < results.size()
                                    && results.get(at.get()) instanceof Integer count
                            ? count
                            : -1;
                    case "close" -> null;
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static ResultSet resultSet(List<String> columns, List<?> rows) {
        ResultSetMetaData metadata = (ResultSetMetaData) Proxy.newProxyInstance(
                ConsoleFakes.class.getClassLoader(),
                new Class<?>[] {ResultSetMetaData.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getColumnCount" -> columns.size();
                    case "getColumnLabel" -> columns.get((Integer) args[0] - 1);
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        AtomicInteger cursor = new AtomicInteger(-1);
        return (ResultSet) Proxy.newProxyInstance(
                ConsoleFakes.class.getClassLoader(),
                new Class<?>[] {ResultSet.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getMetaData" -> metadata;
                    case "next" -> cursor.incrementAndGet() < rows.size();
                    case "getString" -> ((List<?>) rows.get(cursor.get())).get((Integer) args[0] - 1);
                    case "close" -> null;
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }
}
