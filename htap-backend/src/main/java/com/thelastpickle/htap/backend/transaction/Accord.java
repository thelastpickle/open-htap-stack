package com.thelastpickle.htap.backend.transaction;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.thelastpickle.htap.backend.config.CassandraSettings;
import com.thelastpickle.htap.backend.engine.CassandraPath;
import com.thelastpickle.htap.backend.support.Cells;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * How a transaction, and a write to a table in Accord's care, reach the node.
 *
 * <p>Three methods and one consistency level. {@code transactional_mode='full'} routes every read
 * and every write of an opted-in table through Accord, not only a {@code BEGIN TRANSACTION}, and
 * Accord refuses the driver's default: "ConsistencyLevel LOCAL_ONE is unsupported with Accord for
 * write/commit, supported are [ANY, ONE, QUORUM, ALL, SERIAL]". So a plain {@code INSERT} into one
 * of these tables needs QUORUM exactly as a transaction does, and an ordinary {@code SELECT} of one
 * needs it too, with its own list: "supported are [ONE, QUORUM, ALL, SERIAL]".
 *
 * <p>QUORUM of the five, because it is what the sink already writes at, so a transaction here is not
 * quietly held to a weaker standard than an ordinary write in this stack. It also keeps the demo's
 * two reference writes at the same consistency as the transaction they are compared against, which
 * is the only way that comparison means anything.
 *
 * <p>Kept out of {@link CassandraPath}, which is the read request path the five access paths are
 * compared on. Every method here writes, or reads a table whose reads are consensus; putting them
 * behind that class's names would make its promise, that a read through it is a point read or a
 * bounded scan, no longer true.
 */
@ApplicationScoped
public class Accord {

    private final CassandraPath cassandra;
    private final CassandraSettings settings;

    Accord(CassandraPath cassandra, CassandraSettings settings) {
        this.cassandra = cassandra;
        this.settings = settings;
    }

    /** The keyspace every statement names, because the page shows the statement it ran. */
    public String keyspace() {
        return settings.keyspace();
    }

    /**
     * Runs one Accord transaction and returns its {@code SELECT} projection.
     *
     * <p>Prepared, and for the placeholders rather than for the saving: a transaction is written
     * with {@code ?}, which only a prepared statement binds.
     *
     * <p>An Accord transaction reports differently from a lightweight transaction: it returns no
     * {@code [applied]} column, only the row its own {@code SELECT} projects, and nothing at all
     * when it projects nothing. So a caller cannot ask the server whether the {@code IF} fired; it
     * reads the guard values back out of the projection and decides. Returning the one projected row
     * rather than a list is what makes that legible at the call site.
     *
     * <p>The statement must also be deterministic, so every timeuuid and timestamp the demo writes
     * is bound here rather than generated in the statement: {@code now()} would be evaluated per
     * replica.
     */
    public Map<String, Object> transact(String cql, Object... values) {
        Row projected = cassandra
                .execute(cassandra.prepare(cql).bind(values)
                        .setConsistencyLevel(ConsistencyLevel.QUORUM))
                .one();
        return projected == null ? Map.of() : projection(projected);
    }

    /**
     * Runs a statement that writes.
     *
     * <p>Named apart from {@link #read} because a method that writes should not hide behind a name
     * that says query. Nothing reads what it answers: a lightweight transaction does report an
     * {@code [applied]} row, and the reference measurement times the write rather than inspecting
     * it.
     */
    public void write(String cql, Object... values) {
        cassandra.execute(atQuorum(cql, values));
    }

    /** One bounded read of a table whose reads are consensus, at the level Accord accepts. */
    public List<Row> read(String cql, Object... values) {
        return cassandra.execute(atQuorum(cql, values)).all();
    }

    private static SimpleStatement atQuorum(String cql, Object... values) {
        return SimpleStatement.newInstance(cql, values)
                .setConsistencyLevel(ConsistencyLevel.QUORUM);
    }

    /**
     * The projected row, by the names the node gave it.
     *
     * <p>By position and not by name, which is what the values-by-name reads elsewhere do: a
     * transaction projects columns called {@code session_ok.session_id}, and asking a row for a
     * dotted name is asking for a column the driver's own identifier rules do not spell that way.
     * The order is the projection's either way.
     */
    static Map<String, Object> projection(Row row) {
        ColumnDefinitions definitions = row.getColumnDefinitions();
        Map<String, Object> projected = new LinkedHashMap<>(definitions.size());
        for (int index = 0; index < definitions.size(); index++) {
            projected.put(
                    definitions.get(index).getName().asInternal(),
                    Cells.plain(row.getObject(index)));
        }
        return projected;
    }
}
