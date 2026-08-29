package com.thelastpickle.htap.backend.sql;

import com.thelastpickle.htap.backend.api.dto.SchemaColumn;
import com.thelastpickle.htap.backend.api.dto.SchemaIndex;
import com.thelastpickle.htap.backend.api.dto.SchemaTable;
import com.thelastpickle.htap.backend.api.dto.SchemaView;
import com.thelastpickle.htap.backend.api.dto.SqlConsoleResult;
import com.thelastpickle.htap.backend.config.AccordSqlSettings;
import com.thelastpickle.htap.backend.support.Messages;
import jakarta.enterprise.context.ApplicationScoped;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * cassandra-sql's own tables, read from the catalog views that are accurate.
 *
 * <p>Which view a fact comes from is the whole difficulty here, because this catalog is partly
 * stale. Measured at cassandra-sql revision a0257ec9a22ff84daaf6f529ae8b523fdc45b431.
 *
 * <p><b>{@code pg_tables} is stale after a {@code DROP}</b> and is therefore never read: it listed
 * ten tables where five existed, and {@code SELECT COUNT(*)} over one of the other five answered
 * "Table does not exist". {@code pg_class WHERE relkind = 'r'} is accurate, and
 * {@code pg_attribute WHERE attrelid = <oid> ORDER BY attnum} is too, a {@code WHERE} and an
 * {@code ORDER BY} both working there because the statement is ungrouped.
 *
 * <p><b>{@code pg_attribute} does not exist until one {@code CREATE TABLE} has run in that JVM.</b>
 * A restarted service answers "Table does not exist: PG_ATTRIBUTE" to every read of it while
 * {@code pg_class} still answers its rows, so the refusal is reported per table with its recovery
 * rather than a five-column table being shown as having none. Rows of a dropped table stay as well,
 * which the per-{@code oid} read is indifferent to: a new table gets a new {@code oid}.
 *
 * <p><b>The joins are done here and not in SQL</b>, and that is not a preference: a column name
 * held by two joined tables resolves to one of them for the whole statement, so joining
 * {@code pg_class} to {@code pg_attribute} would be using the very defect the console documents.
 */
@ApplicationScoped
public class SqlCatalog {

    /** The type of a column whose {@code oid} {@code pg_type} did not name. */
    static final String UNKNOWN_TYPE = "unknown";

    static final String TYPES_SQL = "SELECT oid, typname FROM pg_catalog.pg_type";
    static final String RELATIONS_SQL =
            "SELECT oid, relname, relnatts FROM pg_catalog.pg_class WHERE relkind = 'r'";
    static final String INDEXES_SQL =
            "SELECT indexname, tablename, indexdef FROM pg_catalog.pg_indexes";

    /** What the catalog cannot report at all, said on every response because none of it changes. */
    static final String CATALOG_GAPS =
            "There is no information_schema here, and pg_constraint is empty, so UNIQUE, the one"
                    + " constraint this engine enforces, is the one its catalog does not report."
                    + "  pg_enum and pg_sequence do not exist, though the schema declares two ENUMs"
                    + " and a sequence.";

    private final SqlConsoleClient client;
    private final AccordSqlSettings settings;

    SqlCatalog(SqlConsoleClient client, AccordSqlSettings settings) {
        this.client = client;
        this.settings = settings;
    }

    /** Every live table, its columns, its rows where they can be counted, and its indexes. */
    public SchemaView schema() {
        if (!client.ensureReady()) {
            return failed("cassandra-sql is not reachable at " + settings.host() + ":"
                    + settings.port());
        }
        Map<String, String> types = types();
        SqlAnswer relations;
        try {
            relations = client.execute(RELATIONS_SQL);
        } catch (SQLException | RuntimeException e) {
            return failed(Messages.oneLine(e));
        }

        List<SchemaTable> tables = new ArrayList<>(relations.rows().size());
        for (List<String> relation : byName(relations)) {
            tables.add(table(cell(relations, relation, "relname"),
                    cell(relations, relation, "oid"), types));
        }

        List<String> warnings = new ArrayList<>(2);
        List<SchemaIndex> indexes = indexes(tables, warnings);
        warnings.add(CATALOG_GAPS);
        return new SchemaView(SqlConsoleResult.ENGINE, settings.database(), tables, indexes,
                SqlConsole.KEYSPACES, warnings, null);
    }

    /** {@code oid} to type name, read rather than assumed; empty where the read was refused. */
    private Map<String, String> types() {
        SqlAnswer answer;
        try {
            answer = client.execute(TYPES_SQL);
        } catch (SQLException | RuntimeException e) {
            return Map.of();
        }
        Map<String, String> types = new HashMap<>();
        for (List<String> row : answer.rows()) {
            types.put(cell(answer, row, "oid"), cell(answer, row, "typname"));
        }
        return types;
    }

    /**
     * One table, with each of the two reads it needs reported where it failed.
     *
     * <p>The name and the {@code oid} come from the catalog and never from a caller, which is what
     * makes both safe to write into a statement; the {@code oid} is parsed to an integer anyway, so
     * nothing but a number reaches the {@code WHERE}.
     */
    private SchemaTable table(String name, String oid, Map<String, String> types) {
        SqlAnswer attributes;
        try {
            attributes = client.execute("SELECT attname, attnum, atttypid, attnotnull"
                    + " FROM pg_catalog.pg_attribute WHERE attrelid = " + Integer.parseInt(oid.strip())
                    + " ORDER BY attnum");
        } catch (SQLException | RuntimeException e) {
            // The one recorded cause is the lazy catalog, so the recovery goes in the note.
            return new SchemaTable(name, List.of(), "", null, "",
                    "columns could not be read: " + Messages.oneLine(e)
                            + ".  pg_attribute is created on first use, so a restarted cassandra-sql"
                            + " refuses it until one CREATE TABLE has run;"
                            + " POST /api/sql-console/reset is that CREATE.");
        }
        List<SchemaColumn> columns = new ArrayList<>(attributes.rows().size());
        for (List<String> row : attributes.rows()) {
            columns.add(column(attributes, row, types));
        }

        // COUNT(*) raises over an empty table here rather than answering zero, so the count is left
        // absent and the reason recorded against the table.
        try {
            SqlAnswer counted = client.execute("SELECT COUNT(*) AS n FROM " + name);
            Integer rows = counted.rows().isEmpty()
                    ? null
                    : (int) Double.parseDouble(cell(counted, counted.rows().getFirst(), "n"));
            return new SchemaTable(name, columns, "", rows, "", "");
        } catch (SQLException | RuntimeException e) {
            return new SchemaTable(name, columns, "", null, "", "no count: " + Messages.oneLine(e));
        }
    }

    /**
     * One column, keyed as the primary key where the engine marks it not-null.
     *
     * <p>{@code attnotnull} is true for the primary key alone here, and a column declared
     * {@code TEXT UNIQUE NOT NULL} reads false, so it is reported as the key it marks rather than as
     * a nullability the engine does not hold.
     */
    private static SchemaColumn column(SqlAnswer answer, List<String> row, Map<String, String> types) {
        boolean primary = Boolean.parseBoolean(cell(answer, row, "attnotnull").strip());
        return new SchemaColumn(
                cell(answer, row, "attname"),
                types.getOrDefault(cell(answer, row, "atttypid"), UNKNOWN_TYPE),
                primary ? "primary key" : "regular",
                primary ? 0 : -1,
                // "none" rather than empty, as the CQL side says for a column with no clustering
                // order: this engine has no clustering at all, so every column reads the same.
                "none");
    }

    /**
     * The indexes of the live tables, and a warning naming the tables that are gone.
     *
     * <p>{@code pg_indexes} carries the {@code CREATE} statement, which is the more useful of the
     * two index catalogs, but it lists indexes on tables that no longer exist and a {@code DELETE}
     * from it reports success and changes nothing; {@code pg_class WHERE relkind = 'i'} carries
     * fewer indexes than exist. So the live tables filter this list and the warning says what was
     * dropped.
     */
    private List<SchemaIndex> indexes(List<SchemaTable> tables, List<String> warnings) {
        Set<String> live = new HashSet<>();
        tables.forEach(table -> live.add(table.name()));
        SqlAnswer listed;
        try {
            listed = client.execute(INDEXES_SQL);
        } catch (SQLException | RuntimeException e) {
            warnings.add("the index list could not be read: " + Messages.oneLine(e));
            return List.of();
        }

        List<SchemaIndex> indexes = new ArrayList<>();
        Set<String> stale = new TreeSet<>();
        for (List<String> row : listed.rows()) {
            String table = cell(listed, row, "tablename");
            if (live.contains(table)) {
                indexes.add(new SchemaIndex(
                        cell(listed, row, "indexname"), table, cell(listed, row, "indexdef"), ""));
            } else {
                stale.add(table);
            }
        }
        if (!stale.isEmpty()) {
            warnings.add("pg_indexes still lists indexes on " + String.join(", ", stale)
                    + ", which no longer exist; this catalog is not cleared by DROP TABLE, and"
                    + " DROP INDEX is not implemented in either form.");
        }
        return indexes;
    }

    /** A schema nobody could read, rather than an engine holding no tables. */
    private SchemaView failed(String error) {
        return new SchemaView(SqlConsoleResult.ENGINE, settings.database(), List.of(), List.of(),
                SqlConsole.KEYSPACES, List.of(), error);
    }

    /** The rows in table-name order, which {@code pg_class} does not answer in. */
    private static List<List<String>> byName(SqlAnswer relations) {
        List<List<String>> rows = new ArrayList<>(relations.rows());
        rows.sort(Comparator.comparing(row -> cell(relations, row, "relname")));
        return rows;
    }

    /**
     * One named cell of a row.
     *
     * <p>By label rather than by position, and case-insensitively, because a catalog view here is
     * reached through the same result set as everything else: the labels are the server's spelling
     * of them and every value is text.
     */
    private static String cell(SqlAnswer answer, List<String> row, String column) {
        List<String> columns = answer.columns();
        for (int at = 0; at < columns.size(); at++) {
            if (columns.get(at).equalsIgnoreCase(column)) {
                return at < row.size() ? row.get(at) : "";
            }
        }
        return "";
    }
}
