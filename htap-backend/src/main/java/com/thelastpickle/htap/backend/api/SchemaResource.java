package com.thelastpickle.htap.backend.api;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.thelastpickle.htap.backend.api.dto.SchemaColumn;
import com.thelastpickle.htap.backend.api.dto.SchemaIndex;
import com.thelastpickle.htap.backend.api.dto.SchemaTable;
import com.thelastpickle.htap.backend.api.dto.SchemaView;
import com.thelastpickle.htap.backend.config.CassandraSettings;
import com.thelastpickle.htap.backend.engine.CassandraPath;
import com.thelastpickle.htap.backend.support.Messages;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

/**
 * What the data model is, read from the engine that owns it.
 *
 * <p>Every obvious source for these facts is wrong in some way, so each is taken from a source
 * established by measurement.
 *
 * <p><b>{@code transactional_mode} comes from {@code DESCRIBE KEYSPACE}.</b> It is not a column of
 * {@code system_schema.tables}, where {@code SELECT transactional_mode} is refused with "Undefined
 * column name", and nothing else there distinguishes a transactional table from a plain one:
 * {@code session_timeline} and its non-transactional twin have identical flags, extensions and
 * fast_path. {@code DESCRIBE} is server-side from Cassandra 4.0, so the driver can run it, and one
 * statement returns the keyspace, its tables and its index, each with a {@code create_statement}
 * carrying the option.
 *
 * <p><b>The keys come from {@code system_schema.columns}</b>, which does carry {@code kind},
 * {@code position} and {@code clustering_order} as columns, so they need no parsing. It is a
 * bounded read by {@code keyspace_name}, which keeps the read layer's promise that every read is a
 * point read or a bounded scan.
 */
@Path("/api/schema")
@Produces(MediaType.APPLICATION_JSON)
@Tag(name = "schema")
public class SchemaResource {

    /** {@code transactional_mode} as {@code DESCRIBE} prints it: {@code AND … = 'full'}. */
    private static final Pattern MODE = Pattern.compile("transactional_mode\\s*=\\s*'(\\w+)'");

    /** The order the key is written in, which is the order a reader needs. */
    private static final List<String> KINDS =
            List.of("partition_key", "clustering", "static", "regular");

    private final CassandraSettings settings;
    private final CassandraPath cassandra;

    SchemaResource(CassandraSettings settings, CassandraPath cassandra) {
        this.settings = settings;
        this.cassandra = cassandra;
    }

    /** The demo keyspace: every table, its key, and whether Accord fronts it. */
    @GET
    @Path("/cql")
    public SchemaView cql() {
        String keyspace = settings.keyspace();
        List<String> storage = List.of(keyspace);
        SortedMap<String, String> described;
        Map<String, List<SchemaColumn>> columns;
        try {
            described = describedTables(keyspace);
            columns = columns(keyspace);
        } catch (RuntimeException e) {
            return new SchemaView(
                    "cassandra", keyspace, List.of(), List.of(), storage, List.of(),
                    Messages.oneLine(e));
        }

        List<SchemaTable> tables = new ArrayList<>(described.size());
        described.forEach((name, create) -> tables.add(new SchemaTable(
                name,
                columns.getOrDefault(name, List.of()),
                transactionalMode(create),
                null,
                create,
                "")));

        List<String> warnings = new ArrayList<>();
        List<SchemaIndex> indexes = List.of();
        try {
            indexes = indexes(keyspace);
        } catch (RuntimeException e) {
            warnings.add("the index list could not be read: " + Messages.oneLine(e));
        }
        warnings.add(accordNote(tables));
        return new SchemaView("cassandra", keyspace, tables, indexes, storage, warnings, null);
    }

    /**
     * Every table in the keyspace with its {@code CREATE} statement, by name.
     *
     * <p>One statement for the whole keyspace: {@code DESCRIBE} is server-side, so this is one
     * round trip rather than one per table.
     */
    private SortedMap<String, String> describedTables(String keyspace) {
        SortedMap<String, String> tables = new TreeMap<>();
        // The keyspace name cannot be bound: DESCRIBE takes an identifier rather than a value.
        // It is this backend's own configuration and never anything a caller sent.
        for (Row row : cassandra.execute(
                SimpleStatement.newInstance("DESCRIBE KEYSPACE " + keyspace))) {
            if ("table".equals(row.getString("type"))) {
                tables.put(text(row, "name"), text(row, "create_statement"));
            }
        }
        return tables;
    }

    /**
     * The columns of every table, in the order that identifies a row.
     *
     * <p>Partition key first by position, then the clustering columns by position, then the rest by
     * name. That order is what makes a partition-key column visibly different from a column that
     * merely comes first alphabetically.
     */
    private Map<String, List<SchemaColumn>> columns(String keyspace) {
        Map<String, List<Row>> byTable = new LinkedHashMap<>();
        for (Row row : cassandra.execute(SimpleStatement.newInstance(
                "SELECT table_name, column_name, kind, position, clustering_order, type"
                        + " FROM system_schema.columns WHERE keyspace_name = ?",
                keyspace))) {
            byTable.computeIfAbsent(text(row, "table_name"), table -> new ArrayList<>()).add(row);
        }
        Map<String, List<SchemaColumn>> columns = new LinkedHashMap<>();
        byTable.forEach((table, rows) -> {
            rows.sort(Comparator.<Row>comparingInt(row -> kindRank(text(row, "kind")))
                    .thenComparingInt(row -> row.getInt("position"))
                    .thenComparing(row -> text(row, "column_name")));
            columns.put(table, rows.stream().map(SchemaResource::column).toList());
        });
        return columns;
    }

    /**
     * The keyspace's indexes.
     *
     * <p>The class and the target come from an options map, which is the easier of the two forms to
     * render; the {@code CREATE} statement says the same thing in one line.
     */
    private List<SchemaIndex> indexes(String keyspace) {
        List<SchemaIndex> indexes = new ArrayList<>();
        for (Row row : cassandra.execute(SimpleStatement.newInstance(
                "SELECT table_name, index_name, kind, options FROM system_schema.indexes"
                        + " WHERE keyspace_name = ?",
                keyspace))) {
            Map<String, String> options = row.getMap("options", String.class, String.class);
            Map<String, String> named = options == null ? Map.of() : options;
            indexes.add(new SchemaIndex(
                    text(row, "index_name"),
                    text(row, "table_name"),
                    className(named.getOrDefault("class_name", text(row, "kind"))),
                    named.getOrDefault("target", "")));
        }
        return indexes;
    }

    private static SchemaColumn column(Row row) {
        return new SchemaColumn(
                text(row, "column_name"),
                text(row, "type"),
                text(row, "kind"),
                row.getInt("position"),
                text(row, "clustering_order"));
    }

    /** The mode the statement declares, or empty for a table that declares none. */
    static String transactionalMode(String createStatement) {
        Matcher matcher = MODE.matcher(createStatement);
        return matcher.find() ? matcher.group(1) : "";
    }

    /** Where a column of this kind sorts; an unknown kind sorts after the four known ones. */
    static int kindRank(String kind) {
        int rank = KINDS.indexOf(kind);
        return rank < 0 ? KINDS.size() : rank;
    }

    /** A class name without its package, which is what a reader of the page wants. */
    static String className(String name) {
        return name == null ? "" : name.substring(name.lastIndexOf('.') + 1);
    }

    /**
     * Which tables a transaction can be run against.
     *
     * <p>Said here rather than in the UI copy, because it is the fact that decides whether a table
     * can carry a transaction and the page should not have to know it.
     */
    static String accordNote(List<SchemaTable> tables) {
        long accord = tables.stream().filter(table -> "full".equals(table.transactionalMode())).count();
        return accord + " of " + tables.size() + " tables route reads and writes through Accord;"
                + " the rest, events included, do not, so a transaction against one of them is"
                + " refused.";
    }

    private static String text(Row row, String column) {
        String value = row.getString(column);
        return value == null ? "" : value;
    }
}
