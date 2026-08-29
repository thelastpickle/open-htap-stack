package com.thelastpickle.htap.backend.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.thelastpickle.htap.backend.api.dto.SchemaColumn;
import com.thelastpickle.htap.backend.api.dto.SchemaIndex;
import com.thelastpickle.htap.backend.api.dto.SchemaTable;
import com.thelastpickle.htap.backend.api.dto.SchemaView;
import com.thelastpickle.htap.backend.sql.ConsoleFakes.ScriptedClient;
import java.sql.SQLException;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Which catalog view each fact is taken from, and what a refused read is reported as.
 *
 * <p>Every answer here is one cassandra-sql gave, spellings and text values included: this catalog
 * reports one integer as "3.0" and a not-null flag as "true", and a reader that assumed otherwise
 * would be reading a type the server never sent.
 */
class SqlCatalogTest {

    private static final SqlAnswer TYPES = ConsoleFakes.answer(
            List.of("oid", "typname"), List.of(List.of("23", "int4"), List.of("25", "text")));

    /** Out of name order, because {@code pg_class} answers in none. */
    private static final SqlAnswer RELATIONS = ConsoleFakes.answer(
            List.of("oid", "relname", "relnatts"),
            List.of(List.of("101", "flights", "2"), List.of("102", "operators", "2")));

    private static final SqlAnswer FLIGHT_COLUMNS = ConsoleFakes.answer(
            List.of("attname", "attnum", "atttypid", "attnotnull"),
            List.of(List.of("flight_id", "1", "23", "true"), List.of("distance_km", "2", "700", "false")));

    private static final SqlAnswer OPERATOR_COLUMNS = ConsoleFakes.answer(
            List.of("attname", "attnum", "atttypid", "attnotnull"),
            List.of(List.of("operator_id", "1", "23", "true"), List.of("licence", "2", "25", "false")));

    /** One index on a live table and one on a table this catalog never cleared. */
    private static final SqlAnswer INDEXES = ConsoleFakes.answer(
            List.of("indexname", "tablename", "indexdef"),
            List.of(
                    List.of("flights_pkey", "flights", "CREATE INDEX flights_pkey ON flights"),
                    List.of("products_pkey", "products", "CREATE INDEX products_pkey ON products")));

    private final ScriptedClient client = new ScriptedClient().answering(SqlCatalogTest::answerFor);
    private final SqlCatalog catalog = new SqlCatalog(client, ConsoleFakes.settings());

    /** The stale view is never read, and the two accurate ones are. */
    @Test
    void theTablesComeFromPgClassAndTheColumnsFromPgAttribute() {
        catalog.schema();

        assertTrue(client.ran.contains(SqlCatalog.RELATIONS_SQL));
        assertTrue(client.ran.stream().anyMatch(sql -> sql.contains("pg_catalog.pg_attribute")));
        assertFalse(
                client.ran.stream().anyMatch(sql -> sql.contains("pg_tables")),
                "pg_tables is stale after a DROP and must not be read");
    }

    /** One {@code pg_attribute} read per table, by that table's own {@code oid}. */
    @Test
    void eachTablesColumnsAreReadByItsOid() {
        catalog.schema();

        assertEquals(
                List.of("attrelid = 101", "attrelid = 102"),
                client.ran.stream()
                        .filter(sql -> sql.contains("pg_attribute"))
                        .map(sql -> sql.substring(sql.indexOf("attrelid"), sql.indexOf(" ORDER BY")))
                        .toList());
    }

    /** Sorted here, since the answer arrives in whatever order the key-value store held. */
    @Test
    void theTablesAreOrderedByName() {
        SqlAnswer reversed = ConsoleFakes.answer(
                RELATIONS.columns(), List.of(RELATIONS.rows().get(1), RELATIONS.rows().get(0)));
        SqlCatalog reading = new SqlCatalog(
                new ScriptedClient().answering(
                        sql -> sql.equals(SqlCatalog.RELATIONS_SQL) ? reversed : answerFor(sql)),
                ConsoleFakes.settings());

        assertEquals(
                List.of("flights", "operators"),
                reading.schema().tables().stream().map(SchemaTable::name).toList());
    }

    /**
     * {@code attnotnull} is reported as the key it marks.
     *
     * <p>It is true for the primary key alone here, and a column declared {@code TEXT UNIQUE NOT
     * NULL} reads false, so reporting it as a nullability would state something the engine does not
     * hold.
     */
    @Test
    void theNotNullColumnIsTheKey() {
        List<SchemaColumn> columns = table("operators").columns();

        assertEquals(List.of("operator_id", "licence"), columns.stream().map(SchemaColumn::name).toList());
        assertEquals("primary key", columns.getFirst().kind());
        assertEquals(0, columns.getFirst().position());
        assertEquals("regular", columns.get(1).kind());
        assertEquals(-1, columns.get(1).position());
        // As the CQL side spells a column with no clustering order; nothing here has one.
        assertEquals("none", columns.getFirst().clusteringOrder());
    }

    /** The type name comes from {@code pg_type}, and an {@code oid} it did not name says so. */
    @Test
    void anOidPgTypeDidNotNameIsUnknownRatherThanAbsent() {
        List<SchemaColumn> columns = table("flights").columns();

        assertEquals("int4", columns.getFirst().type());
        assertEquals(SqlCatalog.UNKNOWN_TYPE, columns.get(1).type());
    }

    /** Nothing is asserted about a type when the type list itself could not be read. */
    @Test
    void everyTypeIsUnknownWhenPgTypeIsRefused() {
        SqlCatalog reading = new SqlCatalog(
                new ScriptedClient().answering(sql -> sql.equals(SqlCatalog.TYPES_SQL)
                        ? new SQLException("Table does not exist: PG_TYPE")
                        : answerFor(sql)),
                ConsoleFakes.settings());

        assertTrue(reading.schema().tables().stream()
                .flatMap(table -> table.columns().stream())
                .allMatch(column -> SqlCatalog.UNKNOWN_TYPE.equals(column.type())));
    }

    /** The count arrives as a double, because this engine sends every value as text. */
    @Test
    void theRowCountIsReadThroughTheDecimalFormItArrivesIn() {
        assertEquals(Integer.valueOf(3), table("flights").rowCount());
    }

    /**
     * An empty table reports no count and why, rather than zero.
     *
     * <p>{@code COUNT(*)} raises here over an empty table, and rewriting that to zero would hide one
     * of the defects the SQL subtab exists to show.
     */
    @Test
    void anEmptyTableReportsWhyItCouldNotBeCounted() {
        SchemaTable operators = table("operators");

        assertNull(operators.rowCount());
        assertEquals(
                "no count: Aggregation failed: Index 0 out of bounds for length 0", operators.note());
        assertEquals(2, operators.columns().size(), "a table that cannot be counted still has columns");
    }

    /**
     * A refused {@code pg_attribute} read carries its recovery.
     *
     * <p>The one recorded cause is the lazy catalog: a restarted service refuses the view until one
     * {@code CREATE TABLE} has run in that JVM, and the reset route is that {@code CREATE}.
     */
    @Test
    void aRefusedColumnReadNamesTheRouteThatFixesIt() {
        SqlCatalog reading = new SqlCatalog(
                new ScriptedClient().answering(sql -> sql.contains("pg_attribute")
                        ? new SQLException("Table does not exist: PG_ATTRIBUTE")
                        : answerFor(sql)),
                ConsoleFakes.settings());
        SchemaTable flights = reading.schema().tables().getFirst();

        assertEquals(List.of(), flights.columns());
        assertTrue(
                flights.note().startsWith("columns could not be read: Table does not exist:"
                        + " PG_ATTRIBUTE"),
                flights.note());
        assertTrue(flights.note().contains("POST /api/sql-console/reset is that CREATE."));
    }

    /** An index on a table that is gone is warned about rather than listed as an index. */
    @Test
    void anIndexOnADroppedTableIsAWarningAndNotAnIndex() {
        SchemaView view = catalog.schema();

        assertEquals(
                List.of("flights_pkey"), view.indexes().stream().map(SchemaIndex::name).toList());
        assertEquals("CREATE INDEX flights_pkey ON flights", view.indexes().getFirst().detail());
        assertTrue(view.warnings().getFirst().startsWith("pg_indexes still lists indexes on products,"),
                view.warnings().getFirst());
    }

    /** With nothing stale, the only warning is the one about what the catalog cannot report. */
    @Test
    void theCatalogGapsAreSaidOnEveryAnswer() {
        SqlAnswer live = ConsoleFakes.answer(INDEXES.columns(), List.of(INDEXES.rows().getFirst()));
        SqlCatalog reading = new SqlCatalog(
                new ScriptedClient().answering(
                        sql -> sql.equals(SqlCatalog.INDEXES_SQL) ? live : answerFor(sql)),
                ConsoleFakes.settings());

        assertEquals(List.of(SqlCatalog.CATALOG_GAPS), reading.schema().warnings());
        assertTrue(SqlCatalog.CATALOG_GAPS.contains("pg_constraint is empty"));
    }

    /** A refused index list leaves the tables answered, since the two reads are independent. */
    @Test
    void aRefusedIndexListIsAWarningAndNotTheAnswer() {
        SqlCatalog reading = new SqlCatalog(
                new ScriptedClient().answering(sql -> sql.equals(SqlCatalog.INDEXES_SQL)
                        ? new SQLException("Table does not exist: PG_INDEXES")
                        : answerFor(sql)),
                ConsoleFakes.settings());
        SchemaView view = reading.schema();

        assertEquals(2, view.tables().size());
        assertEquals(List.of(), view.indexes());
        assertEquals(
                "the index list could not be read: Table does not exist: PG_INDEXES",
                view.warnings().getFirst());
    }

    /** A table list nobody could read is the whole answer: there is nothing to say per table. */
    @Test
    void aRefusedTableListIsTheWholeAnswer() {
        SqlCatalog reading = new SqlCatalog(
                new ScriptedClient().answering(sql -> sql.equals(SqlCatalog.RELATIONS_SQL)
                        ? new SQLException("Table does not exist: PG_CLASS")
                        : answerFor(sql)),
                ConsoleFakes.settings());
        SchemaView view = reading.schema();

        assertEquals("Table does not exist: PG_CLASS", view.error());
        assertEquals(List.of(), view.tables());
        assertEquals(List.of(), view.warnings());
    }

    /** A service that is down is a field on the answer, and no statement is attempted. */
    @Test
    void anUnreachableServiceNamesTheAddressItTried() {
        ScriptedClient down = new ScriptedClient();
        down.ready = false;
        SchemaView view = new SqlCatalog(down, ConsoleFakes.settings("sql-host", 5555)).schema();

        assertEquals("cassandra-sql is not reachable at sql-host:5555", view.error());
        assertEquals(List.of(), down.ran);
        assertEquals("cassandra-sql", view.engine());
    }

    /** The keyspaces named are cassandra-sql's own, which is how the page shows what it stores in. */
    @Test
    void theStorageKeyspacesAreTheOnesTheEngineOwns() {
        SchemaView view = catalog.schema();

        assertEquals("cassandra_sql", view.keyspace());
        assertEquals(
                List.of("cassandra_sql", "cassandra_sql_internal", "pg_catalog"),
                view.storageKeyspaces());
    }

    /** No table here carries a transactional mode: the engine decides it, not the table. */
    @Test
    void noTableCarriesATransactionalMode() {
        assertTrue(catalog.schema().tables().stream()
                .allMatch(table -> table.transactionalMode().isEmpty()));
    }

    private SchemaTable table(String name) {
        return catalog.schema().tables().stream()
                .filter(table -> table.name().equals(name))
                .findFirst()
                .orElseThrow();
    }

    /** What the live service answers each of the catalog's statements with. */
    private static Object answerFor(String sql) {
        if (sql.equals(SqlCatalog.TYPES_SQL)) {
            return TYPES;
        }
        if (sql.equals(SqlCatalog.RELATIONS_SQL)) {
            return RELATIONS;
        }
        if (sql.equals(SqlCatalog.INDEXES_SQL)) {
            return INDEXES;
        }
        if (sql.contains("attrelid = 101")) {
            return FLIGHT_COLUMNS;
        }
        if (sql.contains("attrelid = 102")) {
            return OPERATOR_COLUMNS;
        }
        if (sql.equals("SELECT COUNT(*) AS n FROM flights")) {
            return ConsoleFakes.oneCell("3.0");
        }
        if (sql.equals("SELECT COUNT(*) AS n FROM operators")) {
            return new SQLException("Aggregation failed: Index 0 out of bounds for length 0");
        }
        throw new IllegalStateException("no answer scripted for: " + sql);
    }
}
