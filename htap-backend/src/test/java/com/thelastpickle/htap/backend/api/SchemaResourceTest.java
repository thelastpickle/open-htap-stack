package com.thelastpickle.htap.backend.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.thelastpickle.htap.backend.api.dto.SchemaTable;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * What the schema route reads out of what Cassandra tells it.
 *
 * <p>Four decisions, each over text the cluster produced, so none of them needs a cluster. Whether
 * {@code DESCRIBE KEYSPACE} answers at all is settled by running the route against the stack.
 */
class SchemaResourceTest {

    /** As {@code DESCRIBE} prints it: the option is one of many in a {@code WITH} clause. */
    private static final String FULL = """
            CREATE TABLE demo.sessions_open (
                session_id uuid PRIMARY KEY,
                opened_at timestamp
            ) WITH additional_write_policy = '99p'
                AND transactional_mode = 'full'
                AND bloom_filter_fp_chance = 0.01;""";

    @Test
    void theTransactionalModeIsReadOutOfTheCreateStatement() {
        assertEquals("full", SchemaResource.transactionalMode(FULL));
        assertEquals("off", SchemaResource.transactionalMode("… AND transactional_mode = 'off' …"));
        assertEquals(
                "mixed_reads",
                SchemaResource.transactionalMode("… AND transactional_mode='mixed_reads' …"));
    }

    /** A table declaring no mode is empty rather than {@code off}: the statement did not say. */
    @Test
    void aTableThatDeclaresNoModeReportsNoMode() {
        assertEquals("", SchemaResource.transactionalMode("CREATE TABLE demo.events (…);"));
    }

    /** The order the key is written in, which is the order a reader of the page needs. */
    @Test
    void theKindsSortInTheOrderTheKeyIsWrittenIn() {
        assertTrue(SchemaResource.kindRank("partition_key") < SchemaResource.kindRank("clustering"));
        assertTrue(SchemaResource.kindRank("clustering") < SchemaResource.kindRank("static"));
        assertTrue(SchemaResource.kindRank("static") < SchemaResource.kindRank("regular"));
    }

    /** A kind this version of Cassandra did not have sorts after the four that are known. */
    @Test
    void anUnknownKindSortsLast() {
        assertTrue(SchemaResource.kindRank("something_new") > SchemaResource.kindRank("regular"));
    }

    @Test
    void aClassNameIsShownWithoutItsPackage() {
        assertEquals(
                "StorageAttachedIndex",
                SchemaResource.className("org.apache.cassandra.index.sai.StorageAttachedIndex"));
        assertEquals("COMPOSITES", SchemaResource.className("COMPOSITES"));
        assertEquals("", SchemaResource.className(null));
    }

    /** The stack's own count, six of fourteen, which is what the page says of a transaction. */
    @Test
    void theAccordNoteCountsTheTablesATransactionCanBeRunAgainst() {
        assertEquals(
                "6 of 14 tables route reads and writes through Accord; the rest, events included,"
                        + " do not, so a transaction against one of them is refused.",
                SchemaResource.accordNote(tables(6, 8)));
    }

    /** A keyspace the route could not read reports zero of zero rather than dividing by nothing. */
    @Test
    void anEmptyKeyspaceIsCountedRatherThanRefused() {
        assertTrue(SchemaResource.accordNote(List.of()).startsWith("0 of 0 tables"));
    }

    private static List<SchemaTable> tables(int full, int off) {
        List<SchemaTable> tables = new java.util.ArrayList<>(full + off);
        for (int i = 0; i < full; i++) {
            tables.add(table("accord_" + i, "full"));
        }
        for (int i = 0; i < off; i++) {
            tables.add(table("plain_" + i, "off"));
        }
        return tables;
    }

    private static SchemaTable table(String name, String mode) {
        return new SchemaTable(name, List.of(), mode, null, "CREATE TABLE demo." + name + " (…);", "");
    }
}
