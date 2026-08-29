package com.thelastpickle.htap.backend.api.dto;

import java.util.Map;

/**
 * Whether each table will accept a transaction, tested rather than looked up.
 *
 * @param tables one entry per table: the words "accepts transactions", or the node's own refusal
 * @param note what to do about a table that refuses, and empty when none does
 */
public record TransactionSchemaReport(
        String keyspace, Map<String, String> tables, boolean ready, String note) {

    /** The words a table that accepted the probe is reported with. */
    public static final String ACCEPTS = "accepts transactions";

    /**
     * What a stack whose tables are not transactional has to do about it.
     *
     * <p>Wiping is the whole of it, and the reason is that the option cannot be added afterwards: the
     * {@code ALTER} starts a migration that only a repair completes, and at replication factor 1
     * there is no repair to run.
     */
    public static final String WIPE =
            "These tables must be created WITH transactional_mode='full', and that needs "
                    + "accord.enabled on the node.  An existing data directory cannot be altered "
                    + "into it on a single node: the ALTER starts a migration that only a repair "
                    + "completes, and at replication factor 1 nodetool repair declines with \"No "
                    + "repair is needed\".  Wipe with ./stop-and-clean-data-and-schema.sh and start "
                    + "again with CASSANDRA_ACCORD_ENABLED=true.";

    public static TransactionSchemaReport of(String keyspace, Map<String, String> tables) {
        boolean ready = tables.values().stream().allMatch(ACCEPTS::equals);
        return new TransactionSchemaReport(keyspace, tables, ready, ready ? "" : WIPE);
    }
}
