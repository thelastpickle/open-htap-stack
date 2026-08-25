#!/usr/bin/env bash
set -euo pipefail

echo "Stopping containers (and cleaning Kafka data)..."
podman compose -f podman-compose.yml down -v
echo "Cleaning Cassandra and Parquet schema and data..."
# One glob for the whole data directory, which is what makes this correct for the
# Accord tables as well: `accord_journal/` and `system_accord/` go with it, and so
# do cassandra-sql's three keyspaces (cassandra_sql, cassandra_sql_internal,
# pg_catalog), which it recreates on its next start.
#
# Do not replace this with DROP KEYSPACE statements.  A keyspace holding an Accord
# table refuses to be dropped -- "Cannot drop keyspace 'cassandra_sql' as it
# contains accord tables" -- so a CQL teardown would have to drop thirteen tables
# by name first, and would leave the journal behind.
rm -rf cassandra-data/*/*
