#!/usr/bin/env bash
set -euo pipefail

# podman first, then docker: the compose file is a podman one, but the stack runs
# under either, and the workshop runs it under docker.  This cannot reuse
# scripts/cleanup-data.sh's container_cli(), which picks the CLI that has the
# cassandra container *running*; here the stack may already be stopped.
#
# CONTAINER_CLI overrides the choice.  COMPOSE_FILES adds the workshop override,
# so `down` removes what `up` created:
#   COMPOSE_FILES="-f compose.yml -f compose.workshop.yml" ./stop-and-clean-data-and-schema.sh
CLI="${CONTAINER_CLI:-}"
if [[ -z "$CLI" ]]; then
  for cli in podman docker; do
    if command -v "$cli" > /dev/null 2>&1 && "$cli" compose version > /dev/null 2>&1; then
      CLI="$cli"
      break
    fi
  done
fi
if [[ -z "$CLI" ]]; then
  echo "Cannot find podman or docker with a working 'compose' subcommand." >&2
  exit 1
fi

COMPOSE_FILES="${COMPOSE_FILES:--f podman-compose.yml}"

echo "Stopping containers (and cleaning Kafka data) via ${CLI}..."
# shellcheck disable=SC2086  # COMPOSE_FILES carries its own -f flags
"$CLI" compose ${COMPOSE_FILES} down -v
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
