#!/usr/bin/env bash
#
# Reset the generated demo data without stopping the stack or rebuilding anything.
#
# Truncates the tables the ingest sink fills:
#   demo.events, demo.drone_latest_status, demo.drone_events_by_entity,
#   demo.alerts_by_bucket, demo.ingestion_counts
#
# Leaves demo.restricted_zones alone: the zones are reference data, and the
# dashboard's map has nothing to draw without them.
#
# To reset everything instead, including the Cassandra data directory and the
# Kafka volume, stop the stack and use ../stop-and-clean-data-and-schema.sh.

set -euo pipefail

KEYSPACE="${DEMO_KEYSPACE:-demo}"
EVENTS_TABLE="${DEMO_TABLE:-events}"
CASSANDRA_CONTAINER="${CASSANDRA_CONTAINER:-cassandra}"

TABLES=(
  "${EVENTS_TABLE}"
  drone_latest_status
  drone_events_by_entity
  alerts_by_bucket
  ingestion_counts
)

# podman first, then docker: the repository's compose file is a podman one, but
# the stack runs under either.
container_cli() {
  for cli in podman docker; do
    if command -v "$cli" > /dev/null 2>&1 \
      && "$cli" ps --format '{{.Names}}' 2> /dev/null | grep -qx "$CASSANDRA_CONTAINER"; then
      echo "$cli"
      return 0
    fi
  done
  return 1
}

if ! CLI="$(container_cli)"; then
  echo "Cannot find a running '$CASSANDRA_CONTAINER' container under podman or docker." >&2
  echo "Start the stack first:  podman compose -f podman-compose.yml up -d" >&2
  exit 1
fi

echo "This truncates, in keyspace '${KEYSPACE}' (via ${CLI}):"
printf '  %s\n' "${TABLES[@]}"
echo
echo "restricted_zones is left as it is."
echo
read -r -p "Proceed? (y/N): " confirm
if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
  echo "Cancelled."
  exit 0
fi

# Pause ingest first, so nothing is written between the truncates.
echo
echo "Stopping the producer and the sink…"
"$CLI" stop data-producer data-cassandra-sink > /dev/null 2>&1 || true

for table in "${TABLES[@]}"; do
  printf 'Truncating %s.%s… ' "$KEYSPACE" "$table"
  "$CLI" exec -i "$CASSANDRA_CONTAINER" \
    cqlsh "$CASSANDRA_CONTAINER" -e "TRUNCATE ${KEYSPACE}.${table};" > /dev/null
  echo "done"
done

echo
echo "Restarting the producer and the sink…"
"$CLI" start data-producer data-cassandra-sink > /dev/null

echo
echo "Reset complete.  The dashboard repopulates as new telemetry arrives."
