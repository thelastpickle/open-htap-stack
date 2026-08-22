#!/usr/bin/env bash
set -euo pipefail

# Get the container's IP address, this is expected (and has to be) static between container runs
CONTAINER_IP=$(hostname -i)

# Configure Cassandra
cp "${CASSANDRA_HOME}/conf/cassandra_latest.yaml" "${CASSANDRA_HOME}/conf/cassandra.yaml"
CONF="${CASSANDRA_HOME}/conf/cassandra.yaml"
mkdir -p /var/lib/cassandra/{data,commitlog,saved_caches}

sed -i \
  -e "s/^cluster_name:.*/cluster_name: '${CASSANDRA_CLUSTER_NAME:-htap-demo}'/" \
  -e "s/^num_tokens:.*/num_tokens: ${CASSANDRA_NUM_TOKENS:-16}/" \
  -e "s/^seed_provider:/seed_provider:/" \
  -e "s/^listen_address:.*/listen_address: ${CONTAINER_IP}/" \
  -e "s/^rpc_address:.*/rpc_address: ${CONTAINER_IP}/" \
  -e "s/seeds: \"127.0.0.1:7000\"/seeds: \"${CONTAINER_IP}:7000\"/" \
  -e "s/seeds: \"127.0.0.1:7000\"/seeds: \"${CONTAINER_IP}:7000\"/" \
  "${CONF}"

# Accord needs Cassandra 6.0, and 6.0 costs two of the five access paths, so this stays off.
# 6.0 writes BTI SSTables at version "ea": cqlite reads na, nb, oa and da only, and
# cassandra-analytics has bridges for 4.0 and 5.0 only, so spark_bulk has nothing that reads ea.
# No setting holds BTI at da; BtiFormat's current_version is a constant, and only the big
# format's version follows storage_compatibility_mode.  Do not uncomment this without a
# reader for ea, or both the cqlite and spark_bulk paths go dark.
#echo -e "accord:\n  enabled: true" >> "${CONF}"

sed -i 's|cassandra_storagedir="$CASSANDRA_HOME/data|cassandra_storagedir="/var/lib/cassandra|' "${CASSANDRA_HOME}/bin/cassandra.in.sh"
sed -i 's|cassandra_storagedir="$CASSANDRA_HOME/data|cassandra_storagedir="/var/lib/cassandra|' "${CASSANDRA_HOME}/tools/bin/cassandra.in.sh"

# Start Cassandra
echo "Starting Cassandra..."
cassandra -f &
CASSANDRA_PID=$!

# Wait for Cassandra to be ready
echo "Waiting for Cassandra to start..."
until cqlsh -e "DESCRIBE KEYSPACES" ${CONTAINER_IP} 9042 >/dev/null 2>&1; do sleep 2 ;done
echo "Cassandra is ready!"

# Start cassandra-sidecar
echo "Starting Cassandra Sidecar..."

# The launcher's own DEFAULT_JVM_OPTS carries the --add-opens and --add-exports the Sidecar
# needs on Java 17, and points at its bundled conf.  JAVA_OPTS is appended after those, so
# these three properties win and the mounted /config files are read instead.
JAVA_OPTS="-Dsidecar.config=file:///config/sidecar.yaml \
  -Dio.netty.transport.noNative=true \
  -Dlogback.configurationFile=file:///config/sidecar-logback.xml" \
  "${SIDECAR_HOME}/bin/cassandra-sidecar" &

SIDECAR_PID=$!

# Wait for either process to exit
wait -n
EXIT_CODE=$?
echo "A process exited with code $EXIT_CODE"
exit $EXIT_CODE
