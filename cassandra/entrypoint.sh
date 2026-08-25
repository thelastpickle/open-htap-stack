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

# Accord, CEP-15.  cassandra_latest.yaml carries no accord block of its own at 6.0-alpha2, so
# appending one adds a top-level key rather than shadowing an existing one.  Enabling the
# subsystem is not by itself enough to make a transaction run: a table opts in with
# transactional_mode, and only demo's three session tables do, so ingest is untouched.
#
# stopMarkerFailurePolicy is here because its default bricks the node after any unclean stop.
# Accord writes a `started` marker into accord_journal/ and a `stopped` marker on a clean
# shutdown; if it finds the first without the second, AccordService.localStartup() throws
# "Stop marker is older than start marker (-1<...), so cannot assume we have a complete log of
# our votes in any consensus groups. Exiting."  The daemon then exits during startup, and every
# table's data is intact but unreachable.  A `podman machine stop`, a laptop asleep or an OOM
# kill is enough to cause it, and this stack has taken all three.
#
# The enum is EXIT (the default, and what throws), UNSAFE_STARTUP, ALLOW_UNSAFE_STARTUP and
# REBOOTSTRAP; established by disassembling the tableswitch in localStartup(), because 6.0-alpha2
# is installed from a binary tarball and there is no source here to read.  The middle two both
# log "Continuing to startup as configured." and proceed.  What that gives up is the guarantee
# that this node knows every vote it cast, which matters when a peer could hold a conflicting
# one; at RF=1 with no peers there is no such peer, so the loss is of nothing.  A multi-node
# cluster must not carry this setting.
#
# The key is snake_case although the Java field is `stopMarkerFailurePolicy`, because the loader
# converts: the camelCase spelling was tried first and refused with "Invalid yaml. Please remove
# properties [stopMarkerFailurePolicy] from your cassandra.yaml".  That refusal names only the
# leaf, so `journal` is right as it stands.  An unrecognised key is fatal and says which, which
# is a cheap way to check a spelling against a binary distribution.
if [ "${CASSANDRA_ACCORD_ENABLED:-true}" = "true" ]; then
  echo -e "accord:\n  enabled: true\n  journal:\n    stop_marker_failure_policy: ALLOW_UNSAFE_STARTUP" >> "${CONF}"
fi

sed -i 's|cassandra_storagedir="$CASSANDRA_HOME/data|cassandra_storagedir="/var/lib/cassandra|' "${CASSANDRA_HOME}/bin/cassandra.in.sh"
sed -i 's|cassandra_storagedir="$CASSANDRA_HOME/data|cassandra_storagedir="/var/lib/cassandra|' "${CASSANDRA_HOME}/tools/bin/cassandra.in.sh"

# Start Cassandra
echo "Starting Cassandra..."
cassandra -f &
CASSANDRA_PID=$!

# Wait for Cassandra to be ready.  The liveness test on CASSANDRA_PID is what stops a daemon
# that died during startup from leaving this loop spinning forever: podman then reports the
# container "Up (starting)" with nothing listening on 9042, which reads as a slow start rather
# than as a failure and hides the reason in a log nobody thinks to open.  Exiting here instead
# makes the container exit, so `podman ps` says so and `podman logs cassandra` ends at the cause.
echo "Waiting for Cassandra to start..."
until cqlsh -e "DESCRIBE KEYSPACES" ${CONTAINER_IP} 9042 >/dev/null 2>&1; do
  if ! kill -0 "${CASSANDRA_PID}" 2>/dev/null; then
    echo "Cassandra exited during startup; the reason is above.  Not waiting for a node that is gone."
    exit 1
  fi
  sleep 2
done
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
