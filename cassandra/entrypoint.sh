#!/usr/bin/env bash
set -euo pipefail

# Get the container's IP address, this is expected (and has to be) static between container runs
CONTAINER_IP=$(hostname -i)

# Configure Cassandra
cp "${CASSANDRA_HOME}/conf/cassandra_latest.yaml" "${CASSANDRA_HOME}/conf/cassandra.yaml"
CONF="${CASSANDRA_HOME}/conf/cassandra.yaml"
mkdir -p /var/lib/cassandra/{data,commitlog,saved_caches,cdc_raw}

PARTITIONER="${CASSANDRA_PARTITIONER:-org.apache.cassandra.dht.Murmur3Partitioner}"
NUM_TOKENS="${CASSANDRA_NUM_TOKENS:-16}"

sed -i \
  -e "s/^cluster_name:.*/cluster_name: '${CASSANDRA_CLUSTER_NAME:-htap-demo}'/" \
  -e "s|^partitioner:.*|partitioner: ${PARTITIONER}|" \
  -e "s/^seed_provider:/seed_provider:/" \
  -e "s/^listen_address:.*/listen_address: ${CONTAINER_IP}/" \
  -e "s/^rpc_address:.*/rpc_address: ${CONTAINER_IP}/" \
  -e "s/seeds: \"127.0.0.1:7000\"/seeds: \"${CONTAINER_IP}:7000\"/" \
  -e "s/seeds: \"127.0.0.1:7000\"/seeds: \"${CONTAINER_IP}:7000\"/" \
  "${CONF}"

# Only Murmur3Partitioner and RandomPartitioner can use the token allocation algorithm, and
# cassandra_latest.yaml sets allocate_tokens_for_local_replication_factor: 3, so any other
# partitioner has to give it up: BootStrapper.getBootstrapTokens() routes to
# TokenAllocation.allocateTokens() whenever that key is set, and the node then refuses to
# start.  Without it a node takes random tokens, which every partitioner can supply, and one
# token is what a single-node cluster wants in any case.
if [ "${PARTITIONER}" != "org.apache.cassandra.dht.Murmur3Partitioner" ]; then
  sed -i \
    -e "s/^allocate_tokens_for_local_replication_factor:/#allocate_tokens_for_local_replication_factor:/" \
    "${CONF}"
  NUM_TOKENS=1
  echo "Partitioner is ${PARTITIONER}: token allocation disabled, num_tokens forced to 1."
fi

sed -i -e "s/^num_tokens:.*/num_tokens: ${NUM_TOKENS}/" "${CONF}"

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

# Change Data Capture, CEP-8.  Cassandra hard-links each commit log segment into cdc_raw as it
# is discarded, and the Sidecar beside this node reads those segments and publishes the
# mutations of a CDC-enabled table to Kafka.  One table opts in, demo.drone_latest_status; the
# sink owns that declaration.
#
# A hard link cannot cross a filesystem, so cdc_raw has to sit beside the commit log, and both
# are under /var/lib/cassandra here.  cdc_on_repair_enabled is off because a single-node
# cluster at RF=1 never repairs, so the setting could only add work.
#
# cdc_total_space bounds cdc_raw, and cdc_block_writes decides who enforces the bound.  Left at
# its default of true, Cassandra enforces it by refusing the write: CDCSizeTracker marks a
# segment FORBIDDEN when defaultSegmentSize + sizeInProgress > cdc_total_space, and
# CommitLogSegmentManagerCDC.throwIfForbidden then raises CDCWriteException on every mutation to
# the keyspace.  Measured here: the directory settled at 4,261,415,150 bytes, 127 segments of
# 32 MiB with their .idx files, and every write to demo was rejected for as long as it stood
# there.  That is one segment short of the 4,294,967,296 limit, and the Sidecar's own
# CdcRawDirectorySpaceCleaner deletes only while directorySizeBytes > cdc_total_space times
# cdc_raw_max_directory_max_percent, 1.0 by default; so Cassandra forbade the segment that would
# have taken the directory past the cleaner's threshold, the cleaner never fired, and the two
# defaults deadlocked with the demo's writes refused.  Established by javap against this image's
# jars, because 6.0 is installed from a binary tarball and there is no source here to read.
#
# So the node enforces it instead, oldest-first: with cdc_block_writes false the same method
# calls deleteOldLinkedCDCCommitLogSegment for the overflow and logs "Freed up ... in
# non-blocking mode".  What that gives up is the guarantee that every mutation reaches Kafka: a
# segment deleted before the Sidecar has read it is a gap in the stream, and no error says so.
# The demo takes that trade on purpose, because the claim this repository makes is that
# analytical and streaming machinery must not touch the OLTP request path, and a rejected
# INSERT is the loudest way to break it.  The node's deletion is also the only trim there is:
# the Sidecar's cleaner cannot be made to fire first, because the setting that would lower its
# threshold arrives as zero and empties the directory instead; ../sidecar.yaml records that.
#
# patch_yaml, rather than three more lines in the sed above, because a key may be set,
# commented out or absent in this release's cassandra_latest.yaml and each case needs a
# different edit.
patch_yaml() {
  local key="$1" value="$2"
  if grep -q "^${key}:" "${CONF}"; then
    sed -i "s|^${key}:.*|${key}: ${value}|" "${CONF}"
  elif grep -q "^# *${key}:" "${CONF}"; then
    sed -i "s|^# *${key}:.*|${key}: ${value}|" "${CONF}"
  else
    echo "${key}: ${value}" >> "${CONF}"
  fi
}

if [ "${CASSANDRA_CDC_ENABLED:-true}" = "true" ]; then
  patch_yaml commitlog_directory   /var/lib/cassandra/commitlog
  patch_yaml cdc_enabled           true
  patch_yaml cdc_raw_directory     /var/lib/cassandra/cdc_raw
  patch_yaml cdc_total_space       "${CASSANDRA_CDC_TOTAL_SPACE:-4096MiB}"
  patch_yaml cdc_block_writes      false
  patch_yaml cdc_on_repair_enabled false
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

# The Sidecar's CDC settings live in a Cassandra table it creates itself, so they are seeded
# after it starts and in the background: the seed waits for that table, and waiting here would
# hold the container's only foreground process.  A failed seed leaves CDC silent and says so in
# this log; it does not stop the node or the Sidecar, both of which serve the other four access
# paths without it.
#
# disown, because the `wait -n` below waits for any job and the seed is a job that ends:
# without it a successful seed exits 0 after a few seconds, `wait -n` returns, and this
# script exits and takes the container with it.  Disowning removes the seed from the job
# table while leaving its output on this log.
if [ "${CASSANDRA_CDC_ENABLED:-true}" = "true" ]; then
  /seed-cdc-configs.sh "${CONTAINER_IP}" &
  disown
fi

# Wait for either process to exit
wait -n
EXIT_CODE=$?
echo "A process exited with code $EXIT_CODE"
exit $EXIT_CODE
