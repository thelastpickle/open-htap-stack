#!/usr/bin/env bash
#
# Start the JVM, and exit the container if it never serves.
#
# cassandra-sql can fail its startup in a way podman cannot see.  A bean that
# throws in @PostConstruct kills the Spring context, and the JVM survives it,
# because the Cassandra driver's Netty threads are not daemon threads.  The
# observed state is a container `podman inspect` calls running with `Restarts=0`,
# a log ending at "APPLICATION FAILED TO START", and nothing listening on 5432;
# `restart: unless-stopped` never fires, because nothing exited.  Worse,
# `podman compose up -d --no-deps accord-sql` answers "Container accord-sql
# Running" and changes nothing, so the obvious recovery is a no-op and only
# `podman restart accord-sql` clears it.
#
# So this script waits for the Postgres listener to open, and kills the JVM if it
# does not.  The container then exits non-zero, compose's restart policy tries
# again, and the next attempt meets a Cassandra that has had more time.  The
# listener is the right thing to wait on: in the failed state above, 5432 was
# closed, so the port opening is evidence the context came up.
#
# A healthy start was measured at "Started CassandraSqlApplication in 36.688
# seconds", so the default window is an order of magnitude above it and will not
# cut a slow but working start short.
set -uo pipefail

WINDOW_S="${ACCORD_SQL_STARTUP_TIMEOUT_S:-300}"

# Unquoted on purpose: JAVA_OPTS is several arguments.  That is also why no value
# in it may contain a space.
# shellcheck disable=SC2086
java ${JAVA_OPTS} -jar /app/cassandra-sql.jar &
jvm=$!

# This script is PID 1, and bash does not relay a signal to a background child,
# so without the trap `podman stop` would wait its ten seconds and then SIGKILL.
trap 'kill -TERM "$jvm" 2>/dev/null' TERM INT

deadline=$((SECONDS + WINDOW_S))
while ! (exec 3<>/dev/tcp/127.0.0.1/5432) 2>/dev/null; do
    if ! kill -0 "$jvm" 2>/dev/null; then
        # The JVM exited by itself; the container's status should be its status.
        wait "$jvm"
        exit $?
    fi
    if (( SECONDS >= deadline )); then
        echo "accord-sql: nothing listening on 5432 after ${WINDOW_S}s." \
             "Exiting so the restart policy can try again." >&2
        kill -TERM "$jvm" 2>/dev/null
        sleep 5
        kill -KILL "$jvm" 2>/dev/null
        exit 1
    fi
    sleep 2
done

echo "accord-sql: the Postgres wire listener is open on 5432."
wait "$jvm"
status=$?
# A trapped signal interrupts the wait above, so wait again for the JVM's own
# shutdown rather than exiting under it.
wait "$jvm" 2>/dev/null
exit $status
