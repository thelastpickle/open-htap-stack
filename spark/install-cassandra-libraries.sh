#!/usr/bin/env bash
#
# Put the Cassandra libraries on the JVM system classpath of every Spark process
# in this container, and not merely on Spark's application classloader.
#
# Why this exists
# ---------------
# The Thrift Server is a long-lived server: it serves many sessions, over many
# threads, over hours.  Jars supplied with --packages land on Spark's application
# classloader, and that is enough for a spark-submit job that runs once on the
# thread that loaded them.  It is not enough here.  Two libraries reach for things
# by name from threads whose context classloader is the system one, which cannot
# see a jar added to the application loader, and both failures are time-dependent:
# the first queries after a restart succeed, and later ones do not.
#
#   * The Cassandra java driver keeps its defaults in a reference.conf inside its
#     own jar, and re-resolves its configuration every five minutes on an internal
#     thread.  A reload that cannot find the resource yields a profile with no
#     defaults at all, and the next schema refresh then parks for ever on a missing
#     advanced.control-connection.schema-agreement.timeout.
#
#   * A HiveServer2 session opened later cannot load classes it has not already
#     loaded, so the connector fails with NoClassDefFoundError on something as
#     incidental as the driver's shaded netty DefaultPromise$1.
#
#   * The Analytics bulk reader ships one implementation per Cassandra version as a
#     nested jar under bridges/, chosen by cassandra.releaseVersion, and reports
#     "Missing Cassandra implementation for version FIVEZERO" when it cannot find
#     it.
#
# Copying the jars into /opt/spark/jars, which is on the system classpath of the
# driver and of every executor, removes the question of which classloader is asking.
#
# The rule is add, never shadow: a jar whose artifact Spark already ships is
# skipped, so this cannot downgrade Spark's own slf4j, metrics or commons-lang3 to
# the older versions the Cassandra libraries pin.  Those are all backwards
# compatible in the direction that matters, and Spark is the process we are a guest
# in.
set -euo pipefail

# Maven coordinates, comma separated.  Whitespace is stripped so the caller may
# pass them across several lines for readability; --packages does not tolerate a
# space after a comma.
PACKAGES="$(echo "$1" | tr -d "[:space:]")"
IVY_HOME="${IVY_HOME:-/opt/spark/work-dir/.ivy2}"
SPARK_JARS="${SPARK_JARS:-/opt/spark/jars}"

echo "Resolving the Cassandra libraries: ${PACKAGES}"
# Empty the staging directory first.  It is a bind mount that outlives the container,
# so a version change would otherwise leave the previous jars beside the new ones and
# copy both onto the system classpath, where which one loads is not decided by us.
# Measured: after 0.4.0-mck0 became 0.4.0, all six artifacts were present twice.
# The Ivy cache beside it, which is what makes resolution work without the network,
# is committed and is not touched here.
rm -f "${IVY_HOME}"/jars/*.jar
# spark-submit resolves --packages before it looks at the application, so this
# stages every jar in ${IVY_HOME}/jars and then fails on the empty application.
# There is no resolve-only mode; the failure is the point at which we stop.
/opt/spark/bin/spark-submit \
  --master local \
  --packages "${PACKAGES}" \
  /dev/null > /tmp/resolve-cassandra-libraries.log 2>&1 || true

if ! ls "${IVY_HOME}"/jars/*.jar > /dev/null 2>&1; then
  echo "Resolution produced no jars.  The last lines of the attempt:"
  tail -30 /tmp/resolve-cassandra-libraries.log
  exit 1
fi

installed=0
skipped=0
for staged in "${IVY_HOME}"/jars/*.jar; do
  # Ivy names a staged jar <group>_<artifact>-<version>.jar; reduce that to the
  # artifact so it can be compared with what Spark ships.
  artifact="$(basename "${staged}" | sed -E "s/^[^_]+_//" | sed -E "s/-[0-9][^-]*(-[A-Za-z0-9.]+)*\.jar$//")"
  if ls "${SPARK_JARS}" | grep -qE "^${artifact}-[0-9]"; then
    echo "  keeping the Spark version of ${artifact}"
    skipped=$((skipped + 1))
  else
    cp -n "${staged}" "${SPARK_JARS}/"
    installed=$((installed + 1))
  fi
done

echo "Installed ${installed} jars on the system classpath, kept Spark's own for ${skipped}."
