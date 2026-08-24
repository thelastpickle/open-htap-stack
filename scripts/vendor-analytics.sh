#!/usr/bin/env bash
#
# Refresh the committed Ivy repository under spark/ivy/ with a local cassandra-analytics
# build, so `--packages` resolves it offline in the stack and in CI.
#
# Why a script rather than a copy by hand
# ---------------------------------------
# An Ivy repository entry is not one jar.  Two files per module are committed, ivy-<rev>.xml
# and jars/<artifact>-<rev>.jar, and the xml is a translation of the pom that cross-pins
# every sibling revision.  Writing that by hand is how a sibling gets missed, so Ivy writes
# it: this resolves the coordinates in a throwaway Spark container with the host's Maven
# repository offered as a `file:` repository.
#
# The resolver an entry was written by does not matter, which is worth stating because it
# looks as though it should.  ivydata-<rev>.properties records the resolver name and is
# gitignored, so a fresh checkout has no record of one; Ivy then reports "unknown resolver
# null" under its own ERRORS heading and serves the entry from the cache regardless.
# Measured on the 0.4.0 entries before this script existed: a checkout with the gitignored
# files removed resolved all 67 artifacts, six of them analytics, with the container on
# --network none.  The verify step below is that measurement, kept.
#
# Usage: scripts/vendor-analytics.sh 0.5-mck0
set -euo pipefail

VERSION="${1:?usage: $0 <analytics version, e.g. 0.5-mck0>}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IVY="${REPO_ROOT}/spark/ivy"
M2="${M2:-${HOME}/.m2}"
# The same image the spark service runs, so the Ivy version writing these entries is the one
# that will read them.
SPARK_IMAGE="${SPARK_IMAGE:-apache/spark:3.5.9-scala2.12-java17-python3-ubuntu}"
CONNECTOR="com.datastax.spark:spark-cassandra-connector_2.12:3.5.1"

# The three coordinates the stack and CI name.  Everything else analytics needs arrives as a
# transitive dependency of these, which is the point of letting Ivy resolve rather than
# naming them.  The connector is included so the verify step below exercises the whole set
# the spark service asks for, not the analytics part alone.
PACKAGES="org.apache.cassandra:cassandra-analytics-core_spark3_2.12:${VERSION},org.apache.cassandra:analytics-sidecar-vertx-client-all:${VERSION},org.apache.cassandra:cassandra-bridge_spark3_2.12:${VERSION}"

# Only the analytics modules are cleared.  org.apache.cassandra also holds the DataStax
# java-driver artifacts, which have nothing to do with this build and must survive.
ANALYTICS_MODULES=(
  analytics-sidecar-vertx-client-all
  cassandra-analytics-common
  cassandra-analytics-core_spark3_2.12
  cassandra-analytics-sidecar-client
  cassandra-analytics-spark-converter_spark3_2.12
  cassandra-bridge_spark3_2.12
)

echo "== Checking ${M2} for ${VERSION}"
missing=0
for m in cassandra-analytics-core_spark3_2.12 analytics-sidecar-vertx-client-all cassandra-bridge_spark3_2.12; do
  jar="${M2}/repository/org/apache/cassandra/${m}/${VERSION}/${m}-${VERSION}.jar"
  if [[ ! -f "${jar}" ]]; then
    echo "   missing: ${jar}"
    missing=1
  fi
done
if [[ "${missing}" -ne 0 ]]; then
  echo
  echo "   Build and publish them first.  spark/ivy/README.md has the two gradle commands;"
  echo "   both passes are needed, -PartifactType=common then -PartifactType=spark, on a JDK 11."
  exit 1
fi

# The bridge is what makes a node of a given major version readable, and it is nested inside
# the core jar rather than published, so a build that dropped one fails at query time rather
# than here.  Check for it here instead.
core_jar="${M2}/repository/org/apache/cassandra/cassandra-analytics-core_spark3_2.12/${VERSION}/cassandra-analytics-core_spark3_2.12-${VERSION}.jar"
echo "   bridges nested in the core jar:"
unzip -l "${core_jar}" | awk '/bridges\/[a-z-]+\.jar/ {print "     " $4}'
if ! unzip -l "${core_jar}" | grep -q 'bridges/six-zero.jar'; then
  echo "   bridges/six-zero.jar is absent: this build cannot read a Cassandra 6.0 node."
  exit 1
fi

echo
echo "== Removing the previous analytics revisions"
for m in "${ANALYTICS_MODULES[@]}"; do
  d="${IVY}/cache/org.apache.cassandra/${m}"
  if [[ -d "${d}" ]]; then
    find "${d}" -type f | sed "s|^${REPO_ROOT}/|   - |"
    rm -rf "${d}"
  fi
done
# The staging directory is gitignored and outlives a container, so a stale jar there would be
# copied onto the system classpath beside the new one.
rm -f "${IVY}"/jars/*.jar

echo
echo "== Resolving ${VERSION} with ${M2}/repository offered as a file: repository"
# The network stays on: a new revision may pull transitive dependencies the previous one did
# not, and those have to land in the cache too or the offline verify below will fail.
# --master local with /dev/null as the application: spark-submit resolves --packages before
# it looks at the application, so this populates the repository and then fails on the empty
# file.  There is no resolve-only mode; the failure is where we stop.
podman run --rm \
  -v "${M2}/repository:/m2:ro" \
  -v "${IVY}:/opt/spark/work-dir/.ivy2:rw" \
  -e HOME=/opt/spark/work-dir \
  "${SPARK_IMAGE}" \
  /opt/spark/bin/spark-submit --master local \
    --conf spark.jars.ivy=/opt/spark/work-dir/.ivy2 \
    --repositories file:///m2 \
    --packages "${CONNECTOR},${PACKAGES}" \
    /dev/null > /tmp/vendor-analytics-resolve.log 2>&1 || true

added=0
for m in "${ANALYTICS_MODULES[@]}"; do
  jar="${IVY}/cache/org.apache.cassandra/${m}/jars/${m}-${VERSION}.jar"
  if [[ -f "${jar}" ]]; then
    printf '   + %-48s %10s bytes\n' "${m}/jars/${m}-${VERSION}.jar" "$(wc -c < "${jar}" | tr -d ' ')"
    added=$((added + 1))
  else
    echo "   ! ${m} did not resolve"
  fi
done
if [[ "${added}" -ne "${#ANALYTICS_MODULES[@]}" ]]; then
  echo
  echo "   Resolution was incomplete.  The last lines of the attempt:"
  tail -40 /tmp/vendor-analytics-resolve.log
  exit 1
fi

echo
echo "== Verifying a fresh checkout resolves it with no network"
# This is the assertion that matters, and it cannot be made against ${IVY} itself: the run
# above left ivydata and .xml.original files there, and .gitignore excludes both, so what is
# committed is a thinner set than what is on disk.  Copy, strip what git ignores, resolve
# with the container on --network none.
STAGE="$(mktemp -d)"
trap 'rm -rf "${STAGE}"' EXIT
cp -R "${IVY}" "${STAGE}/ivy"
find "${STAGE}/ivy/cache" -name 'ivydata-*.properties' -delete
find "${STAGE}/ivy/cache" -name '*.xml.original' -delete
rm -rf "${STAGE}/ivy/jars"
mkdir -p "${STAGE}/ivy/jars"
chmod -R a+rwX "${STAGE}/ivy"

podman run --rm --network none \
  -v "${STAGE}/ivy:/opt/spark/work-dir/.ivy2:rw" \
  -e HOME=/opt/spark/work-dir \
  "${SPARK_IMAGE}" \
  /opt/spark/bin/spark-submit --master local \
    --conf spark.jars.ivy=/opt/spark/work-dir/.ivy2 \
    --packages "${CONNECTOR},${PACKAGES}" \
    /dev/null > /tmp/vendor-analytics-verify.log 2>&1 || true

staged=0
for m in "${ANALYTICS_MODULES[@]}"; do
  if [[ -f "${STAGE}/ivy/jars/org.apache.cassandra_${m}-${VERSION}.jar" ]]; then
    staged=$((staged + 1))
  else
    echo "   ! ${m}-${VERSION} did not resolve offline"
  fi
done
total="$(ls "${STAGE}/ivy/jars" | wc -l | tr -d ' ')"
if [[ "${staged}" -ne "${#ANALYTICS_MODULES[@]}" ]]; then
  echo
  echo "   The committed subset is not sufficient.  The last lines of the attempt:"
  tail -40 /tmp/vendor-analytics-verify.log
  exit 1
fi
echo "   ${staged} analytics jars, ${total} artifacts in all, resolved on --network none."

echo
echo "== Now commit spark/ivy/.  The jars are tracked by Git LFS; ivydata and .xml.original"
echo "   are gitignored, and the verify above is why leaving them out is safe."
git -C "${REPO_ROOT}" status --porcelain spark/ivy | sed 's/^/   /'
