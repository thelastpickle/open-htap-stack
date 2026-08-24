# The committed Ivy repository

This directory is an Apache Ivy repository, committed so that the `spark` service resolves `--packages` without reaching the network, and so that a CI runner resolves the same bytes a laptop does.  `spark/conf/spark-defaults.conf` points `spark.jars.ivy` here, and `podman-compose.yml` mounts it at `/opt/spark/work-dir/.ivy2`.

## Why it is committed rather than downloaded

One of the coordinates does not exist on Maven Central.  `cassandra-analytics` 0.5-mck0 is a local build of a fork, made to obtain the Cassandra 6.0 bridge; upstream declares 0.5-SNAPSHOT and has published nothing above 0.4.0.  A resolver with nowhere to fetch it from fails the whole `--packages` set, not just that one coordinate, so the artifact travels with the repository.

The rest of the tree is here for a smaller reason: 67 artifacts fetched on every `up` is a slow start and a network dependency the demo does not need.

## What is committed, and what is not

Two files per module are committed: `cache/<org>/<module>/ivy-<rev>.xml` and `cache/<org>/<module>/jars/<artifact>-<rev>.jar`.  `.gitignore` excludes `ivydata-*.properties`, `*.xml.original` and the top-level `jars/` staging directory, all three of which Ivy or `install-cassandra-libraries.sh` rewrites on every run.

`ivydata-<rev>.properties` records the resolver an entry came from, so leaving it out looks as though it should break a later resolve.  It does not.  A checkout without it makes Ivy report `unknown resolver null` under its own `ERRORS` heading and then serve the entry from the cache anyway.  Measured on the 0.4.0 entries: a copy of this directory with every gitignored file removed resolved all 67 artifacts, six of them analytics, in a container on `--network none`.  `scripts/vendor-analytics.sh` runs that same check at the end of every refresh, so the claim is re-measured rather than remembered.

The jars are tracked by Git LFS; see `.gitattributes`.

## Refreshing the analytics artifacts

The analytics build is two Gradle passes, because `gradle/common/publishing.gradle` selects the publication kind from `-PartifactType` and a module with no publication in the current pass exposes no task.  The JDK matters: the analytics root `build.gradle` compares the Gradle JVM's own `java.version` against the profile label, so a Gradle toolchain cannot satisfy it, and `_spark3_2.12` maps to `profiles/scala-2.12-spark-3-jdk-11.gradle`.

```sh
cd ~/src/apache/cassandra-analytics    # thelastpickle/cassandra-analytics, branch mck/37/trunk
export JAVA_HOME="$(jenv prefix 11)" JDK_VERSION=11 SCALA_VERSION=2.12 SPARK_VERSION=3

./gradlew -PartifactType=common -Pversion=0.5-mck0 \
  :cassandra-analytics-common:publishToMavenLocal \
  :cassandra-analytics-sidecar-client:publishToMavenLocal \
  :analytics-sidecar-client:publishToMavenLocal \
  :analytics-sidecar-client-common:publishToMavenLocal \
  :analytics-sidecar-vertx-client:publishToMavenLocal \
  :analytics-sidecar-vertx-client-shaded:publishToMavenLocal --parallel

./gradlew -PartifactType=spark -Pversion=0.5-mck0 \
  :cassandra-analytics-core:publishToMavenLocal \
  :cassandra-bridge:publishToMavenLocal \
  :cassandra-analytics-spark-converter:publishToMavenLocal --parallel
```

Naming the modules is what keeps `cassandra-analytics-integration-framework` and `-integration-tests` out, and with them a `sidecar-server` dependency and the dtest jars.  `DEV-README.md`'s documented commands omit `JDK_VERSION=11` and resolve a profile that does not exist.

Then bring the result into this directory:

```sh
./scripts/vendor-analytics.sh 0.5-mck0
```

That script deletes the previous revision, resolves the new one in a throwaway Spark container with `~/.m2/repository` offered as a `file:` repository, and verifies the committed subset offline.  It also checks that `bridges/six-zero.jar` is nested in the core jar, because a bridge that failed to build fails at query time rather than at build time.

## The six coordinates move together

`podman-compose.yml` names three: `cassandra-analytics-core_spark3_2.12`, `analytics-sidecar-vertx-client-all` and `cassandra-bridge_spark3_2.12`.  Ivy resolves three more as transitive dependencies: `cassandra-analytics-common`, `cassandra-analytics-sidecar-client` and `cassandra-analytics-spark-converter_spark3_2.12`.

All six must be at one revision.  `CassandraDataSource` calls `validateBridges(implementedVersions())` when it loads, so a 0.4.0 jar left beside a 0.5-mck0 one throws `Missing Cassandra implementation for version SIXZERO` rather than degrading to the paths it can still serve.  `vendor-analytics.sh` deletes all six revision directories, not the three it resolves.

## The version string

`0.5-mck0`, not `0.5-SNAPSHOT`.  Upstream declares 0.5-SNAPSHOT, so this follows their form while saying plainly that it is a local build; and a fixed revision names one set of bytes, where a snapshot revision is a moving target that a committed repository cannot represent.  The Sidecar build in `cassandra/dist/` uses the same string for the same reason.
