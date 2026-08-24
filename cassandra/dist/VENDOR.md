# Cassandra Sidecar, built here

`apache-cassandra-sidecar-0.5-mck0.tar.gz` is a local build, not a release.  `../Dockerfile` copies it instead of fetching a tarball from archive.apache.org, and verifies the checksum beside it anyway.

| | |
| --- | --- |
| Fork | [thelastpickle/cassandra-sidecar](https://github.com/thelastpickle/cassandra-sidecar), branch `mck/494/trunk` |
| Commit | `14ab5075` ("Support Apache Cassandra 6.0") |
| Upstream base | `7972401c` (CASSSIDECAR-493), on `trunk` |
| Built | 2026-08-23 |
| Version it declares | `0.5-mck0`; upstream's `gradle.properties` says `0.5-SNAPSHOT` |
| Size | 354,684,569 bytes, stored in Git LFS |
| sha256 | `6d2dfe429ea0fe93517d79feed9276d643b1640541dca09c6159dcee21204c77` |

## Why a local build

The Sidecar chooses an adapter per Cassandra version at runtime, and refuses a node no adapter claims.  `Cassandra60Factory` carries `@MinimumVersion("6.0.0")` and exists only on the branch above, so released 0.4.0 cannot serve a 6.0 node at all.  There is no 0.5 release: upstream declares `0.5-SNAPSHOT`, and Cassandra does not publish snapshots.

The demo cannot drop the Sidecar instead, because the `spark_bulk` access path reads SSTables through it.  Losing it would remove one of the five paths the whole repository exists to compare.

## Why the artifact is committed

A 354 MB binary in the tree is a real cost, and the alternatives were worse.  Building the Sidecar inside the Cassandra image would put a Gradle build, a JDK download and a Maven Central fetch into every stack rebuild, including the scheduled CI build that runs with no layer cache.  Building on the host into an ignored directory would make the repository un-runnable for anyone who has not first cloned two other repositories.  Git LFS keeps the clone honest: `.gitattributes` tracks `*.tar.gz`, and the CI workflow already checks out with `lfs: true`.

## Rebuilding

Requires a JDK 17: `gradle/common/environmentChecks.gradle` fails Test tasks on macOS above 17, and the wrapper is Gradle 8.9, which does not know Java 25.

```sh
cd ~/src/apache/cassandra-sidecar
git checkout mck/494/trunk
JAVA_HOME="$(jenv prefix 17)" ./gradlew distTar -Pversion=0.5-mck0 \
  -x test -x integrationTest -x containerTest
cp build/distributions/apache-cassandra-sidecar-0.5-mck0.tar.gz \
   build/distributions/apache-cassandra-sidecar-0.5-mck0.tar.gz.sha256 \
   <repo>/cassandra/dist/
```

`distTar` is finalized by `generateDistributionChecksums`, so it also writes the zip, the source archives, the deb and the rpm; only the tarball and its `.sha256` are copied here.

**Do not add `--parallel`.**  The root project's `jar` task writes `server-common/src/main/resources/sidecar.version` in a `doFirst`, and `server-common:processResources` reads it, with no declared dependency between the two.  Run in parallel on a clean tree and the resource is packaged before it is written, so `lib/server-common-0.5-mck0.jar` carries no `sidecar.version`.  The Sidecar then fails at startup, in Guice, with `NullPointerException: Cannot invoke "java.io.InputStream.read(byte[])" because "input" is null` from `SidecarVersionProvider.<init>`, and the container exits 1 while Cassandra beside it is healthy.  Measured here: the first build was made with `--parallel` and failed exactly that way; the serial rebuild packaged the resource.  Check it rather than trusting the exit code:

```sh
tar -xzOf build/distributions/apache-cassandra-sidecar-0.5-mck0.tar.gz \
    apache-cassandra-sidecar-0.5-mck0/lib/server-common-0.5-mck0.jar > /tmp/sc.jar
unzip -p /tmp/sc.jar sidecar.version    # must print the version
```

## What the build carries, and what it does not

`lib/` holds `adapters-cassandra60-0.5-mck0.jar` beside the 4.1 and 5.0 adapters, which is what serves a 6.0 node.

`gradle.properties` leaves `analyticsVersion=0.4.0`, so `lib/` also holds cassandra-analytics 0.4.0, which has no 6.0 bridge.  That is deliberate and costs this demo nothing.  The `spark_bulk` path resolves analytics from the Ivy cache in `../../spark/ivy/`, not from this `lib/`; the only Sidecar feature that reads `lib/`'s analytics jars is CDC, and `../sidecar.yaml` does not enable it.  Turning CDC on against a 6.0 node would need this rebuilt with `-PanalyticsVersion=0.5-mck0`.

## Configuration

`../sidecar.yaml` is a trimmed 0.4.0 template, and 0.5 still accepts it.  The two keys most likely to have moved were checked against this build's own `conf/sidecar.yaml`: `driver_parameters.auth_provider.class_name` is still `org.apache.cassandra.sidecar.cluster.auth.ConfigProvider`, and the four `cassandra_input_validation` regexes are unchanged, so a `bti-ea` generation's component names still pass.
