# Cassandra Sidecar, built here

`apache-cassandra-sidecar-0.5-mck2.tar.gz` is a local build, not a release.  `../Dockerfile` copies it instead of fetching a tarball from archive.apache.org, and verifies the checksum beside it anyway.

| | |
| --- | --- |
| Fork | [thelastpickle/cassandra-sidecar](https://github.com/thelastpickle/cassandra-sidecar), branch `mck/494/trunk` |
| Commit | `9e73b502` ("Register an unsupported-type table with blob columns rather than skipping it"), on `6050ff20` ("Adapt CDC schema supply to the analytics 0.5 SchemaSupplier"), on `14ab5075` ("Support Apache Cassandra 6.0") |
| Upstream base | `7972401c` (CASSSIDECAR-493), on `trunk` |
| Analytics it bundles | `0.5-mck1`, which is `69add7b` in the analytics fork; see [The commit log reader](#the-commit-log-reader-and-a-table-it-cannot-know) below |
| Built | 2026-08-26 |
| Version it declares | `0.5-mck2`; upstream's `gradle.properties` says `0.5-SNAPSHOT` |
| Size | 511,196,166 bytes, stored in Git LFS |
| sha256 | `56b20c214ae3f10435f23acdece132561c6660964676b1c0668b05e459616c1f` |

## Why a local build

The Sidecar chooses an adapter per Cassandra version at runtime, and refuses a node no adapter claims.  `Cassandra60Factory` carries `@MinimumVersion("6.0.0")` and exists only on the branch above, so released 0.4.0 cannot serve a 6.0 node at all.  There is no 0.5 release: upstream declares `0.5-SNAPSHOT`, and Cassandra does not publish snapshots.

The demo cannot drop the Sidecar instead, because the `spark_bulk` access path reads SSTables through it.  Losing it would remove one of the five paths the whole repository exists to compare.

CDC then needed a second local change, on top of the 6.0 adapter.  Building against analytics 0.5 rather than 0.4.0 failed `:server:compileJava` with two errors, because CASSANALYTICS-182 had replaced `SchemaSupplier.getCdcEnabledTables()` with `getTables()`: the interface now asks for *every* table, so the bridge's `Schema.instance` can deserialize any mutation in a commit log segment, and `CqlTable.cdc()` carries the per-table flag.  Upstream `trunk` still calls the old method, so `6050ff20` adapts it: `CdcSchemaSupplier` builds every non-system table and reads each table's `cdc` property from its own create statement.

A third change, `9e73b502`, followed from what "every table" means.  That first adaptation skipped a table whose schema would not build, and `demo.ingestion_counts` has a `counter` column, which `AbstractSchemaBuilder.validateType` refuses with "counter data type is not supported".  One commit log serves every table, so a segment carrying one counter mutation could not be deserialized; `BufferingCommitLogReader` treats that as an unrecoverable segment error, calls `requestTermination()` and re-reads the same segment for as long as it exists.  Measured on this stack: 926 skip warnings and 5,740 unrecoverable-segment errors in ten minutes, with no record published and no topic created.

So the commit registers such a table with the refused columns as `blob` and CDC off, rather than skipping it.  A cell carries its own length unless `AbstractType.valueLengthIfFixed()` is positive, and neither `CounterColumnType` nor `BytesType` overrides it, so the reader consumes the same bytes it would have consumed with the declared type; established with `javap` against this image's jars.  What is given up is the value's meaning, which is why a CDC-enabled table is still skipped: publishing a counter's internal context as a blob would be worse than refusing it.

Two other repairs were considered.  Patching cassandra-analytics to widen `validateType` would need an analytics republish as well as a Sidecar rebuild, and moves the decision away from the class that owns the contract.  Dropping the counter column from the demo schema would remove a real Cassandra feature from the demonstration and hide the limitation this commit reports.

## The commit log reader and a table it cannot know

One table in this demo is invisible to the schema the reader is given, and that turned out to stop CDC outright rather than to cost one table's rows.

`demo.drone_text_embeddings` declares `payload_vector vector<float, 1536>`, and the Sidecar's driver is DataStax Java driver 3.x, which cannot parse that type: `SchemaParser.buildTables` raises `IllegalArgumentException: Could not parse type name vector<float, 1536>` and drops the table.  `CdcSchemaSupplier` reads `metadata().exportSchemaAsString()`, so no create statement for that table ever reaches `buildTables`, and the blob rewrite above never runs.

The blob rewrite could not have saved it in any case, for two reasons.  `AbstractSchemaBuilder.validateType` accepts only `CQL3Type.Native`, `Collection`, `UserDefined` and `Tuple`, and a vector is `CQL3Type.Vector`, whose refusal reads "Only native, collection, tuples or UDT data types are supported" and does not match the `UNSUPPORTED_TYPE` pattern.  And `blob` is not byte-compatible here: `VectorType.valueLengthIfFixed()` is positive for a fixed-length element type, 6,144 bytes for 1,536 floats, so the cell carries no length of its own and a reader expecting `BytesType` would read a length that was never written.

So the mutation has to be skipped, which is what `BufferingCommitLogReader` already does for a table absent from `Schema.instance`.  On Cassandra 6.0 that skip never happened.  `SchemaProvider.getExistingTableMetadata` throws `UnknownTableException`, then `PartitionUpdate.PartitionUpdateSerializer.deserialize` catches it and rethrows `new CoordinatorBehindException(exception.getMessage(), exception)`, which extends `RuntimeException`; a `catch (UnknownTableException)` therefore does not fire, and the failure reaches `handleUnrecoverableError`, which calls `requestTermination()`.  `CdcScannerBuilder` then re-reads the same segment for as long as it exists.  Established with `javap` against this image's jars: the exception's only two-argument constructor takes an `UnknownTableException`, and `PartitionUpdate$PartitionUpdateSerializer` is the class on the deserialization path that calls it.

Measured here, from deleting one row of that table: publication stopped at 1,379,291 records and did not advance in the following eight minutes, while `cdc_raw` grew from 76 segments and 2,449,982,776 bytes to 125 segments and 4,093,884,585.  The error named the table as `tid:c`, which is `1b255f4d-ef25-40a6-0000-00000000000c` in `system_schema.tables`, `demo.drone_text_embeddings`.

The repair is in the analytics fork, `69add7b` on `mck/37/trunk`, published as `0.5-mck1` and bundled here: `readMutationInternal` looks for an `UnknownTableException` along the cause chain instead of catching the declared type.  Skipping is safe wherever the wrapping happens, because `readSection` does `readFully` for the mutation's serialized size before calling the method, so the file pointer is past the mutation whether it deserialized or not.  What is given up is loudness: a table the supplier could not build now leaves a silent hole in the topic, counted only by `mutationsIgnoredUnknownTableCount`.

`../../spark/ivy/` stays at analytics `0.5-mck0` on purpose.  The Spark paths never read a commit log, so the fix cannot change what they do, and re-vendoring would re-commit six jars through Git LFS for identical behaviour.

## Why the artifact is committed

A 511 MB binary in the tree is a real cost, and the alternatives were worse.  Building the Sidecar inside the Cassandra image would put a Gradle build, a JDK download and a Maven Central fetch into every stack rebuild, including the scheduled CI build that runs with no layer cache.  Building on the host into an ignored directory would make the repository un-runnable for anyone who has not first cloned two other repositories.  Git LFS keeps the clone honest: `.gitattributes` tracks `*.tar.gz`, and the CI workflow already checks out with `lfs: true`.

## Rebuilding

Requires a JDK 17: `gradle/common/environmentChecks.gradle` fails Test tasks on macOS above 17, and the wrapper is Gradle 8.9, which does not know Java 25.

```sh
cd ~/src/apache/cassandra-sidecar
git checkout mck/494/trunk
JAVA_HOME="$(jenv prefix 17)" ./gradlew distTar -Pversion=0.5-mck2 \
  -PanalyticsVersion=0.5-mck1 -x test -x integrationTest -x containerTest
cp build/distributions/apache-cassandra-sidecar-0.5-mck2.tar.gz \
   build/distributions/apache-cassandra-sidecar-0.5-mck2.tar.gz.sha256 \
   <repo>/cassandra/dist/
```

`-PanalyticsVersion=0.5-mck1` resolves cassandra-analytics from `~/.m2`, so `../../spark/ivy/README.md`'s two publish passes must have run first, with `-Pversion=0.5-mck1`.  The build also needs the four CDC modules published, which that file's recipe now names.

Check that the bundled reader is the patched one, because the version string alone does not say so:

```sh
python3 - <<'PY'
import tarfile, zipfile, io
t = tarfile.open("build/distributions/apache-cassandra-sidecar-0.5-mck2.tar.gz")
m = next(m for m in t if m.name.endswith("cassandra-analytics-cdc_spark3_2.12-0.5-mck1.jar"))
outer = zipfile.ZipFile(io.BytesIO(t.extractfile(m).read()))
inner = zipfile.ZipFile(io.BytesIO(outer.read("bridges/six-zero-bridge.jar")))
cls = inner.read("org/apache/cassandra/db/commitlog/BufferingCommitLogReader.class")
print("patched" if b"unknownTableCause" in cls else "STALE")
PY
```

The fork's unit tests are adapted to the 0.5 API but were not run: `test-common`'s test fixtures need `dtest-jars/dtest-4.1.11.jar`, which is built from Cassandra sources and is absent here, so `:server:compileTestJava` fails before any test of this repository's own change can run.  What is verified instead is the running stack: the Sidecar logs the tables it found, and `demo.drone_latest_status` mutations arrive in Kafka.

`distTar` is finalized by `generateDistributionChecksums`, so it also writes the zip, the source archives, the deb and the rpm; only the tarball and its `.sha256` are copied here.

**Do not add `--parallel`.**  The root project's `jar` task writes `server-common/src/main/resources/sidecar.version` in a `doFirst`, and `server-common:processResources` reads it, with no declared dependency between the two.  Run in parallel on a clean tree and the resource is packaged before it is written, so `lib/server-common-0.5-mck2.jar` carries no `sidecar.version`.  The Sidecar then fails at startup, in Guice, with `NullPointerException: Cannot invoke "java.io.InputStream.read(byte[])" because "input" is null` from `SidecarVersionProvider.<init>`, and the container exits 1 while Cassandra beside it is healthy.  Measured here: the first build was made with `--parallel` and failed exactly that way; the serial rebuild packaged the resource.  Check it rather than trusting the exit code:

```sh
tar -xzOf build/distributions/apache-cassandra-sidecar-0.5-mck2.tar.gz \
    apache-cassandra-sidecar-0.5-mck2/lib/server-common-0.5-mck2.jar > /tmp/sc.jar
unzip -p /tmp/sc.jar sidecar.version    # must print the version
```

## What the build carries, and what it does not

`lib/` holds `adapters-cassandra60-0.5-mck2.jar` beside the 4.1 and 5.0 adapters, which is what serves a 6.0 node.

`lib/` holds cassandra-analytics 0.5-mck1, including the four CDC modules, because CDC is the one Sidecar feature that reads those jars and it is enabled here.  `gradle.properties` still declares `analyticsVersion=0.4.0`, which has no 6.0 bridge; the `-PanalyticsVersion` override above is what replaces it.

That override is what made the tarball grow by 156,507,541 bytes, from 354,684,569 to 511,192,110 measured on the `0.5-mck0` build.  `cassandra-analytics-cdc_spark3_2.12-0.5-mck1.jar` nests one bridge set per supported release — four-zero, five-zero and six-zero, about 50 MB each — and CDC needs the six-zero one to deserialize a 6.0 commit log.  Trimming the other two would be an analytics build change rather than a flag here, so the cost is paid.

The `spark_bulk` path is unaffected either way: it resolves analytics from the Ivy repository in `../../spark/ivy/`, not from this `lib/`.

## Configuration

`../sidecar.yaml` is a trimmed 0.4.0 template, and 0.5 still accepts it.  The two keys most likely to have moved were checked against this build's own `conf/sidecar.yaml`: `driver_parameters.auth_provider.class_name` is still `org.apache.cassandra.sidecar.cluster.auth.ConfigProvider`, and the four `cassandra_input_validation` regexes are unchanged, so a `bti-ea` generation's component names still pass.

**One CDC setting in this build cannot be given the value it documents.**  `CdcConfigurationImpl` declares `cdcRawDirectoryMaxPercentUsage` a `float`, and `CdcRawDirectorySpaceCleaner` deletes only while the directory exceeds `cdc_total_space` times that fraction, whose default of 1.0 means the cleaner never fires before Cassandra's own bound.  Lowering it is refused by the setter: `setCdcRawDirectoryMaxPercentUsage` takes a `long`, so `0.75` arrives as 0 and the cleaner empties `cdc_raw` on its first pass, which was measured here from the log line `maxSize=0`.  So the key is absent from `../sidecar.yaml`, the node's own oldest-first deletion is the only trim, and `../entrypoint.sh` says so where it sets `cdc_block_writes`.

CDC reads three parts of that file.  `cassandra_instances[0].cdc_dir` and `commitlog_dir` are local paths, because this Sidecar runs inside the `cassandra` container rather than in one of its own, and they must match what `../entrypoint.sh` patches into `cassandra.yaml`.  `sidecar.cdc.enabled` turns the feature on, and `sidecar.schema.enabled` has to be on beside it: the Sidecar keeps its CDC settings and its per-segment progress in `sidecar_internal`, and `CassandraClusterSchemaMonitor` does not run at all while schema is off.  The settings themselves are rows in `sidecar_internal.configs`, which `../seed-cdc-configs.sh` writes once the Sidecar has created that table.
