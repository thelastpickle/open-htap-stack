# The cqlite reader's C library, built here

tl;dr: `libcqlite_datafusion_c-0.1.0-linux-*.so.gz` is the `cqlite` access path with a C boundary in front of it, so the Java backend can drive it over Panama. &emsp;One library per architecture, gzipped, in Git LFS, with a checksum beside it. &emsp;It is built from a commit in the cqlite fork by `../../scripts/build-cqlite-so.sh`, and the whole of its version coupling is one number: `cqlite_abi_version()`.

- [What was built](#what-was-built)
- [Why a C ABI and not the DataFusion capsule the Python reader used](#why-a-c-abi-and-not-the-datafusion-capsule-the-python-reader-used)
- [Why the source is not here](#why-the-source-is-not-here)
- [Why the artefact is committed](#why-the-artefact-is-committed)
- [Why it is gzipped](#why-it-is-gzipped)
- [Why it is twice the size of the Python reader's library](#why-it-is-twice-the-size-of-the-python-readers-library)
- [What a caller needs at run time](#what-a-caller-needs-at-run-time)
- [What the fork commits carry](#what-the-fork-commits-carry)
- [Rebuilding](#rebuilding)

`914e1280` is the fourth of four commits above `2bde26a7`, and the three below it are why this library cannot come from crates.io: the provider crates, the `ea` version letter Cassandra 6.0 writes, and a query's token bound on a BTI reader. &emsp;Each is described below. &emsp;A Python wheel over the same three commits served this access path until the backend was ported; its own file went with it, and what it recorded about those three commits is here.

## What was built

| | |
| --- | --- |
| Fork | [thelastpickle/cqlite](https://github.com/thelastpickle/cqlite), branch `mck/open-htap-stack` |
| Commit | `914e1280` ("feat: add a C boundary over cqlite-datafusion, scoped per query") |
| Upstream base | `2bde26a7` (#3328), on `main` |
| Built | 2026-08-29 |
| Version it declares | `0.1.0`; the crate is unpublished |
| ABI version | `1`, which `CQLITE_ABI_VERSION` in `cqlite_datafusion.h` declares and `cqlite_abi_version()` reports |
| DataFusion | 54.1.0, statically linked |
| Rust | 1.97.1, which the fork's own `rust-toolchain.toml` pins; the script's `RUST_VERSION` bootstraps rustup and is then overridden by that file |
| glibc | 2.43, from `eclipse-temurin:25-jdk`, which is Ubuntu 26.04 |

| Committed file | Size | sha256 |
| --- | --- | --- |
| `…-linux-aarch64.so.gz` | 37,878,472 bytes | `0e034e7ac9303203bff71e6667352d169719f7ad096b7ca0084c7b90933d90c6` |
| `…-linux-x86_64.so.gz` | 40,413,378 bytes | `e588280360428ea2e9705a62d5813e357215ab3b4aa269738ca17f684bce23d1` |

The `.sha256` beside each covers the compressed file, because that is what the image copies. &emsp;What comes out of it:

| Library | Size | sha256 |
| --- | --- | --- |
| `…-linux-aarch64.so` | 114,818,408 bytes | `e4c0609461467fd253c3f93a99ccb8dc2c578af9c4f8c28cc729a2415c90b899` |
| `…-linux-x86_64.so` | 123,869,440 bytes | `9a31262a5fbe9e4a731659e4006e08ff0a1759e38532bdeaad800d745f524f43` |

Both compressed files are stored in Git LFS; `.gitattributes` tracks `*.so.gz` and `*.so` alike, and `.gitignore` keeps a raw library out of a commit altogether. &emsp;They are 78.3 MB of the repository's 762.7 MB of LFS objects, over the 72 tracked files that remain now that the Python reader's two wheels are deleted. &emsp;Two more rebuilds of the pair and GitHub's free 1 GB a month is gone: LFS never replaces an object, so a rebuild adds 78.3 MB beside the old one rather than over it, giving 841.0 MB and then 919.3 MB. &emsp;So rebuild for a reason, and read the [Rebuilding](#rebuilding) section's "a rebuild for a comment" line as arithmetic rather than as taste.

**Both CI workflows that build an image now read them.** &emsp;`test-podman-compose.yaml` checks out with `lfs: false` and pulls `cassandra/dist/**`, `htap-cqlite/dist/**` and `spark/ivy/**` by name, because the backend image copies one of these libraries and verifies its checksum; `publish-images.yaml` names `htap-cqlite/dist/**` on its `backend` entry for the same reason. &emsp;A pull that did not name this path left the build copying a 130-byte pointer and dying at the checksum, which is the failure the compose workflow's second assertion now catches: it fails if any LFS object is left unnamed. &emsp;`java-tests.yaml` sets no `lfs` at all, deliberately, because the reactor's tests need no artefact.

**`914e1280` is not pushed yet.** &emsp;The tip of `thelastpickle/mck/open-htap-stack` is `8f179fd1`, the commit below it, so the commit these libraries name can be read only in a local clone until someone pushes it. &emsp;`cqlite_build_info()` will report it either way, which is the hazard: the string looks traceable and is not. &emsp;The build script warns when the commit it is given is on no remote branch, and this line is that warning recorded rather than dismissed.

`cqlite_datafusion.h` is committed beside them and is not the fork's file by reference but a copy of it. &emsp;It is what a reader of the Java binding compares against, and the `_Static_assert` lines in it are the struct-layout check a C caller gets at compile time and a Panama caller does not.

## Why a C ABI and not the DataFusion capsule the Python reader used

That reader handed Python an `FFI_TableProvider` capsule and let Python own the DataFusion session. &emsp;That makes three things one pin: the `datafusion` crate the library was built against, `datafusion-ffi`, and the `datafusion` wheel the host installs. &emsp;`FFI_TableProvider` is a `#[repr(C)]` struct rather than a published specification, so raising any one of the three was a segfault rather than a build failure, and that reader's own vendor file existed in part to hold the pin by hand.

This library owns the session instead. &emsp;It parses and plans the SQL itself and hands rows back over the Arrow C Data Interface, which is a published specification with a stable layout, so the caller's Arrow version and this library's need not match at all. &emsp;What is left to check is one integer at load, and a caller that finds a number it does not know registers nothing.

The cost of moving the boundary is that the library must carry DataFusion's SQL front end, which is most of the size difference below.

## Why the source is not here

The same reason the wheel's file gives: the provider reads below cqlite's query layer, straight from `StreamingMerger::step_streaming()` into typed Arrow builders, so it depends on the reader's internals and on two fixes the fork carries. &emsp;Compiled against the registry's `cqlite-core 0.16.1` it builds and then refuses Cassandra 6.0 files and returns every row once per token slice. &emsp;So the crates live beside the patched reader as a nested workspace in the fork, and a change is made where the code it changes lives.

That is the arrangement `../../cassandra/dist/` uses for the Sidecar and `../../spark/ivy/` for the Analytics jars: the fork holds the change, this repository holds the artefact.

## Why the artefact is committed

Compiling Rust in the backend image cost 9 min 25 s of every cold build while the Python backend did it, and CI pays that on the scheduled run where no layer cache exists. &emsp;Nothing in the Java backend image compiles either, for the same reason.

The cost is that a reader change is a fork commit, then `../../scripts/build-cqlite-so.sh` twice, then a commit here. &emsp;Measured on this commit on a seven-core darwin/arm64 machine: 940 s for the native arm64 library, of which `cargo build` was 12 min 05 s, and 799 s for the cross-compiled amd64 one, of which `cargo build` was 9 min 40 s. &emsp;The cross build being the faster of the two is what the wheel's build measured as well, at 7 min 58 s against 11 min 56 s, and neither pair was run under a controlled load, so read the ordering rather than the ratio.

Emulating the foreign architecture does not work and was not tried again here: under `podman build --platform linux/amd64`, rustup installs the toolchain and `rustc -vV` then dies with "qemu: uncaught target signal 11 (Segmentation fault)" before a crate compiles. &emsp;So the script cross-compiles with the base image's own `gcc-x86-64-linux-gnu`, which is what makes the linked glibc the one that will load the library, and verifies the result by loading it in a container of the target platform, where emulation is reliable.

## Why it is gzipped

Git LFS stores its objects as they are and compresses nothing, so the raw pair would add 238.7 MB to this repository for good, and each rebuild would add as much again beside it rather than in place of it. &emsp;726.2 MB is already there, over half of it one Sidecar tarball, and GitHub's free LFS allowance is 1 GB.

Measured on this pair: `gzip -9` gives 78.3 MB, `zstd -19` 44.1 MB and `xz -9` 35.6 MB. &emsp;gzip is the largest of the three and is the one taken, because it is the only one `eclipse-temurin:25-jdk` and `:25-jre` already carry: `xz` and `zstd` are in neither, so either would put a package fetch in the image build, and this repository's rule is that a build-time download carries a checksum. &emsp;`gzip -9 -n` drops the name and the timestamp, so the same library always compresses to the same bytes and a rebuild that changed nothing produces an identical file.

The image build decompresses, not the running process. &emsp;A library must be a real path for `SymbolLookup.libraryLookup`, and unpacking at startup would want a writable directory and would put 114 MB of I/O in front of the first request; the container image is the same size either way.

## Why it is twice the size of the Python reader's library

114,818,408 bytes against the wheel's inner `cqlite_datafusion.abi3.so` at 58,018,632, and both are stripped: `.text` is 82,678,144 bytes here against 40,611,780 there, with no debug section in either.

The extra code is DataFusion's SQL front end and its function library, which is what owning the session buys. &emsp;Measured with `strings` over both libraries: `regexp_replace`, `date_bin`, `approx_percentile_cont` and `arrow_cast` each appear here and appear nowhere in the wheel's library, and `sqlparser` appears 20 times here against 11. &emsp;A capsule provider is asked to scan a table and never to parse a statement, so the linker drops all of it.

Read that as the boundary's price rather than as something to tune. &emsp;`lto = false` with 16 codegen units is a deliberate trade in the fork's `[profile.release]`, because the amd64 build is a container build and wall clock is a real cost; a size sweep would have to measure scan throughput as well.

## What a caller needs at run time

The library links only the C runtime. &emsp;`ldd` on the arm64 build lists `linux-vdso.so.1`, `libgcc_s.so.1`, `libm.so.6`, `libc.so.6` and `ld-linux-aarch64.so.1`, and the x86_64 build lists the same five with its own loader. &emsp;There is no Python, no JVM and no Cassandra in that list, which is the access path's whole claim.

A Java caller should pass `--enable-native-access=ALL-UNNAMED`: every call into this library is a Panama downcall, which JDK 24 made restricted, so without the flag JDK 25 warns on the first call and a later release will refuse it. &emsp;Nothing here has measured the flagless case, because the build script's probe always passes the flag. &emsp;The Java binding and what it needs beyond that arrive in the commit after this one.

The library reads `cassandra-data/` where it lies and needs it mounted read-only, exactly as the wheel did. &emsp;It never writes.

## What the fork commits carry

`914e1280` adds `crates/cqlite-datafusion-cabi`, 2,517 lines over 12 files, and changes `cqlite-datafusion` in two places to give a statement its own scope. &emsp;47 unit tests drive the exports as C drives them, and the crate builds with no change to the two commits below it.

Three of its decisions are worth knowing before changing the boundary.

**A statement is the unit of cancellation and of accounting.** &emsp;Without `QueryScope`, a scan draws its cancellation tokens from the provider, so cancelling one query stops every query on the table, and the provider's `last_scan` holds only the newer of two scans in flight. &emsp;The scope rides the session configuration as an extension; a caller that hands the provider to another library still gets the old behaviour, so the scope is an addition rather than a replacement.

**Registration refuses a column type outside the twelve the header lists.** &emsp;A caller reading the C stream by hand needs that set to be finite. &emsp;The check is at registration because that is where the set is exactly enumerable: a statement's output can be wider, since `count(*)` is `Int64` whatever it counts, and both the header and the code say so rather than implying a guarantee the check cannot give.

**An open option above 1,048,576 is refused.** &emsp;`token_splits` builds a `Vec` of `splits` entries, so `u64::MAX` aborts the process, and a checked conversion alone caught nothing because every `u64` fits a `usize` on the targets this ships for. &emsp;The figure is generous rather than measured. &emsp;The defaults are 1 split, 8,192 rows a batch and 1 partition at a time, and two of those three carry a measurement in the header: `splits` and `key_chunk` do, and `batch_rows` is DataFusion's own default with no run behind it here.

The library is `libcqlite_datafusion_c` because two crates in one workspace cannot both emit `libcqlite_datafusion`, and the Python crate already holds that name.

**Three things the header does not say, and they are the fork's to correct rather than this copy's.** &emsp;Editing the copy here would make it stop describing the commit the libraries came from, and a rebuild for a comment costs half an hour and a second pair of LFS objects, so these arrive with the next rebuild that has another reason. &emsp;All three answers are read from `crates/cqlite-datafusion-cabi/src/lib.rs` at `914e1280`. &emsp;`cqlite_query` checks both out-parameters for null before it does any work and writes neither unless the query succeeded, so on a negative return `*out_stream` is untouched and a caller that zeroed the struct may test `release == NULL`; the header documents only the success case. &emsp;`cqlite_stmt_scan` declares `int` and says nothing about which codes it returns, where `cqlite_cancel` beside it does: the answer is `CQLITE_ERROR_BAD_ARGUMENT` for a null `stmt` or a null `out`, and `CQLITE_ERROR_PANIC` if a panic crossed the boundary, which `guard` gives every export. &emsp;And the file's second line separates its name from its description with an em-dash, where the style this repository writes to takes a colon.

### `914e1280`'s three commits below it

### `4bc6b913` — apply a query's token bound on a BTI reader, which dropped it

`ScanTokenBound::contains` was called in one place, the Summary-guided walk in `summary_scan/mod.rs`, and `stream_all_partitions_for_query` gates that walk on `bti_partitions_db.is_none()`.  Every BTI generation, `da` or `ea`, has a `Partitions.db`, so such a reader falls through to the full-ring routes, whose signatures take no bound.  The commit adds `TokenGate` at the one emit both of those routes pass through, above every format branch.

The provider tests the same bound again in `scan.rs::in_slice`, because the bound is documented as a hint the consumer must enforce and the crate is published against a registry `cqlite-core` that has no gate.  Without that second test, four slices over a 100-row table returned 400 rows.

What filtering inside the reader saves, measured on one 203.7 MB `da` generation holding 1,102,576 rows of `demo.events`, median of three interleaved rounds of CPU time (user+sys, because the host's load average made a wall clock unusable), with an equivalent gate one layer above `TokenGate`'s:

| Slices | CPU without the gate | CPU with the gate | Peak resident with the gate |
| --- | --- | --- | --- |
| 1 | 16.45 s | 11.79 s | 35 MB |
| 2 | 38.88 s | 22.05 s | 36 MB |
| 4 | 71.16 s | 40.33 s | 36 MB |
| 7 | 127.73 s | 73.01 s | 38 MB |

Peak resident without the gate reached 716 MB at seven slices, because every producer converted every partition and the out-of-slice rows queued in the merge before the consumer discarded them.  The gate holds it at 35 to 39 MB whatever the slice count.  `TokenGate` filters earlier than the gate those figures were taken with, so read them as the floor of what it saves.

The gate does **not** make splitting pay, and the same table says why: solving N·P + R over the 2- and 4-slice points gives 71% of a slice repeated and 29% divided, in both sweeps.  Each slice re-reads and re-parses the whole data section, because this route has no partition-index seek.  The fit is a fit: it predicts 67.8 s at seven slices against the 73.01 s measured, and over-predicts the one-slice point, so read the 71% as the order of the repeated share rather than as a constant.  That is why `cqlite_splits` stays at 1.

### `f8854103` — accept BTI `ea`, the Cassandra 6.0 version letter

Five sites, no upstream line removed.  `version_gate/bti.rs` admits `ea` beside `da`; `format_detector.rs` maps `ea` to `V5x` and lists it in `is_supported`; `reader/header_helpers.rs` adds it to the version letters generation extraction recognises.

The letters are layout-identical for a table on the default compressor, and 6.0's own source says so.  `BtiFormat.java:296-297` in 6.0-alpha2 reads:

```java
// da (5.0): initial version of the BTI format
// ea (6.0): compression dictionary metadata in CompressionInfo component
```

Two facts make that section harmless here.  `CompressionMetadata.doPrepare()` writes the header, then the chunk offsets, then calls `writeCompressionDictionary` **last**, and that method is `if (compressionDictionary == null) return;`.  So a table with no dictionary writes no section at all, and `compression_info.rs` stops reading after the chunk offsets, which means a table that *does* carry one is not mis-parsed either; it is refused by `zstd_dictionary_option()`, which fails closed on any option key naming a dictionary.

Every feature gate agrees letter for letter.  `BtiVersion` in 6.0-alpha2 overrides each `has*()` with a constant, identical to 5.0.8's, and reads the version string only in `isLatestVersion`, `isCompatible` and `isCompatibleForStreaming`, none of which this crate calls.  6.0 adds no new `has*()` method to `Version`.  The error's `floor` stays `"da"`, which is still 6.0's own `earliest_supported_version`.

The `header_helpers.rs` site was missed while the reader was vendored here, and the stack worked anyway by luck: the numeric fallback returns the first number in the filename inside `0 < n < 1_000_000`, so a generation of a million or more would have read as 0.  Two things hid it.  The vendored copy's `[dev-dependencies]` were removed, so no inline test module compiled; and the existing `test_extract_generation_from_path` never calls the function under test, asserting only that a temporary file exists.  The fork has both fixed, and its `version_gate/mod.rs` test that asserted `ea` must be rejected is repaired to use `fa` for the next letter Cassandra writes.

### `8f179fd1` — the provider crates the boundary sits on

`cqlite-datafusion/` in the fork, a nested workspace listed in the root `exclude` so no root `cargo build` pulls DataFusion 54 in for members that do not need it.  38 unit tests and a doctest, in about a second.  `[patch.crates-io]` points `cqlite-core` at `../cqlite-core`, and a `[patch]` binds only the workspace declaring it, so `crates/cqlite-datafusion` still names plain `cqlite-core = "0.16.1"` and stays publishable.

## DataFusion is not modified

`thelastpickle/datafusion` has a branch `mck/open-htap-stack`, at `a6e2d3f7a` on `main`, and it carries no commit.  It exists so a future patch has a home.  DataFusion is an unmodified crates.io dependency of the provider and a statically linked dependency of this library; nothing here changes it.

## Rebuilding

```sh
# from the repository root
scripts/build-cqlite-so.sh 914e12807 arm64
scripts/build-cqlite-so.sh 914e12807 amd64
```

The script archives the fork at the named commit, builds `--locked` in a throwaway container of the same base image the Java backend runs, writes the gzipped library and its `.sha256` here, and then decompresses and loads it in a clean container of that platform through Panama, reading `cqlite_abi_version()` and `cqlite_build_info()`. &emsp;Loading is most of the check: a wrong glibc, a wrong architecture, a missing export or a `.gz` the LFS filter mangled all fail there, and the build string names the crate version and the commit, so the artefact says what it was built from without this file being trusted. &emsp;The probe holds that string against the version in the file name and the commit the run was given, so a fork that raised the crate cannot produce a `0.1.0` file with every check green. &emsp;It decompresses from the committed file rather than from the build's own output for exactly that reason.

Two details of that check are worth knowing, because both were once weaker than the sentence above. &emsp;It resolves every name the header declares, all thirteen, and not only the two it calls, so a dropped `#[no_mangle]` fails here rather than at the first Java call. &emsp;And the version it expects comes from `CQLITE_ABI_VERSION` in the header it has just copied out of the build image, so the check is header against library; a constant in the script would have failed a correct build the day the fork raises the ABI.

The two architectures are two runs and each takes its own commit-ish, so the pair's agreement is the operator's to keep. &emsp;What guards it is a warning: after writing its own file the script reads the sibling `.so.gz`, whose build string is a literal in the data section, and says so if the commit is not the one it was just given.

`FORK` overrides the fork path, `~/src/mck/cqlite` by default. &emsp;Both architectures are needed: CI runs ubuntu-24.04 on amd64, and a development machine here is darwin/arm64. &emsp;The script warns if the named commit is on no remote branch, because a library whose `cqlite_build_info` names an unpushed commit cannot be traced back to source by anyone else.

Then update the tables above and commit `htap-cqlite/dist/`. &emsp;Check that Git LFS took the libraries rather than committing them as blobs:

```sh
git lfs ls-files | grep so.gz
```
