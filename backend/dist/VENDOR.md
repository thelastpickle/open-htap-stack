# The cqlite reader's wheel, built here

`cqlite_datafusion-0.1.0-cp310-abi3-linux_*.whl` are local builds of the DataFusion table provider over Cassandra's SSTable files, which is the `cqlite` access path.  `../Dockerfile` copies one of them, picked by `uname -m`, and verifies the checksum beside it.

| | |
| --- | --- |
| Fork | [thelastpickle/cqlite](https://github.com/thelastpickle/cqlite), branch `mck/open-htap-stack` |
| Commit | `8f179fd1` ("feat: add cqlite-datafusion, a DataFusion table provider over SSTable files") |
| Upstream base | `2bde26a7` (#3328), on `main` |
| Built | 2026-08-24 |
| Version it declares | `0.1.0`; the crate is unpublished |
| Python | `cp310-abi3`, so one wheel per architecture serves CPython 3.10 and later |

| Wheel | Size | sha256 |
| --- | --- | --- |
| `…-linux_aarch64.whl` | 20,385,999 bytes | `45aa36a2e9eef0e445ce7d7b59c80090e63459e8b30d0895de31390411ad0066` |
| `…-linux_x86_64.whl` | 21,429,769 bytes | `a2c6f917adbe27ff663fc432ad7ccf28e9f7d5b053750e80603fb8d928b88486` |

Both are stored in Git LFS; `.gitattributes` tracks `*.whl`, and the CI workflow checks out with `lfs: true`.

## Why the source is not here

The provider reads below cqlite's query layer.  `scan.rs` steps `StreamingMerger::step_streaming()` straight into typed Arrow builders, so it depends on the reader's internals and on two fixes the fork carries.  Compiled against the registry's `cqlite-core 0.16.1` it builds and then gets both of them wrong: it refuses Cassandra 6.0 files, and a token split returns every row once per slice.  So the crates live beside the patched reader, as a nested workspace in the fork, and a modification is made where the code it modifies lives.

That is the same arrangement as `../../cassandra/dist/` for the Sidecar and `../../spark/ivy/` for the Analytics jars: the fork holds the change, this repository holds the artefact.

## Why the artefact is committed

Compiling it here cost 9 min 25 s of every cold backend build, and CI pays that on the scheduled run where no layer cache exists.  It also put 313,000 lines of Rust in the tree, across 503 files and 13.3 MB, of which 484 were a copy of upstream cqlite.

The cost of the change is that a reader change is no longer one command.  It is a fork commit, then `../../scripts/build-cqlite-wheel.sh` twice, then a commit here.  Measured on this commit on a seven-core darwin/arm64 machine: 11 min 56 s of `cargo build` for the native arm64 wheel, and 7 min 58 s for the cross-compiled amd64 one.

Emulating the foreign architecture was tried first and does not work.  Under `podman build --platform linux/amd64`, rustup installs the toolchain and then `rustc -vV` dies with "qemu: uncaught target signal 11 (Segmentation fault)", before a single crate compiles.  So the script cross-compiles instead, with Debian's `gcc-x86-64-linux-gnu` from the same suite as the base image; both are Debian 13 (trixie) at glibc 2.41, which is what makes the cross link the right one.  Emulated CPython is reliable where emulated rustc is not, so the script still verifies each wheel by importing it in a container of the target platform.

## The three-way pin

The `datafusion` and `datafusion-ffi` crates the wheel was compiled against, and the `datafusion` wheel in `../requirements.txt`, are one pin and not three.  `FFI_TableProvider` is `#[repr(C)]`, and the capsule crossing the boundary is read as that struct by whichever library the other side was built against.  Both sides are at **54**.

This file is what holds that pin now.  While the reader was compiled here, `--locked` against a committed `Cargo.lock` held the Rust side; a prebuilt wheel has no lockfile in this repository, so raising `datafusion` in `../requirements.txt` without rebuilding the wheel from a fork commit that raised the crates would be undetected until a query segfaults.

The Rust crates are published at 55.0.0, which carries a newer arrow; the Python wheel stops at 54.0.0.  So 54 is the highest both sides can hold, and DataFusion 55 waits on PyPI.  Raise all three together or none of them.

## What the fork branch carries

Three commits above `2bde26a7`, and the first two are why the wheel cannot come from crates.io.

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

### `8f179fd1` — the provider crates

`cqlite-datafusion/` in the fork, a nested workspace listed in the root `exclude` so no root `cargo build` pulls DataFusion 54 in for members that do not need it.  38 unit tests and a doctest, in about a second.  `[patch.crates-io]` points `cqlite-core` at `../cqlite-core`, and a `[patch]` binds only the workspace declaring it, so `crates/cqlite-datafusion` still names plain `cqlite-core = "0.16.1"` and stays publishable.

## DataFusion is not modified

`thelastpickle/datafusion` has a branch `mck/open-htap-stack`, at `a6e2d3f7a` on `main`, and it carries no commit.  It exists so a future patch has a home.  DataFusion is an unmodified crates.io dependency of the provider and an unmodified PyPI dependency of the backend; nothing here changes either.

## Rebuilding

```sh
# from the repository root
scripts/build-cqlite-wheel.sh mck/open-htap-stack arm64
scripts/build-cqlite-wheel.sh mck/open-htap-stack amd64
```

The script archives the fork at the named commit, builds in a throwaway container of the *same* base image the backend runs (`python:3.14-slim`), writes the wheel and its `.sha256` here, and then verifies the wheel by installing it in a clean container of that platform and importing `SSTableProvider`.  Importing is the whole check: it dlopens the abi3 library, which is what a wrong glibc or a wrong architecture fails on.

`FORK` overrides the fork path, `~/src/mck/cqlite` by default.  Both architectures are needed: CI runs ubuntu-24.04 on amd64, and a development machine here is darwin/arm64.

`--compatibility linux`, not a manylinux tag.  The wheel is not for PyPI; the base image identity is what makes the glibc match, and `linux_x86_64` and `linux_aarch64` are both in pip's supported tags.  Asking for manylinux would fail the audit on a library deliberately built against this glibc.

Then update the two tables above and commit `backend/dist/`.  Check that Git LFS took the wheels rather than committing them as blobs:

```sh
git lfs ls-files | grep whl
```
