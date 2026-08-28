#!/usr/bin/env bash
#
# Build the cqlite reader's C shared library from the cqlite fork, for one container
# platform, into htap-cqlite/dist/.  It writes `<name>.so.gz` and a `.sha256` beside it, not
# the library itself; the comment at the `gzip` call says why, and the image build is what
# decompresses.
#
# Why a script rather than a Dockerfile stage
# -------------------------------------------
# The reader's source does not live here.  `cqlite-datafusion` is a nested workspace in
# thelastpickle/cqlite, beside the patched `cqlite-core` it reads below, because both of
# this stack's fixes are internal to the reader: the token bound of `4bc6b913a` and the
# `ea` version letter of `f88541038`.  Compiled against the registry's cqlite-core the
# provider builds and then refuses Cassandra 6.0 files, so the library can only be built
# from the fork.  htap-cqlite/dist/VENDOR.md records which commit each library came from.
#
# Building it in the backend image instead cost 9m25s of every cold build when the reader
# shipped as a Python wheel, including the scheduled CI build that has no layer cache, and
# it put 313,000 lines of Rust in this repository to do it.
#
# What crosses, and why the version coupling is one number
# --------------------------------------------------------
# The library exports a C ABI of its own: it owns the DataFusion session, plans and runs
# the SQL itself, and hands rows back through the Arrow C Data Interface.  So the Java
# caller's Arrow version and this library's need not agree, and the whole of the coupling
# is `cqlite_abi_version()`, which the load check below reads.  The wheel this replaces
# had no such check: it handed DataFusion an `FFI_TableProvider` capsule, which is a struct
# layout rather than a published specification, so the datafusion wheel, the datafusion
# crate and datafusion-ffi all had to agree and a mismatch was a crash.
#
# Why one library per platform
# ----------------------------
# The library is glibc- and architecture-specific.  CI runs ubuntu-24.04 on amd64 and the
# development machine is darwin/arm64, so both are needed; the backend image picks one by
# its own architecture.
#
# The foreign architecture is CROSS-COMPILED, not emulated.  Emulating it was tried when
# the wheel was built the same way and does not work: under `podman build --platform
# linux/amd64` on an arm64 host, rustup installs the toolchain and then `rustc -vV` dies
# with "qemu: uncaught target signal 11 (Segmentation fault)", before a single crate
# compiles.  An emulated JVM is fine, so the load check below still runs in a container of
# the target platform.
#
# Usage: scripts/build-cqlite-so.sh <fork-commit-ish> <amd64|arm64>
#        FORK=~/src/mck/cqlite scripts/build-cqlite-so.sh mck/open-htap-stack arm64
set -euo pipefail

COMMITISH="${1:?usage: $0 <fork-commit-ish> <amd64|arm64>}"
ARCH="${2:?usage: $0 <fork-commit-ish> <amd64|arm64>}"
FORK="${FORK:-${HOME}/src/mck/cqlite}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DIST="${REPO_ROOT}/htap-cqlite/dist"

# The same image family the backend runtime uses, so the library links the glibc that will
# load it.  Measured, not assumed: `eclipse-temurin:25-jre` is Ubuntu 26.04 with glibc
# 2.43, and the `-jdk` tag beside it is the same suite, so the cross glibc this build
# installs is the one the runtime has.  A `rust:*-slim` builder would be a Debian suite
# away, and the failure would show up as an UnsatisfiedLinkError in the running container
# rather than here.
BASE_IMAGE="${BASE_IMAGE:-docker.io/library/eclipse-temurin:25-jdk}"
# Bootstraps rustup only.  The fork's own `rust-toolchain.toml` pins 1.97.1 and rustup
# honours it over an installed default, so that is the compiler either build uses; the
# nested workspace has no toolchain file of its own and inherits the root's.  1.97.1
# clears DataFusion 54's 1.88 floor, which is the only requirement here.
RUST_VERSION="${RUST_VERSION:-1.98.0}"
SO_VERSION="${SO_VERSION:-0.1.0}"

case "${ARCH}" in
  amd64) MACHINE=x86_64;  TRIPLE=x86_64-unknown-linux-gnu ;;
  arm64) MACHINE=aarch64; TRIPLE=aarch64-unknown-linux-gnu ;;
  *) echo "arch must be amd64 or arm64, not ${ARCH}" >&2; exit 1 ;;
esac
# `_c` because two crates in one workspace cannot both emit libcqlite_datafusion, and the
# Python crate already holds that name.
BUILT="libcqlite_datafusion_c.so"
SO="libcqlite_datafusion_c-${SO_VERSION}-linux-${MACHINE}.so"

if [[ ! -d "${FORK}/.git" ]]; then
  echo "No cqlite fork at ${FORK}.  Clone thelastpickle/cqlite there, or set FORK." >&2
  exit 1
fi

HOST_ARCH="$(podman info --format '{{.Host.Arch}}')"
COMMIT="$(git -C "${FORK}" rev-parse --verify "${COMMITISH}^{commit}")"
echo "== ${SO}"
echo "   fork    ${FORK}"
echo "   commit  ${COMMIT}"
git -C "${FORK}" log -1 --format='   subject %s%n   dated   %cs' "${COMMIT}"

# A library whose commit is only local cannot be traced back, and VENDOR.md claims it can.
# Warn rather than fail: a build made while iterating is legitimate, it just must not be
# the one that is committed.
if ! git -C "${FORK}" branch -r --contains "${COMMIT}" 2>/dev/null | grep -q .; then
  echo "   ! this commit is on no remote branch.  Push it before committing the library."
fi

STAGE="$(mktemp -d)"
trap 'rm -rf "${STAGE}"' EXIT

# `git archive` rather than a copy of the working tree: the library must come from the
# commit VENDOR.md names, not from whatever is staged or dirty beside it.  The whole
# repository travels, because the nested workspace patches `cqlite-core` by path and
# takes its pinned toolchain from the root.
git -C "${FORK}" archive --format=tar --prefix=cqlite/ "${COMMIT}" > "${STAGE}/cqlite.tar"
echo "   source  $(wc -c < "${STAGE}/cqlite.tar" | tr -d ' ') bytes of tar"

# The build container is always the HOST's platform.  For the host's own architecture that
# is a native build; for the other one it is a cross build, and the two differ only in the
# three ARGs below.
if [[ "${ARCH}" == "${HOST_ARCH}" ]]; then
  CROSS_PKG=""
  LINKER_ENV=""
  echo "   build   native on linux/${HOST_ARCH}"
else
  # Ubuntu's cross gcc for the target, from the same suite as the base image, so the cross
  # glibc is the glibc of the image that will load the library.
  # The package name and the binary name are not the same word: the amd64 package is
  # `gcc-x86-64-linux-gnu` with dashes and installs `x86_64-linux-gnu-gcc` with an
  # underscore, so name both rather than deriving one from the other.
  #
  # `libc6-dev-*-cross` is named because the build uses --no-install-recommends and the
  # cross gcc only recommends it.  Without it the compiler falls back to the host's
  # /usr/include and stops at `bits/libc-header-start.h: No such file or directory`,
  # inside zstd-sys, which is the first crate here with C in it.
  case "${ARCH}" in
    amd64) CROSS_PKG="gcc-x86-64-linux-gnu libc6-dev-amd64-cross"
           CROSS_CC="x86_64-linux-gnu-gcc" ;;
    arm64) CROSS_PKG="gcc-aarch64-linux-gnu libc6-dev-arm64-cross"
           CROSS_CC="aarch64-linux-gnu-gcc" ;;
  esac
  # Cargo reads the linker from a per-target variable; the triple is upper-cased with
  # dashes as underscores.
  LINKER_VAR="CARGO_TARGET_$(echo "${TRIPLE}" | tr 'a-z-' 'A-Z_')_LINKER"
  LINKER_ENV="${LINKER_VAR}=${CROSS_CC}"
  echo "   build   cross to linux/${ARCH} on a linux/${HOST_ARCH} container"
fi

cat > "${STAGE}/Dockerfile" <<'DOCKERFILE'
ARG BASE_IMAGE
FROM ${BASE_IMAGE}
ARG RUST_VERSION
ARG CROSS_PKG=""
ARG LINKER_ENV=""
ARG TRIPLE
ARG COMMIT

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH

RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential curl ${CROSS_PKG} \
 && rm -rf /var/lib/apt/lists/* \
 && curl -sSf https://sh.rustup.rs \
    | sh -s -- -y --no-modify-path --profile minimal \
      --default-toolchain "${RUST_VERSION}"

WORKDIR /src
ADD cqlite.tar /src/

# `rustup target add` runs from the workspace, so the toolchain file at the fork root
# decides which toolchain gets the target.  Adding it to the rustup default instead would
# install std for a compiler this build never invokes.
#
# --locked, so the build fails rather than resolving a different DataFusion than
# Cargo.lock records.  The Arrow C Data Interface makes a version difference between this
# library and its caller harmless, but a difference between two arrow crates inside this
# library is two unrelated RecordBatch types.
#
# CQLITE_GIT_SHA reaches `cqlite_build_info()`, so the loaded library names the commit it
# came from and VENDOR.md can be checked against a running container rather than trusted.
#
# CARGO_PROFILE_RELEASE_STRIP rather than a profile change in the fork: the debug symbols
# are most of the file, and every byte of it is committed to Git LFS here.
#
# The registry cache mount keeps a second build of the same commit short.  The target
# directory is not cached: a release build is the only thing this image does.
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    cd /src/cqlite/cqlite-datafusion \
 && rustup target add "${TRIPLE}" \
 && env ${LINKER_ENV} \
    CQLITE_GIT_SHA="${COMMIT}" \
    CARGO_PROFILE_RELEASE_STRIP=symbols \
    cargo build --release --locked --target "${TRIPLE}" \
      -p cqlite-datafusion-cabi \
 && mkdir -p /out \
 && cp "target/${TRIPLE}/release/libcqlite_datafusion_c.so" /out/ \
 && cp crates/cqlite-datafusion-cabi/include/cqlite_datafusion.h /out/
DOCKERFILE

IMAGE="cqlite-so-build:${ARCH}"
echo
echo "== Building"
SECONDS=0
podman build --platform "linux/${HOST_ARCH}" \
  --build-arg "BASE_IMAGE=${BASE_IMAGE}" \
  --build-arg "RUST_VERSION=${RUST_VERSION}" \
  --build-arg "CROSS_PKG=${CROSS_PKG}" \
  --build-arg "LINKER_ENV=${LINKER_ENV}" \
  --build-arg "TRIPLE=${TRIPLE}" \
  --build-arg "COMMIT=${COMMIT}" \
  -t "${IMAGE}" "${STAGE}"
echo "   built in ${SECONDS}s"

mkdir -p "${DIST}"
CID="$(podman create --platform "linux/${HOST_ARCH}" "${IMAGE}" true)"
podman cp "${CID}:/out/${BUILT}" "${DIST}/${SO}"
# The header travels with the libraries, because it is the declaration the Java binding is
# written against and it lives in the fork.  One copy serves both architectures and this run
# overwrites whatever the other run left, so the copy describes THIS commit and the sibling
# check below is what says the other library came from the same one.
podman cp "${CID}:/out/cqlite_datafusion.h" "${DIST}/cqlite_datafusion.h"
podman rm -f "${CID}" > /dev/null

# Committed gzipped, and Git LFS is the reason rather than the image.  LFS stores its objects
# as they are, so the raw pair would add 238.7 MB to this repository permanently and every
# rebuild would add as much again, against 726.2 MB already there.  Measured: `gzip -9` takes
# the pair to 78.3 MB, `zstd -19` to 44.1 MB and `xz -9` to 35.6 MB, and gzip is the only one
# of the three that `eclipse-temurin:25-jdk` and `:25-jre` already carry, so it is the only
# one that costs the image build no package fetch.  `-n` drops the name and the timestamp, so
# the same library always gzips to the same bytes.
RAW_BYTES="$(wc -c < "${DIST}/${SO}" | tr -d ' ')"
RAW_SHA="$(cd "${DIST}" && sha256sum "${SO}" | cut -d' ' -f1)"
gzip -9 -n -c "${DIST}/${SO}" > "${DIST}/${SO}.gz"
rm "${DIST}/${SO}"

# The checksum file's shape follows cassandra/dist/: `<sha>  <bare filename>`, which is
# what the Dockerfile checks with a plain `sha256sum -c`.  It covers the compressed file,
# because that is the file the image copies; the raw library's own checksum is printed below
# for VENDOR.md, which is where a decompressed library is identified.
(cd "${DIST}" && sha256sum "${SO}.gz" > "${SO}.gz.sha256")
printf '   %s  %s bytes\n' "${SO}" "${RAW_BYTES}"
printf '   %s  %s\n' "${RAW_SHA}" "${SO}"
printf '   %s  %s bytes\n' "${SO}.gz" "$(wc -c < "${DIST}/${SO}.gz" | tr -d ' ')"
sed 's/^/   /' "${DIST}/${SO}.gz.sha256"

# The pair has to come from one commit, and this is the only thing that says it does.  Each
# architecture is a separate run taking its own commit-ish, and the second run overwrites the
# header while leaving the first library where it is, so a pair built at two commits looks
# exactly like a pair built at one: same header, one VENDOR.md row, and each library passing
# its own load check.  CQLITE_GIT_SHA reaches the library as a string literal, so the sibling
# can be read here rather than in a container of its platform.  A warning and not a failure,
# because a build made while iterating is legitimate; it is the committed pair that must agree.
case "${MACHINE}" in
  x86_64)  SIBLING_ARCH=arm64; SIBLING_MACHINE=aarch64 ;;
  aarch64) SIBLING_ARCH=amd64; SIBLING_MACHINE=x86_64 ;;
esac
SIBLING="libcqlite_datafusion_c-${SO_VERSION}-linux-${SIBLING_MACHINE}.so.gz"
if [[ -f "${DIST}/${SIBLING}" ]]; then
  # Tested for decompressing first, because otherwise one message covers two states: with
  # `pipefail` set, a `gunzip` that fails leaves the condition below false, and a clone that
  # has not run `git lfs pull` holds a 130-byte pointer here and reads as a pair built at two
  # commits.
  if ! gunzip -t "${DIST}/${SIBLING}" 2> /dev/null; then
    echo "   ! ${SIBLING} does not decompress, so its commit cannot be read here."
    echo "   !   git lfs pull --include='htap-cqlite/dist/**', then run this again"
  # `grep -c` and not `-q`, because `-q` exits at the first match, `gunzip` then dies of
  # SIGPIPE, and `set -o pipefail` above reports the pipeline as failed on a match.
  elif gunzip -c "${DIST}/${SIBLING}" | grep -ca "${COMMIT}" > /dev/null; then
    echo "   ${SIBLING} carries the same commit"
  else
    echo "   ! ${SIBLING} was NOT built from ${COMMIT}, so the committed pair would disagree."
    echo "   !   $0 ${COMMIT} ${SIBLING_ARCH}"
  fi
else
  echo "   no ${SIBLING} yet, so the pair is incomplete"
fi

echo
echo "== Verifying the library loads in a clean linux/${ARCH} ${BASE_IMAGE##*/}"
# Both figures come from the header this run just copied out, so the check is header against
# library.  A constant here instead would fail a correct build the day the fork raises the ABI,
# and would let the header and the library disagree with nothing reporting it.
ABI_EXPECTED="$(awk '$1 == "#define" && $2 == "CQLITE_ABI_VERSION" { print $3 }' \
  "${DIST}/cqlite_datafusion.h")"
EXPORTS="$(grep -oE '^[A-Za-z].*\bcqlite_[a-z_]+\(' "${DIST}/cqlite_datafusion.h" \
  | grep -oE 'cqlite_[a-z_]+' | sort -u | tr '\n' ' ')"
if [[ -z "${ABI_EXPECTED}" || -z "${EXPORTS}" ]]; then
  echo "cannot read CQLITE_ABI_VERSION or the declarations from cqlite_datafusion.h" >&2
  exit 1
fi
echo "   header  ABI ${ABI_EXPECTED}, $(echo ${EXPORTS} | wc -w | tr -d ' ') exports declared"
# Pull first, and retry.  Docker Hub answered "unable to retrieve auth token: invalid
# username/password" twice while the wheel script was being written, and succeeded on the
# next attempt; a build that has just spent ten minutes should not be thrown away for that.
for attempt in 1 2 3; do
  podman pull -q --platform "linux/${ARCH}" "${BASE_IMAGE}" > /dev/null 2>&1 && break
  echo "   pull attempt ${attempt} failed; retrying"
  sleep 20
done

# The check is the loader the backend uses: `SymbolLookup.libraryLookup` dlopens the file
# and the two calls below are Panama downcalls, so a wrong glibc, a wrong architecture or a
# bad cross link fails here.  It resolves every export the header declares, not only the two
# it calls, because a dropped `#[no_mangle]` on any other one would otherwise surface at the
# first Java call.  Reading `cqlite_abi_version()` is the version check the binding does at
# startup, and `cqlite_build_info()` names the crate version and the commit, which the probe
# holds against `SO_VERSION` and `COMMIT` rather than only printing: those two are what the
# file name and VENDOR.md's rows are written from, and nothing else compares them.  The
# source arrives on stdin because podman on macOS shares only /Users with its virtual
# machine, so a file under $TMPDIR cannot be bind-mounted.  An emulated JVM is reliable
# where an emulated rustc is not, so this runs on the target platform even when the build
# did not.
#
# It decompresses first, which makes the check cover the committed artefact rather than an
# intermediate: `gunzip` here is the same step the image build does, so a truncated or
# mis-filtered `.so.gz` fails here and not in the image.
podman run --rm -i --platform "linux/${ARCH}" --network none \
  -v "${DIST}:/dist:ro" "${BASE_IMAGE}" \
  sh -c "cat > /tmp/Probe.java \
     && gunzip -c /dist/${SO}.gz > /tmp/${SO} \
     && ldd /tmp/${SO} | sed 's/^/   ldd /' \
     && java --enable-native-access=ALL-UNNAMED /tmp/Probe.java \
            /tmp/${SO} ${ABI_EXPECTED} ${SO_VERSION} ${COMMIT} ${EXPORTS}" <<'JAVA'
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.file.Path;

public final class Probe {

    /**
     * {@code <library> <abi the header declares> <version the file name carries> <commit
     * this build was given> <every name the header declares>}.
     */
    public static void main(String[] args) throws Throwable {
        int expectedAbi = Integer.parseInt(args[1]);
        String expectedVersion = args[2];
        String expectedCommit = args[3];
        Linker linker = Linker.nativeLinker();
        try (Arena arena = Arena.ofConfined()) {
            SymbolLookup library =
                    SymbolLookup.libraryLookup(Path.of(args[0]), arena);
            for (int i = 4; i < args.length; i++) {
                String name = args[i];
                library.find(name).orElseThrow(
                        () -> new AssertionError("the library exports no " + name));
            }
            System.out.println("   exports " + (args.length - 4) + ", all resolved");
            MethodHandle abiVersion = linker.downcallHandle(
                    library.find("cqlite_abi_version").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_INT));
            MethodHandle buildInfo = linker.downcallHandle(
                    library.find("cqlite_build_info").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.ADDRESS));

            int abi = (int) abiVersion.invokeExact();
            MemorySegment info = (MemorySegment) buildInfo.invokeExact();
            String build = info.reinterpret(Long.MAX_VALUE).getString(0);
            System.out.println("   abi     " + abi);
            System.out.println("   build   " + build);
            if (abi != expectedAbi) {
                throw new AssertionError("abi " + abi
                        + ", where the header declares " + expectedAbi);
            }
            // "cqlite-datafusion-cabi <version>; datafusion <version>; commit <sha>".  The
            // file name and VENDOR.md's version row both come from SO_VERSION, which nothing
            // else compares against the library: a fork that raised the crate to 0.2.0 would
            // otherwise write a 0.1.0 file name with every check green.  The commit is checked
            // beside it, because CQLITE_GIT_SHA reaches the library as a string literal and
            // a stale target directory would carry the previous one.
            String prefix = "cqlite-datafusion-cabi " + expectedVersion + ";";
            if (!build.startsWith(prefix)) {
                throw new AssertionError("the library declares " + build
                        + ", where this build names version " + expectedVersion);
            }
            if (!build.endsWith("commit " + expectedCommit)) {
                throw new AssertionError("the library declares " + build
                        + ", where this build names commit " + expectedCommit);
            }
        }
    }
}
JAVA

# A `--platform` run or build leaves the local base-image tag pointing at that platform,
# and `podman compose build backend` afterwards resolves the tag without asking for one.
# So a foreign-architecture run here would silently make the next backend build emulated.
# It happened while the wheel script was being written: the only symptom was a two-minute
# dependency install of x86_64 artefacts on an arm64 machine.  Put the tag back.
if [[ "${ARCH}" != "${HOST_ARCH}" ]]; then
  echo
  echo "== Restoring the local ${BASE_IMAGE##*/} tag to linux/${HOST_ARCH}"
  podman pull -q --platform "linux/${HOST_ARCH}" "${BASE_IMAGE}" > /dev/null
fi

echo
echo "== Now record ${COMMIT} in htap-cqlite/dist/VENDOR.md and commit htap-cqlite/dist/."
echo "   .gitattributes tracks *.so.gz in Git LFS; check with: git lfs ls-files | grep so.gz"
