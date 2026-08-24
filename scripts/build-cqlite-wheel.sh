#!/usr/bin/env bash
#
# Build the cqlite reader's Python wheel from the cqlite fork, for one container
# platform, into backend/dist/.
#
# Why a script rather than a Dockerfile stage
# -------------------------------------------
# The reader's source does not live here.  `cqlite-datafusion` is a nested workspace in
# thelastpickle/cqlite, beside the patched `cqlite-core` it reads below, because both of
# this stack's fixes are internal to the reader: the token bound of `4bc6b913a` and the
# `ea` version letter of `f88541038`.  Compiled against the registry's cqlite-core the
# provider builds and then refuses Cassandra 6.0 files, so the wheel can only be built
# from the fork.  backend/dist/VENDOR.md records which commit each wheel came from.
#
# Building it here in the backend image instead cost 9m25s of every cold build, including
# the scheduled CI build that has no layer cache, and it put 313,000 lines of Rust in
# this repository to do it.
#
# Why one wheel per platform
# --------------------------
# pyo3's abi3-py310 makes one wheel serve CPython 3.10 and later, but the shared library
# inside it is still glibc- and architecture-specific.  CI runs ubuntu-24.04 on amd64 and
# the development machine is darwin/arm64, so both are needed: `uname -m` inside the image
# build picks one.
#
# The foreign architecture is CROSS-COMPILED, not emulated.  Emulating it was tried and
# does not work: under `podman build --platform linux/amd64` on an arm64 host, rustup
# installs the toolchain and then `rustc -vV` dies with "qemu: uncaught target signal 11
# (Segmentation fault)", before a single crate compiles.  Emulated CPython is fine, so the
# verify step below still runs in a container of the target platform.
#
# Usage: scripts/build-cqlite-wheel.sh <fork-commit-ish> <amd64|arm64>
#        FORK=~/src/mck/cqlite scripts/build-cqlite-wheel.sh mck/open-htap-stack arm64
set -euo pipefail

COMMITISH="${1:?usage: $0 <fork-commit-ish> <amd64|arm64>}"
ARCH="${2:?usage: $0 <fork-commit-ish> <amd64|arm64>}"
FORK="${FORK:-${HOME}/src/mck/cqlite}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DIST="${REPO_ROOT}/backend/dist"

# The same base the backend runtime uses, so the library links the glibc that will load
# it.  A `rust:*-slim` builder would be one Debian suite away from a different one, and
# the failure would show up as an import error in the running container rather than here.
BASE_IMAGE="${BASE_IMAGE:-docker.io/library/python:3.14-slim}"
# Bootstraps rustup only.  The fork's own `rust-toolchain.toml` pins 1.97.1 and rustup
# honours it over an installed default, so that is the compiler either build uses; the
# nested workspace has no toolchain file of its own and inherits the root's.  1.97.1
# clears DataFusion 54's 1.88 floor, which is the only requirement here.
RUST_VERSION="${RUST_VERSION:-1.98.0}"
WHEEL_VERSION="${WHEEL_VERSION:-0.1.0}"

case "${ARCH}" in
  amd64) MACHINE=x86_64;  TRIPLE=x86_64-unknown-linux-gnu ;;
  arm64) MACHINE=aarch64; TRIPLE=aarch64-unknown-linux-gnu ;;
  *) echo "arch must be amd64 or arm64, not ${ARCH}" >&2; exit 1 ;;
esac
WHEEL="cqlite_datafusion-${WHEEL_VERSION}-cp310-abi3-linux_${MACHINE}.whl"

if [[ ! -d "${FORK}/.git" ]]; then
  echo "No cqlite fork at ${FORK}.  Clone thelastpickle/cqlite there, or set FORK." >&2
  exit 1
fi

HOST_ARCH="$(podman info --format '{{.Host.Arch}}')"
COMMIT="$(git -C "${FORK}" rev-parse --verify "${COMMITISH}^{commit}")"
echo "== ${WHEEL}"
echo "   fork    ${FORK}"
echo "   commit  ${COMMIT}"
git -C "${FORK}" log -1 --format='   subject %s%n   dated   %cs' "${COMMIT}"

# A wheel whose commit is only local cannot be traced back, and VENDOR.md claims it can.
# Warn rather than fail: a build made while iterating is legitimate, it just must not be
# the one that is committed.
if ! git -C "${FORK}" branch -r --contains "${COMMIT}" 2>/dev/null | grep -q .; then
  echo "   ! this commit is on no remote branch.  Push it before committing the wheel."
fi

STAGE="$(mktemp -d)"
trap 'rm -rf "${STAGE}"' EXIT

# `git archive` rather than a copy of the working tree: the wheel must come from the
# commit VENDOR.md names, not from whatever is staged or dirty beside it.  The whole
# repository travels, because the nested workspace patches `cqlite-core` by path and
# takes its pinned toolchain from the root.
git -C "${FORK}" archive --format=tar --prefix=cqlite/ "${COMMIT}" > "${STAGE}/cqlite.tar"
echo "   source  $(wc -c < "${STAGE}/cqlite.tar" | tr -d ' ') bytes of tar"

# The build container is always the HOST's platform.  For the host's own architecture
# that is a native build; for the other one it is a cross build, and the two differ only
# in the three ARGs below.
if [[ "${ARCH}" == "${HOST_ARCH}" ]]; then
  CROSS_PKG=""
  LINKER_ENV=""
  echo "   build   native on linux/${HOST_ARCH}"
else
  # Debian's cross gcc for the target, from the same suite as the base image, so the
  # cross glibc is the glibc of the image that will load the wheel.  Checked: both are
  # Debian 13 (trixie), glibc 2.41.
  # The package name and the binary name are not the same word: Debian spells the amd64
  # package `gcc-x86-64-linux-gnu` with dashes and installs `x86_64-linux-gnu-gcc` with
  # an underscore, so name both rather than deriving one from the other.
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

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH

RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential curl ${CROSS_PKG} \
 && rm -rf /var/lib/apt/lists/* \
 && curl -sSf https://sh.rustup.rs \
    | sh -s -- -y --no-modify-path --profile minimal \
      --default-toolchain "${RUST_VERSION}" \
 && pip install --no-cache-dir "maturin>=1.7,<2.0"

WORKDIR /src
ADD cqlite.tar /src/

# `rustup target add` runs from the workspace, so the toolchain file at the fork root
# decides which toolchain gets the target.  Adding it to the rustup default instead
# would install std for a compiler this build never invokes.
#
# --locked, so the build fails rather than resolving a different DataFusion than
# Cargo.lock records: the capsule the reader hands the datafusion wheel is a repr(C)
# struct, and both sides must agree on its layout.
#
# --compatibility linux rather than a manylinux tag.  The wheel is not for PyPI; the
# base image above is what makes the glibc match, and `linux_x86_64` and `linux_aarch64`
# are both in pip's supported tags.  Asking for manylinux would fail the audit on a
# library that is deliberately built against this glibc.
#
# --target is always passed, so a native and a cross build take the same code path and
# write the same wheel tag.  An abi3 wheel needs no target interpreter, which is what
# makes cross-compiling it a linker question alone.
#
# The registry cache mount keeps a second build of the same commit short.  The target
# directory is not cached: a release build is the only thing this image does.
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    cd /src/cqlite/cqlite-datafusion \
 && rustup target add "${TRIPLE}" \
 && env ${LINKER_ENV} \
    maturin build --release --locked --compatibility linux --target "${TRIPLE}" \
      --out /wheels -m crates/cqlite-datafusion-py/Cargo.toml
DOCKERFILE

IMAGE="cqlite-wheel-build:${ARCH}"
echo
echo "== Building"
SECONDS=0
podman build --platform "linux/${HOST_ARCH}" \
  --build-arg "BASE_IMAGE=${BASE_IMAGE}" \
  --build-arg "RUST_VERSION=${RUST_VERSION}" \
  --build-arg "CROSS_PKG=${CROSS_PKG}" \
  --build-arg "LINKER_ENV=${LINKER_ENV}" \
  --build-arg "TRIPLE=${TRIPLE}" \
  -t "${IMAGE}" "${STAGE}"
echo "   built in ${SECONDS}s"

mkdir -p "${DIST}"
CID="$(podman create --platform "linux/${HOST_ARCH}" "${IMAGE}" true)"
podman cp "${CID}:/wheels/${WHEEL}" "${DIST}/${WHEEL}"
podman rm -f "${CID}" > /dev/null

# The checksum file's shape follows cassandra/dist/: `<sha>  <bare filename>`, which is
# what ../Dockerfile checks with a plain `sha256sum -c`.
(cd "${DIST}" && sha256sum "${WHEEL}" > "${WHEEL}.sha256")
printf '   %s  %s bytes\n' "${WHEEL}" "$(wc -c < "${DIST}/${WHEEL}" | tr -d ' ')"
sed 's/^/   /' "${DIST}/${WHEEL}.sha256"

echo
echo "== Verifying the wheel loads in a clean linux/${ARCH} ${BASE_IMAGE##*/}"
# Pull first, and retry.  Docker Hub answered "unable to retrieve auth token: invalid
# username/password" twice while this script was being written, and succeeded on the next
# attempt; a build that has just spent ten minutes should not be thrown away for that.
for attempt in 1 2 3; do
  podman pull -q --platform "linux/${ARCH}" "${BASE_IMAGE}" > /dev/null 2>&1 && break
  echo "   pull attempt ${attempt} failed; retrying"
  sleep 20
done

# Importing is the whole check: it dlopens the abi3 library, which is what a wrong glibc,
# a wrong architecture or a bad cross link fails on.  datafusion is not installed here on
# purpose, so a failure is the library's and not a dependency's.  Emulated CPython is
# reliable where emulated rustc is not, so this runs on the target platform even when the
# build did not.
podman run --rm --platform "linux/${ARCH}" --network none \
  -v "${DIST}:/dist:ro" "${BASE_IMAGE}" \
  sh -c "pip install --no-cache-dir --no-index /dist/${WHEEL} > /dev/null \
      && python -c 'from cqlite_datafusion import SSTableProvider; print(\"   import ok:\", SSTableProvider)'"

# A `--platform` run or build leaves the local base-image tag pointing at that platform,
# and `podman compose build backend` afterwards resolves the tag without asking for one.
# So a foreign-architecture run here would silently make the next backend build emulated.
# It happened: the only symptom was a two-minute pip install of x86_64 wheels on an arm64
# machine.  Put the tag back.
if [[ "${ARCH}" != "${HOST_ARCH}" ]]; then
  echo
  echo "== Restoring the local ${BASE_IMAGE##*/} tag to linux/${HOST_ARCH}"
  podman pull -q --platform "linux/${HOST_ARCH}" "${BASE_IMAGE}" > /dev/null
fi

echo
echo "== Now record ${COMMIT} in backend/dist/VENDOR.md and commit backend/dist/."
echo "   .gitattributes tracks *.whl in Git LFS; check with: git lfs ls-files | grep whl"
