# The build context is the repository root, not backend/, because the image also
# builds the cqlite reader from cqlite-datafusion/; see the backend service in
# podman-compose.yml and the ignore rules in .dockerignore.

# ---------------------------------------------------------------- the reader --
# Built on the same base as the runtime stage below, so the shared library it
# produces links against the glibc that will load it.  A `rust:*-slim` builder
# would be one Debian suite away from a different one, and the failure would
# appear as an import error in the runtime image rather than here.
FROM python:3.14-slim AS reader

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH

# DataFusion 54 needs 1.88 or newer; this is the version the crate is developed
# against.  maturin is bounded rather than pinned, because the build backend
# requirement in crates/cqlite-datafusion-py/pyproject.toml already bounds it and
# two pins that could disagree are worse than one.
ARG RUST_VERSION=1.98.0

RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential curl \
 && rm -rf /var/lib/apt/lists/* \
 && curl -sSf https://sh.rustup.rs \
    | sh -s -- -y --no-modify-path --profile minimal \
      --default-toolchain "${RUST_VERSION}" \
 && pip install --no-cache-dir "maturin>=1.7,<2.0"

WORKDIR /src
COPY cqlite-datafusion/ ./cqlite-datafusion/

# --locked, so the build fails rather than resolving a different DataFusion than
# Cargo.lock records: the capsule the reader hands the datafusion wheel is a
# repr(C) struct, and both sides must agree on its layout.
#
# The cache mounts are what make a rebuild after a Rust change minutes rather
# than a quarter of an hour; they hold the registry and the target directory
# between builds and reach no further than this stage.
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/src/cqlite-datafusion/target,sharing=locked \
    maturin build --release --locked --out /wheels \
      -m cqlite-datafusion/crates/cqlite-datafusion-py/Cargo.toml

# --------------------------------------------------------------- the backend --
FROM python:3.14-slim

WORKDIR /app

COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# One abi3 shared library in a wheel.  No Rust toolchain reaches this image, and
# nothing in it compiles at start-up.
COPY --from=reader /wheels/*.whl /tmp/wheels/
RUN pip install --no-cache-dir /tmp/wheels/*.whl && rm -rf /tmp/wheels

COPY backend/app/ ./app/

EXPOSE 8000

# Single worker: the engine clients hold one connection each and keep their
# in-memory demo settings, which several workers would not share.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
