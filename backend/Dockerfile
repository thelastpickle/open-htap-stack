# Nothing in this image compiles.  The cqlite reader arrives as one prebuilt abi3
# wheel from dist/, whose VENDOR.md names the fork commit it was built from and the
# script that rebuilds it.
FROM python:3.14-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ARG WHEEL_VERSION=0.1.0
# The checksum is verified even though the file is copied rather than fetched: the wheel
# is stored in Git LFS, and a checkout whose LFS objects did not arrive leaves a 130-byte
# pointer file in its place.  Failing here beats an image that starts and then declines
# every cqlite query with an import error.
#
# `uname -m` picks the wheel, so no TARGETARCH mapping is needed: abi3-py310 makes one
# wheel serve CPython 3.10 and later, but its shared library is still built for one
# architecture and one glibc.  dist/ holds x86_64 for CI and aarch64 for a development
# machine, and a platform with neither fails on the missing file.
COPY dist/ /tmp/wheels/
RUN cd /tmp/wheels \
 && WHEEL="cqlite_datafusion-${WHEEL_VERSION}-cp310-abi3-linux_$(uname -m).whl" \
 && if [ ! -f "${WHEEL}" ]; then \
      echo "No wheel for $(uname -m).  dist/ holds:" >&2; ls >&2; \
      echo "Build one: scripts/build-cqlite-wheel.sh <fork-commit> <amd64|arm64>" >&2; \
      echo "If the file is there but small, its Git LFS object did not arrive." >&2; \
      exit 1; \
    fi \
 && sha256sum -c "${WHEEL}.sha256" \
 && pip install --no-cache-dir "${WHEEL}" \
 && cd / && rm -rf /tmp/wheels

COPY app/ ./app/

EXPOSE 8000

# Single worker: the engine clients hold one connection each and keep their
# in-memory demo settings, which several workers would not share.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
