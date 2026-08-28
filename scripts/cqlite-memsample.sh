#!/usr/bin/env bash
# Run one cqlite statement and sample the backend's cgroup while it runs.
#
# memory.peak counts anonymous memory and page cache together, so on its own it
# cannot say whether a reader held the rows or the kernel merely kept the file
# pages it had read.  memory.stat separates them: `anon` is the reader's own
# memory and `file` is the SSTable pages charged to this container, which the
# kernel reclaims under pressure.  Both matter, for different reasons, and the
# limit applies to their sum.
#
# A sample costs two `podman exec` calls, so a long statement wants a wide
# interval: sampling the whole-table walk every 5 s stretched it from 539 s to
# 753 s, and a wall clock is therefore taken from an unsampled run.
#
#   scripts/cqlite-memsample.sh <label> "<sql>" [interval_s]
#
# Recreate the backend first, so the figures belong to this statement:
# memory.peak is a high-water mark since the container started, and the page
# cache a previous read charged stays charged until the container goes.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p .ci-tmp
LABEL="$1"
SQL="$2"
INTERVAL="${3:-5}"
# The same default as podman-compose.yml's backend mapping, and the same
# variable, so a stack brought up with BACKEND_PORT set is still reachable here.
BACKEND_PORT="${BACKEND_PORT:-8000}"

field() { podman exec backend awk -v k="$1" '$1==k{print $2}' /sys/fs/cgroup/memory.stat; }
gb() { python3 -c "import sys; print(f'{int(sys.argv[1])/1e9:.2f}')" "$1"; }

jq -n --arg sql "$SQL" \
  '{sql: $sql, limit: 5, engines: ["cqlite"], mode: "sequential", reuse_snapshot: false}' \
  > ".ci-tmp/mem-$LABEL-req.json"

curl -s -m 3600 -X POST "localhost:$BACKEND_PORT/api/query/benchmark" \
  -H 'Content-Type: application/json' -d "@.ci-tmp/mem-$LABEL-req.json" \
  > ".ci-tmp/mem-$LABEL.json" &
CURL=$!

MAXA=0
MAXF=0
while kill -0 "$CURL" 2>/dev/null; do
  A=$(field anon || echo 0)
  F=$(field file || echo 0)
  if [ "${A:-0}" -gt "$MAXA" ]; then MAXA="$A"; fi
  if [ "${F:-0}" -gt "$MAXF" ]; then MAXF="$F"; fi
  sleep "$INTERVAL"
done
wait "$CURL" || true

echo "$LABEL: anon_peak=$(gb "$MAXA") GB  file_peak=$(gb "$MAXF") GB  cgroup_peak=$(gb "$(podman exec backend cat /sys/fs/cgroup/memory.peak)") GB"
jq -r '.cqlite | "  rows=\(.rows) ms=\(.query_time_ms) files=\(.sstable_files) bytes=\(.sstable_bytes) open_ms=\(.reader_open_ms) age_s=\(.data_age_s) err=\(.error)"' \
  ".ci-tmp/mem-$LABEL.json"
