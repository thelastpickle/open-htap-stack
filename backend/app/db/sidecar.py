"""The Cassandra Sidecar's HTTP API — what a bulk read is about to read.

The bulk reader streams SSTable files from the Sidecar, so the volume of a read is
knowable exactly: it is the size of the snapshot the reader just took.  Reporting it
is what tells a slow read that is simply large from one that has gone wrong, and on
a demo whose table grows by tens of megabytes a minute that distinction is most of
the question.

Only the Sidecar can answer it.  Cassandra's own table size is close but not the
same figure, and Spark's metrics describe the job rather than the data.
"""
from typing import Optional

import httpx

from app.config import settings

# This runs while a query is being set up, so a Sidecar that will not answer should cost
# the query little beyond a missing figure; but 5s was inside the range the Sidecar
# actually takes, and losing the figure is what CI asserts against.  Measured from one
# CI runner's Sidecar access log, listing the files of a three-SSTable snapshot of
# demo.drone_latest_status: the request logged at 15:52:40 answered 200 at 15:52:43.751,
# the one at 15:54:50 at 15:54:54.203, and the one at 15:55:35 at 15:55:41.117.  The
# third is 6.1s, so httpx gave up at 5s, snapshot_bytes came back null, and the dashboard
# step failed with "spark_bulk answered without saying how many bytes it scanned" on a
# read that had otherwise succeeded and returned its 5 rows.
#
# 15s is 2.5x the worst of those three.  It is a bound rather than a generous value
# because this call sits inside the read that query_time_ms times, so whatever it waits
# is added to a reported figure; a Sidecar slow enough to reach it is one whose read
# will be slow anyway, and a read reporting no volume cannot be attributed at all.
TIMEOUT_S = 15.0


def snapshot_bytes(table: str, snapshot: str) -> Optional[int]:
    """Total size of every file in one snapshot, or None if it cannot be read.

    Every component counts, not only Data.db: the reader opens the index and filter
    files as well, and they are part of what the Sidecar has to stream.

    Returns None rather than raising.  This is a figure to report beside a result,
    so failing to get it must not fail the read that is about to happen.
    """
    url = (
        f"http://{settings.cassandra_host}:{settings.sidecar_port}"
        f"/api/v1/keyspaces/{settings.cassandra_keyspace}/tables/{table}/snapshots/{snapshot}"
    )
    try:
        response = httpx.get(url, timeout=TIMEOUT_S)
        response.raise_for_status()
        files = response.json().get("snapshotFilesInfo") or []
    except Exception as e:
        print(f"[db] could not size snapshot {snapshot}: {e}")
        return None
    return sum(int(f.get("size") or 0) for f in files)
