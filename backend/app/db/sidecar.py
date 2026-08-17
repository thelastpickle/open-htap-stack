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

# Short: this runs while a query is being set up, so a Sidecar that will not answer
# should cost the query nothing beyond a missing figure.
TIMEOUT_S = 5.0


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
