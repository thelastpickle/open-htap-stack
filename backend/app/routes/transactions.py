"""Accord transactions — multi-partition conditional writes, and their references.

Two demonstrations live here, on two schemas, because they make two different
claims.  The session one is exactly-once in-order delivery.  The airspace
clearance one is a distributed semaphore: a fixed number of drones may hold a
clearance into a restricted zone at once, and the count and the two indexes of who
holds what have to agree after every grant and every release.

What both show is the one thing the five access paths cannot: a write that is
conditional on rows in *other* partitions.  A CQL batch is atomic but not
conditional across partitions, and a lightweight transaction conditions on one
partition only, so neither can express "append this event to the session's
timeline, but only if the session is open, this sequence number has not already
been applied, and the one before it has".  Those three facts live in three tables
with three different partition keys, and Accord reads all three and writes two in
a single strictly-serializable transaction.

The session demonstration is exactly-once and in-order delivery, which is the shape
of almost every stream-to-projection problem: a replayed event must not duplicate a
row, and an event that arrives early must not leave a gap.  The clearance one is the
shape of every admission-control problem: a count that must never be oversubscribed,
however many callers ask at once.  Each has a scripted sequence that drives the
refusals as well as the successes, and shows the state unchanged after each refusal.

Kept apart from query.py because that module rejects every write keyword, by
design: its ``_validate`` is what keeps the read console honest.  A transaction is
a write, so it needs its own route rather than a hole in that check.
"""
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from cassandra.util import uuid_from_time
from fastapi import APIRouter, HTTPException, Query

from app.config import settings
from app.db.cassandra_client import cassandra_client
from app.models import (
    ClearanceContentionResult,
    ClearanceDemoResult,
    ClearanceState,
    ClearanceZone,
    TransactionDemoResult,
    TransactionStep,
    TransactionTimelineRow,
)
from app.routes.query import BASELINE_WINDOW_S, _OltpProbe, _probe_subject

router = APIRouter(prefix="/api/transactions", tags=["transactions"])

# The demo writes into its own session, so nothing it does can collide with
# another run or with the sink.  A caller may name a session to step through one
# by hand from the UI.
DEMO_USER_PREFIX = "txn-demo"

# How many times an applied transaction is repeated when a p50 is asked for.  Small
# because each repeat is a real consensus round trip on a real node, and the figure
# wanted here is an order of magnitude rather than a benchmark.
DEFAULT_REPEATS = 20

# One transaction demo at a time.  Two overlapping runs would each be timed while
# the other ran, which is the same reason query.py holds a lock for comparisons.
_lock = threading.Lock()


def _keyspace() -> str:
    return settings.cassandra_keyspace


# ──────────────────────── The statements ────────────────────────
#
# Every timeuuid and timestamp below is bound by this module, never generated in
# the statement.  An Accord transaction must be deterministic, so now() and
# toTimestamp(now()) are out: each would be evaluated per replica, and the same
# transaction would then write different values depending on who executed it.


def _apply_cql(keyspace: str, seq: int) -> str:
    """The transaction that appends one event to a session's timeline.

    Three reads guard two writes.  ``prev_ok`` is omitted for seq=0, which has no
    predecessor; including it would make the first event of every session
    unappendable.
    """
    guards = [
        f"LET session_ok = (SELECT session_id FROM {keyspace}.sessions_open "
        "WHERE user_id = ? AND session_id = ?);",
        f"LET already = (SELECT seq FROM {keyspace}.session_seq_applied "
        "WHERE user_id = ? AND session_id = ? AND seq = ?);",
    ]
    projection = "SELECT session_ok.session_id, already.seq"
    condition = "session_ok IS NOT NULL AND already IS NULL"
    if seq > 0:
        guards.append(
            f"LET prev_ok = (SELECT seq FROM {keyspace}.session_seq_applied "
            "WHERE user_id = ? AND session_id = ? AND seq = ?);"
        )
        projection = "SELECT session_ok.session_id, already.seq, prev_ok.seq"
        condition += " AND prev_ok IS NOT NULL"
    return (
        "BEGIN TRANSACTION\n  "
        + "\n  ".join(guards)
        + f"\n  {projection};"
        + f"\n  IF {condition} THEN"
        + f"\n    INSERT INTO {keyspace}.session_timeline "
        "(user_id, session_id, seq, event_id, event_time, event_type, payload) "
        "VALUES (?, ?, ?, ?, ?, ?, ?);"
        + f"\n    INSERT INTO {keyspace}.session_seq_applied (user_id, session_id, seq) "
        "VALUES (?, ?, ?);"
        + "\n  END IF\nCOMMIT TRANSACTION;"
    )


def _apply_params(
    user_id: str, session_id: uuid.UUID, seq: int, event_type: str, payload: str
) -> Tuple[Any, ...]:
    """Bind values for _apply_cql, in the order the statement reads them."""
    event_id = uuid_from_time(datetime.now(timezone.utc))
    event_time = datetime.now(timezone.utc)
    params: List[Any] = [user_id, session_id, user_id, session_id, seq]
    if seq > 0:
        params += [user_id, session_id, seq - 1]
    params += [user_id, session_id, seq, event_id, event_time, event_type, payload]
    params += [user_id, session_id, seq]
    return tuple(params)


def _reason(seq: int, projection: Dict[str, Any]) -> str:
    """Why the IF did not fire, read out of the guards the transaction projected.

    An Accord transaction returns no [applied] column, so this is the only way to
    tell one refusal from another.  The order matters: a caller wants to hear
    "already applied" rather than "no predecessor" when both are true.
    """
    if not projection:
        return "the transaction projected nothing, so its guards cannot be read"
    if projection.get("session_ok.session_id") is None:
        return "the session is not open"
    if projection.get("already.seq") is not None:
        return f"seq={seq} was already applied, so the replay changed nothing"
    if seq > 0 and projection.get("prev_ok.seq") is None:
        return f"seq={seq - 1} has not been applied, so seq={seq} would leave a gap"
    return ""


def _timeline_rows(keyspace: str, user_id: str, session_id: uuid.UUID) -> List[Dict[str, Any]]:
    """The projection itself: one bounded single-partition read, at QUORUM.

    QUORUM because transactional_mode='full' routes a table's ordinary reads through
    Accord as well as its writes, and Accord refuses the driver's default:
    "ConsistencyLevel LOCAL_ONE is unsupported with Accord for read, supported are
    [ONE, QUORUM, ALL, SERIAL]".  Worth knowing before opting any table in: had
    demo.events taken transactional_mode='full', every read path on the dashboard
    would have started failing this way rather than merely slowing down.
    """
    return cassandra_client.execute_query(
        SimpleStatement(
            f"SELECT seq, event_id, event_time, event_type, payload "
            f"FROM {keyspace}.session_timeline WHERE user_id = %s AND session_id = %s",
            consistency_level=ConsistencyLevel.QUORUM,
        ),
        (user_id, session_id),
    )


def _open_session(keyspace: str, user_id: str, session_id: uuid.UUID) -> None:
    cassandra_client.execute_write(
        f"INSERT INTO {keyspace}.sessions_open (user_id, session_id) VALUES (%s, %s)",
        (user_id, session_id),
    )


def _run_step(
    keyspace: str,
    action: str,
    user_id: str,
    session_id: uuid.UUID,
    seq: int,
    event_type: str = "session.step",
    payload: str = "{}",
) -> TransactionStep:
    """Run one transaction, and report what it did without asking twice."""
    cql = _apply_cql(keyspace, seq)
    params = _apply_params(user_id, session_id, seq, event_type, payload)
    # The driver's positional placeholder for a prepared statement is ?, and the
    # simple-statement one is %s.  A transaction is prepared, so the statement is
    # written with ? and the driver binds it.
    start = time.perf_counter()
    try:
        projection = cassandra_client.execute_transaction(cql, params)
    except Exception as exc:
        return TransactionStep(
            action=action,
            cql=cql,
            duration_ms=round((time.perf_counter() - start) * 1000, 2),
            error=str(exc),
            timeline_rows=len(_timeline_rows(keyspace, user_id, session_id)),
        )
    duration_ms = round((time.perf_counter() - start) * 1000, 2)
    reason = _reason(seq, projection)
    rows = _timeline_rows(keyspace, user_id, session_id)
    return TransactionStep(
        action=action,
        cql=cql,
        applied=not reason,
        reason=reason,
        projection={k: str(v) if v is not None else None for k, v in projection.items()},
        duration_ms=duration_ms,
        timeline_rows=len(rows),
    )


def _reference_ms(
    keyspace: str, user_id: str, session_id: uuid.UUID, repeats: int
) -> Dict[str, float]:
    """Time the same row written two other ways, on a non-transactional twin table.

    Same columns, same key and the same QUORUM as the transaction, so what is
    compared is the write path and not two table definitions or two consistency
    levels.  Repeated the same number of times as the transaction and reported as a
    p50, because a single sample against a forty-run p50 is not a comparison.

    A distinct sequence number per repeat, so the lightweight transaction's IF NOT
    EXISTS finds nothing and takes its applied path every time; reusing one key
    would make every repeat after the first a rejection, which is a different and
    cheaper operation.

    Nothing is measured at all when the caller asks for no repeats, which the CI
    step does.  A single sample reported under a p50 label, beside a transaction
    p50 that is null for the same reason, would be a comparison against nothing.
    """
    columns = "(user_id, session_id, seq, event_id, event_time, event_type, payload)"
    out: Dict[str, float] = {}
    if repeats <= 0:
        return out
    for label, suffix, base in (
        ("plain_insert_p50_ms", "", 100_000),
        ("lwt_if_not_exists_p50_ms", " IF NOT EXISTS", 200_000),
    ):
        samples: List[float] = []
        for index in range(repeats):
            start = time.perf_counter()
            try:
                cassandra_client.execute_write(
                    f"INSERT INTO {keyspace}.session_timeline_plain {columns} "
                    f"VALUES (%s, %s, %s, %s, %s, %s, %s){suffix}",
                    (
                        user_id,
                        session_id,
                        base + index,
                        uuid_from_time(datetime.now(timezone.utc)),
                        datetime.now(timezone.utc),
                        "reference",
                        "{}",
                    ),
                )
                samples.append((time.perf_counter() - start) * 1000)
            except Exception:
                # A reference that failed is left out rather than reported as zero: a
                # zero here would read as "faster than everything", which is the one
                # thing it cannot mean.
                pass
        if samples:
            out[label] = _percentile(samples, 0.5)
            out[label.replace("_p50_ms", "_max_ms")] = round(max(samples), 2)
    return out


def _percentile(values: List[float], fraction: float) -> float:
    ordered = sorted(values)
    index = min(len(ordered) - 1, int(round(fraction * (len(ordered) - 1))))
    return round(ordered[index], 2)


# ──────────────────────── Routes ────────────────────────


@router.get("/session/schema")
def transaction_schema() -> Dict[str, Any]:
    """Whether each table will accept a transaction, tested rather than looked up.

    There is no schema column to read.  ``transactional_mode`` does not appear in
    system_schema.tables on 6.0-alpha2, and nothing else there distinguishes a
    transactional table from a plain one: session_timeline and its non-transactional
    twin have identical flags, extensions and fast_path.  Only DESCRIBE TABLE shows
    the option, and DESCRIBE is not something to parse from a driver.

    So this asks the node the question directly, with a transaction that reads one
    row of each table and writes nothing, and reports the node's own answer.  That
    is the better test in any case: what a page needs to know is whether a
    transaction will run, and a table can be transactional and still refuse one
    while its migration is incomplete.
    """
    keyspace = _keyspace()
    probe_user, probe_session = "__schema_probe__", uuid.UUID(int=0)
    # A transaction must project a named column, not a whole LET reference: "SELECT
    # probe" is refused with "SELECT references must specify a column."  So each
    # probe names one.
    reads = {
        "sessions_open": (
            f"(SELECT session_id FROM {keyspace}.sessions_open "
            "WHERE user_id = ? AND session_id = ?)",
            "session_id",
        ),
        "session_seq_applied": (
            f"(SELECT seq FROM {keyspace}.session_seq_applied "
            "WHERE user_id = ? AND session_id = ? AND seq = 0)",
            "seq",
        ),
        "session_timeline": (
            f"(SELECT seq FROM {keyspace}.session_timeline "
            "WHERE user_id = ? AND session_id = ? AND seq = 0)",
            "seq",
        ),
    }
    status: Dict[str, str] = {}
    for table, (read, column) in reads.items():
        cql = (
            f"BEGIN TRANSACTION\n  LET probe = {read};\n  SELECT probe.{column};"
            "\nCOMMIT TRANSACTION;"
        )
        try:
            cassandra_client.execute_transaction(cql, (probe_user, probe_session))
            status[table] = "accepts transactions"
        except Exception as exc:
            status[table] = str(exc)
    ready = all(value == "accepts transactions" for value in status.values())
    return {
        "keyspace": keyspace,
        "tables": status,
        "ready": ready,
        "note": (
            ""
            if ready
            else "These tables must be created WITH transactional_mode='full', and that "
            "needs accord.enabled on the node.  An existing data directory cannot be "
            "altered into it on a single node: the ALTER starts a migration that only a "
            "repair completes, and at replication factor 1 nodetool repair declines with "
            "\"No repair is needed\".  Wipe with ./stop-and-clean-data-and-schema.sh and "
            "start again with CASSANDRA_ACCORD_ENABLED=true."
        ),
    }


@router.get("/session/{user_id}/{session_id}")
def session_timeline(user_id: str, session_id: str) -> Dict[str, Any]:
    """The projection for one session, as the transactions left it."""
    try:
        parsed = uuid.UUID(session_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="session_id is not a UUID")
    try:
        rows = _timeline_rows(_keyspace(), user_id, parsed)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    return {
        "user_id": user_id,
        "session_id": session_id,
        "timeline": [
            TransactionTimelineRow(
                seq=int(row["seq"]),
                event_id=str(row["event_id"]),
                event_time=str(row["event_time"]),
                event_type=str(row["event_type"]),
                payload=str(row["payload"]),
            )
            for row in rows
        ],
    }


@router.post("/session/step")
def session_step(
    user_id: str = Query(..., description="The session's user, which is its partition key"),
    session_id: str = Query(..., description="The session, created by /session/open"),
    seq: int = Query(..., ge=0, description="The sequence number to attempt"),
    event_type: str = Query("session.step"),
) -> TransactionStep:
    """Attempt one sequence number, so the UI can drive the demo a step at a time."""
    try:
        parsed = uuid.UUID(session_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="session_id is not a UUID")
    if not _lock.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="a transaction demo is already running")
    try:
        return _run_step(_keyspace(), f"apply seq={seq}", user_id, parsed, seq, event_type)
    finally:
        _lock.release()


@router.post("/session/open")
def session_open(user_id: Optional[str] = Query(None)) -> Dict[str, str]:
    """Open a session, which is the guard every step below reads first."""
    keyspace = _keyspace()
    session_id = uuid.uuid4()
    resolved = user_id or f"{DEMO_USER_PREFIX}-{session_id.hex[:8]}"
    try:
        _open_session(keyspace, resolved, session_id)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    return {"user_id": resolved, "session_id": str(session_id)}


@router.post("/session/demo")
def session_demo(
    repeats: int = Query(
        DEFAULT_REPEATS,
        ge=0,
        # High enough to make the OLTP probe say something.  The probe reads once
        # every PROBE_INTERVAL_S, so a run of 40 transactions at a few milliseconds
        # each is over before it has taken three samples; roughly 200 repeats per
        # second of run means about 2,000 for a ten-second window.  The default is
        # deliberately not that: the default run answers "what does one cost", and a
        # caller who wants the probe's answer asks for it.
        le=4000,
    ),
    probe: bool = Query(True, description="Point-read one asset while the transactions run"),
) -> TransactionDemoResult:
    """The whole scripted sequence, in one call, on a session of its own.

    Six steps in this order, because the order is the argument: apply seq=0, replay
    it, attempt seq=2 before seq=1, apply seq=1, then apply seq=2.  Steps two and
    three must leave the timeline exactly as they found it, and step five must
    succeed only once its predecessor exists.
    """
    if not _lock.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="a transaction demo is already running")
    keyspace = _keyspace()
    try:
        session_id = uuid.uuid4()
        user_id = f"{DEMO_USER_PREFIX}-{session_id.hex[:8]}"
        subject = _probe_subject() if probe else None

        def sequence() -> List[TransactionStep]:
            steps: List[TransactionStep] = []
            start = time.perf_counter()
            _open_session(keyspace, user_id, session_id)
            steps.append(
                TransactionStep(
                    action="open the session",
                    # A plain INSERT, and it still goes through Accord: the table is
                    # transactional_mode='full', which routes every write and not only
                    # a BEGIN TRANSACTION.  Timed so the reader can see what that
                    # costs beside the plain_insert reference below, which writes the
                    # same shape into a table that is not transactional.
                    cql=f"INSERT INTO {keyspace}.sessions_open (user_id, session_id) VALUES (%s, %s)",
                    applied=True,
                    duration_ms=round((time.perf_counter() - start) * 1000, 2),
                    timeline_rows=0,
                )
            )
            steps.append(_run_step(keyspace, "apply seq=0", user_id, session_id, 0))
            steps.append(_run_step(keyspace, "replay seq=0", user_id, session_id, 0))
            steps.append(_run_step(keyspace, "attempt seq=2 out of order", user_id, session_id, 2))
            steps.append(_run_step(keyspace, "apply seq=1", user_id, session_id, 1))
            steps.append(_run_step(keyspace, "apply seq=2", user_id, session_id, 2))
            return steps

        baseline: Dict[str, Any] = {}
        if subject:
            # The same idle window the comparison page takes, and for the same
            # reason: a probe figure on its own says nothing, because it is the
            # difference from idle that shows what the work cost the request path.
            with _OltpProbe(subject) as idle:
                time.sleep(BASELINE_WINDOW_S)
            baseline = idle.impact().model_dump()
            with _OltpProbe(subject) as oltp:
                steps = sequence()
                applied_ms = _repeat_applied(keyspace, repeats)
            impact = oltp.impact().model_dump()
            impact["entity_id"] = subject
        else:
            steps = sequence()
            applied_ms = _repeat_applied(keyspace, repeats)
            impact = {}

        rows = _timeline_rows(keyspace, user_id, session_id)
        return TransactionDemoResult(
            user_id=user_id,
            session_id=str(session_id),
            steps=steps,
            timeline=[
                TransactionTimelineRow(
                    seq=int(row["seq"]),
                    event_id=str(row["event_id"]),
                    event_time=str(row["event_time"]),
                    event_type=str(row["event_type"]),
                    payload=str(row["payload"]),
                )
                for row in rows
            ],
            reference_ms=_reference_ms(keyspace, user_id, session_id, repeats),
            repeats=len(applied_ms),
            applied_p50_ms=_percentile(applied_ms, 0.5) if applied_ms else None,
            applied_max_ms=round(max(applied_ms), 2) if applied_ms else None,
            oltp_probe=impact,
            oltp_baseline=baseline,
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    finally:
        _lock.release()


def _repeat_applied(keyspace: str, repeats: int) -> List[float]:
    """Latencies of transactions that all applied, for a p50 and a max.

    In a session of its own, not the one the six steps illustrate: putting forty
    repeats into that session would bury its three-row story under forty more rows
    and make the page's own point unreadable.

    Every repeat takes the three-guard path with a predecessor to find, because
    seq=0 has one guard fewer and timing it would flatter the figure.
    """
    if repeats <= 0:
        return []
    session_id = uuid.uuid4()
    user_id = f"{DEMO_USER_PREFIX}-measure-{session_id.hex[:8]}"
    _open_session(keyspace, user_id, session_id)
    _run_step(keyspace, "apply seq=0", user_id, session_id, 0)
    latencies: List[float] = []
    for seq in range(1, repeats + 1):
        step = _run_step(keyspace, f"apply seq={seq}", user_id, session_id, seq)
        if step.applied:
            latencies.append(step.duration_ms)
    return latencies


# ──────────────────── Airspace clearance: a distributed semaphore ────────────────────
#
# The second demonstration, on three tables of its own.  A restricted zone admits a
# fixed number of drones at once, and a clearance is recorded twice: once under the
# zone, so the tower can list who is inside, and once under the drone, so a grant can
# be refused without scanning every zone.  Those are three partition keys, and one
# grant has to move all three or none of them.
#
# What no other path here can express: a lightweight transaction conditions on one
# partition, and a CQL batch is atomic without being conditional, so neither can say
# "admit this drone only if the zone has a slot left and the drone holds no clearance
# already".  The invariant that check protects is capacity == remaining + holders, and
# the state route reports whether it still holds rather than assuming it.
#
# The zone is a real one from the map and the drones are real fleet assets, but the
# clearance itself is the demo's own: nothing in ingest reads these tables, and no
# drone's telemetry changes because a clearance was granted or refused.

# zone-oslo-airport has capacity 2 in the sink's seed, so two grants exhaust it and
# the third is refused for the reason the demo exists to show.  A larger zone would
# need more steps to reach the same point.
DEMO_ZONE = "zone-oslo-airport"

# Three drones the map is already showing.  The first two fill the zone; the third
# finds it full.
DEMO_ASSETS = ("asset-000000", "asset-000001", "asset-000002")

# A second zone, for the step that shows one drone cannot hold two clearances.
DEMO_OTHER_ZONE = "zone-royal-palace"

# Where the repeated grant/release pairs are timed.  Fornebu, the widest zone in the
# seed, so a repeat cannot exhaust it even if one release were refused; and neither of
# the two zones the scripted steps touch, because the state the page shows must be what
# those steps left rather than what a measurement loop did afterwards.
DEMO_MEASURE_ZONE = "zone-fornebu"


def _grant_cql(keyspace: str) -> str:
    """Grant a clearance: two reads guard a decrement and two inserts.

    ``remaining > 0`` and not ``granted < capacity`` because Accord will not compare
    two LET references to each other: ``IF occ.granted < occ.capacity`` is refused
    with a SyntaxException.  Counting down needs only a reference and a literal, and
    ``SET remaining -= 1`` is then the whole read-modify-write, done inside consensus
    rather than by reading a value and writing it back.
    """
    return (
        "BEGIN TRANSACTION\n"
        f"  LET occ = (SELECT capacity, remaining FROM {keyspace}.zone_occupancy "
        "WHERE zone_id = ?);\n"
        f"  LET held = (SELECT zone_id FROM {keyspace}.drone_clearance "
        "WHERE entity_id = ?);\n"
        "  SELECT occ.remaining, occ.capacity, held.zone_id;\n"
        "  IF occ.remaining IS NOT NULL AND occ.remaining > 0 AND held.zone_id IS NULL THEN\n"
        f"    UPDATE {keyspace}.zone_occupancy SET remaining -= 1 WHERE zone_id = ?;\n"
        f"    INSERT INTO {keyspace}.zone_clearance (zone_id, entity_id, granted_at) "
        "VALUES (?, ?, ?);\n"
        f"    INSERT INTO {keyspace}.drone_clearance (entity_id, zone_id, granted_at) "
        "VALUES (?, ?, ?);\n"
        "  END IF\n"
        "COMMIT TRANSACTION;"
    )


def _release_cql(keyspace: str) -> str:
    """Release a clearance: one read guards an increment and two deletes.

    The caller names the zone rather than the transaction reading it out of ``held``,
    because a LET reference cannot appear in a write's WHERE clause.  Naming it costs
    nothing and buys the guard: ``held.zone_id = ?`` refuses a release of a clearance
    the drone does not hold, and refuses a second release of one already given back,
    since a null compares equal to nothing.
    """
    return (
        "BEGIN TRANSACTION\n"
        f"  LET held = (SELECT zone_id FROM {keyspace}.drone_clearance "
        "WHERE entity_id = ?);\n"
        "  SELECT held.zone_id;\n"
        "  IF held.zone_id = ? THEN\n"
        f"    UPDATE {keyspace}.zone_occupancy SET remaining += 1 WHERE zone_id = ?;\n"
        f"    DELETE FROM {keyspace}.zone_clearance WHERE zone_id = ? AND entity_id = ?;\n"
        f"    DELETE FROM {keyspace}.drone_clearance WHERE entity_id = ?;\n"
        "  END IF\n"
        "COMMIT TRANSACTION;"
    )


def _grant_reason(zone_id: str, entity_id: str, projection: Dict[str, Any]) -> str:
    """Why a grant did not fire, read out of the guards it projected.

    Ordered by what a caller most wants to hear.  An unknown zone leaves nothing else
    to say; a drone that already holds a clearance is a more useful answer than a
    full zone, because it is true of that drone wherever it asks.
    """
    if not projection:
        return "the transaction projected nothing, so its guards cannot be read"
    if projection.get("occ.remaining") is None:
        return f"{zone_id} has no occupancy row, so there is no capacity to draw on"
    held = projection.get("held.zone_id")
    if held is not None:
        return f"{entity_id} already holds a clearance into {held}"
    if int(projection["occ.remaining"]) <= 0:
        capacity = projection.get("occ.capacity")
        return f"{zone_id} is full: all {capacity} slots are held"
    return ""


def _release_reason(zone_id: str, entity_id: str, projection: Dict[str, Any]) -> str:
    if not projection:
        return "the transaction projected nothing, so its guards cannot be read"
    held = projection.get("held.zone_id")
    if held is None:
        return f"{entity_id} holds no clearance, so there is nothing to give back"
    if str(held) != zone_id:
        return f"{entity_id} holds a clearance into {held}, not into {zone_id}"
    return ""


def _clearance_state(keyspace: str) -> ClearanceState:
    """The ledger, read from both sides, and whether the two agree.

    Three reads plus one per holder, all of them bounded: zone_occupancy holds one row
    per zone and zone_clearance is read one partition at a time, capacity rows at
    most.  The mirror check is a point read per holder rather than a scan of
    drone_clearance, which has no partition to bound it.

    It checks one direction only.  A drone_clearance row whose zone-side twin is
    missing would not be found here, and finding it would cost the scan this read
    exists to avoid; the state route says so rather than implying a full audit.
    """
    zones: List[ClearanceZone] = []
    mismatched: List[str] = []
    occupancy = cassandra_client.execute_query(
        SimpleStatement(
            f"SELECT zone_id, zone_name, severity, capacity, remaining "
            f"FROM {keyspace}.zone_occupancy",
            consistency_level=ConsistencyLevel.QUORUM,
        )
    )
    for row in occupancy:
        zone_id = str(row["zone_id"])
        holders = [
            str(held["entity_id"])
            for held in cassandra_client.execute_query(
                SimpleStatement(
                    f"SELECT entity_id FROM {keyspace}.zone_clearance WHERE zone_id = %s",
                    consistency_level=ConsistencyLevel.QUORUM,
                ),
                (zone_id,),
            )
        ]
        for entity_id in holders:
            mirror = cassandra_client.execute_query(
                SimpleStatement(
                    f"SELECT zone_id FROM {keyspace}.drone_clearance WHERE entity_id = %s",
                    consistency_level=ConsistencyLevel.QUORUM,
                ),
                (entity_id,),
            )
            if not mirror or str(mirror[0]["zone_id"]) != zone_id:
                mismatched.append(entity_id)
        capacity = int(row["capacity"] or 0)
        remaining = int(row["remaining"] or 0)
        zones.append(
            ClearanceZone(
                zone_id=zone_id,
                zone_name=str(row["zone_name"] or ""),
                severity=str(row["severity"] or ""),
                capacity=capacity,
                remaining=remaining,
                holders=sorted(holders),
                consistent=capacity == remaining + len(holders),
            )
        )
    zones.sort(key=lambda zone: zone.zone_id)
    return ClearanceState(zones=zones, mismatched=sorted(set(mismatched)))


def _run_grant(
    keyspace: str, action: str, zone_id: str, entity_id: str, with_state: bool = True
) -> TransactionStep:
    """One grant, timed, with the ledger as it stood after it."""
    cql = _grant_cql(keyspace)
    granted_at = datetime.now(timezone.utc)
    params = (
        zone_id,
        entity_id,
        zone_id,
        zone_id,
        entity_id,
        granted_at,
        entity_id,
        zone_id,
        granted_at,
    )
    return _clearance_step(
        keyspace, action, cql, params, zone_id, entity_id, _grant_reason, with_state
    )


def _run_release(
    keyspace: str, action: str, zone_id: str, entity_id: str, with_state: bool = True
) -> TransactionStep:
    """One release, timed, with the ledger as it stood after it."""
    cql = _release_cql(keyspace)
    params = (entity_id, zone_id, zone_id, zone_id, entity_id, entity_id)
    return _clearance_step(
        keyspace, action, cql, params, zone_id, entity_id, _release_reason, with_state
    )


def _clearance_step(
    keyspace: str,
    action: str,
    cql: str,
    params: Tuple[Any, ...],
    zone_id: str,
    entity_id: str,
    reason_of: Any,
    with_state: bool = True,
) -> TransactionStep:
    """Run one clearance transaction and report what it did to the zone.

    The step's ``state`` carries the zone's slots and holders afterwards, which is the
    clearance demo's counterpart of the session demo's timeline row count: a refused
    step is only convincing if the reader can see the number it did not move.  Reading
    it costs several bounded reads, so the measurement loop asks for none: it reports
    latencies and no state, and those reads would slow the loop without being reported.
    """
    start = time.perf_counter()
    try:
        projection = cassandra_client.execute_transaction(cql, params)
    except Exception as exc:
        return TransactionStep(
            action=action,
            cql=cql,
            duration_ms=round((time.perf_counter() - start) * 1000, 2),
            error=str(exc),
        )
    duration_ms = round((time.perf_counter() - start) * 1000, 2)
    # The reason is read from the raw projection, before the values are stringified
    # for the response: remaining is compared as a number, and "0" is truthy.
    reason = reason_of(zone_id, entity_id, projection)
    zone = None
    if with_state:
        zone = next((z for z in _clearance_state(keyspace).zones if z.zone_id == zone_id), None)
    return TransactionStep(
        action=action,
        cql=cql,
        applied=not reason,
        reason=reason,
        projection={k: str(v) if v is not None else None for k, v in projection.items()},
        duration_ms=duration_ms,
        state=zone.model_dump() if zone else {},
    )


def _clearance_reset(keyspace: str) -> List[str]:
    """Give every clearance back, so the demo starts from a full set of slots.

    An interrupted run would otherwise leave a zone permanently short, and the next
    run's first grant would fail for a reason that has nothing to do with what it
    means to show.  Each row is released by the same transaction a caller would use,
    rather than by writing capacity back over remaining: a reset that repaired a
    broken invariant by overwriting it would hide exactly the failure the state route
    reports.
    """
    actions: List[str] = []
    for zone in _clearance_state(keyspace).zones:
        for entity_id in zone.holders:
            step = _run_release(keyspace, f"release {entity_id}", zone.zone_id, entity_id)
            actions.append(
                f"released {entity_id} from {zone.zone_id}"
                if step.applied
                else f"could not release {entity_id} from {zone.zone_id}: "
                f"{step.reason or step.error}"
            )
    return actions


# ──────────────────────── Clearance routes ────────────────────────


@router.get("/clearance/state")
def clearance_state() -> ClearanceState:
    """The ledger now: slots left, who holds them, and whether the two sides agree."""
    try:
        return _clearance_state(_keyspace())
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc))


@router.post("/clearance/grant")
def clearance_grant(
    zone_id: str = Query(..., description="The restricted zone to be admitted to"),
    entity_id: str = Query(..., description="The drone asking, e.g. asset-000000"),
) -> TransactionStep:
    """Ask for one clearance, so the UI can drive the demo a step at a time."""
    if not _lock.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="a transaction demo is already running")
    try:
        return _run_grant(_keyspace(), f"grant {entity_id} into {zone_id}", zone_id, entity_id)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    finally:
        _lock.release()


@router.post("/clearance/release")
def clearance_release(
    zone_id: str = Query(..., description="The zone the clearance was granted for"),
    entity_id: str = Query(..., description="The drone giving it back"),
) -> TransactionStep:
    """Give one clearance back, which is the only way a slot returns."""
    if not _lock.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="a transaction demo is already running")
    try:
        return _run_release(_keyspace(), f"release {entity_id} from {zone_id}", zone_id, entity_id)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    finally:
        _lock.release()


@router.post("/clearance/reset")
def clearance_reset() -> Dict[str, Any]:
    """Release every clearance, so a run starts from a full set of slots."""
    if not _lock.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="a transaction demo is already running")
    try:
        actions = _clearance_reset(_keyspace())
        return {"actions": actions, "state": _clearance_state(_keyspace())}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    finally:
        _lock.release()


@router.post("/clearance/contend")
def clearance_contend(
    zone_id: str = Query(DEMO_ZONE, description="The zone every asker wants"),
    askers: int = Query(
        16,
        ge=2,
        # Bounded by the fleet, because each asker is a real drone the map is drawing
        # rather than an invented identifier.  100 is the demo's default fleet size.
        le=100,
        description="How many drones ask at once",
    ),
) -> ClearanceContentionResult:
    """Ask for one zone from many drones at once, and count how many got in.

    The demonstration that the seven scripted steps cannot make on their own: those
    run one after another, so nothing they show rules out a count read and written
    back outside consensus.  This overlaps the asks, and the answer has to be the
    zone's capacity exactly.

    Each ask is a real fleet asset, and every clearance is released first so the zone
    starts full.  Threads rather than tasks because the driver's session is
    thread-safe and each transaction blocks on consensus; the futures are all
    submitted before any is read, so the asks genuinely overlap.
    """
    if not _lock.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="a transaction demo is already running")
    keyspace = _keyspace()
    try:
        _clearance_reset(keyspace)
        before = next((z for z in _clearance_state(keyspace).zones if z.zone_id == zone_id), None)
        if before is None:
            raise HTTPException(status_code=404, detail=f"{zone_id} has no occupancy row")
        entity_ids = [f"asset-{index:06d}" for index in range(askers)]

        def ask(entity_id: str) -> TransactionStep:
            return _run_grant(keyspace, f"grant {entity_id}", zone_id, entity_id, False)

        start = time.perf_counter()
        with ThreadPoolExecutor(max_workers=askers) as pool:
            steps = list(pool.map(ask, entity_ids))
        duration_ms = round((time.perf_counter() - start) * 1000, 2)

        after = next((z for z in _clearance_state(keyspace).zones if z.zone_id == zone_id), None)
        errors = [f"{step.action}: {step.error}" for step in steps if step.error]
        return ClearanceContentionResult(
            zone_id=zone_id,
            capacity=before.capacity,
            askers=askers,
            granted=sum(1 for step in steps if step.applied),
            refused=sum(1 for step in steps if not step.applied and not step.error),
            winners=sorted(after.holders) if after else [],
            errors=errors,
            duration_ms=duration_ms,
            zone=after,
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    finally:
        _lock.release()


@router.post("/clearance/demo")
def clearance_demo(
    repeats: int = Query(
        DEFAULT_REPEATS,
        ge=0,
        le=4000,
        description="Grant/release pairs timed after the scripted steps, for a p50",
    ),
) -> ClearanceDemoResult:
    """The whole scripted sequence, in one call, on the airport zone.

    Seven steps, and the order is the argument.  Grant the first drone; replay that
    grant; ask for a second zone with one already held; grant the second drone, taking
    the last slot; ask for a third, into a zone now full; release the first; release it
    again.  Only three of the seven may change anything, and after all seven the
    zone's slots and its holders must still add up to its capacity.
    """
    if not _lock.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="a transaction demo is already running")
    keyspace = _keyspace()
    try:
        # Start from a full set of slots.  A previous run interrupted halfway would
        # otherwise leave the first grant refused for the wrong reason.
        _clearance_reset(keyspace)
        first, second, third = DEMO_ASSETS
        steps = [
            _run_grant(keyspace, f"grant {first} into {DEMO_ZONE}", DEMO_ZONE, first),
            _run_grant(keyspace, f"replay the grant of {first}", DEMO_ZONE, first),
            _run_grant(
                keyspace,
                f"grant {first} a second clearance, into {DEMO_OTHER_ZONE}",
                DEMO_OTHER_ZONE,
                first,
            ),
            _run_grant(keyspace, f"grant {second} the last slot", DEMO_ZONE, second),
            _run_grant(keyspace, f"grant {third} into a full zone", DEMO_ZONE, third),
            _run_release(keyspace, f"release {first}", DEMO_ZONE, first),
            _run_release(keyspace, f"release {first} again", DEMO_ZONE, first),
        ]
        grant_ms, release_ms = _repeat_clearance(keyspace, repeats)
        return ClearanceDemoResult(
            zone_id=DEMO_ZONE,
            entity_ids=list(DEMO_ASSETS),
            steps=steps,
            state=_clearance_state(keyspace),
            repeats=len(grant_ms),
            grant_p50_ms=_percentile(grant_ms, 0.5) if grant_ms else None,
            grant_max_ms=round(max(grant_ms), 2) if grant_ms else None,
            release_p50_ms=_percentile(release_ms, 0.5) if release_ms else None,
            release_max_ms=round(max(release_ms), 2) if release_ms else None,
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    finally:
        _lock.release()


def _repeat_clearance(keyspace: str, repeats: int) -> Tuple[List[float], List[float]]:
    """Time a grant and a release over repeats, both on their applied path.

    One drone cycles in and out of the quietest zone, so every grant finds a slot and
    every release finds a clearance to give back; a repeat that was refused would time
    a cheaper transaction and flatter the figure.  A drone of its own, outside the
    fleet the map draws, so a run leaves no clearance against a drone a viewer is
    watching.

    Not the airport zone the seven steps use, because the last of those steps must be
    the last thing that touched it: the state this returns is what the page shows.
    """
    if repeats <= 0:
        return [], []
    entity_id = "asset-measure"
    grant_ms: List[float] = []
    release_ms: List[float] = []
    for _ in range(repeats):
        grant = _run_grant(keyspace, "measure grant", DEMO_MEASURE_ZONE, entity_id, False)
        if grant.applied:
            grant_ms.append(grant.duration_ms)
        release = _run_release(keyspace, "measure release", DEMO_MEASURE_ZONE, entity_id, False)
        if release.applied:
            release_ms.append(release.duration_ms)
    return grant_ms, release_ms
