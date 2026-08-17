"""Platform health routes — reachability, work in flight, and stopping it.

The reachability half is a TCP probe per service.  The rest is the operator's half
of the dashboard: what the engines are working on right now, whoever asked for it,
and the controls to stop a query or rebuild a connection.  Restarting a *service*
is deliberately not here; see reconnect() for why.
"""
import socket
import threading
import time
from typing import List, Optional, Tuple

from fastapi import APIRouter, HTTPException

from app.config import settings
from app.db.cassandra_client import cassandra_client
from app.db.presto_client import presto_client
from app.db import spark_ui
from app.models import (
    KillQueryRequest,
    OperationResult,
    PlatformHealthResponse,
    ReconnectRequest,
    RunningQuery,
    RunningWorkResponse,
    ServiceHealth,
)
from app.routes.query import ENGINES, cancel_comparison, running_comparison

router = APIRouter(prefix="/api/platform", tags=["platform"])

PROBE_TIMEOUT_S = 2
# The overview KPIs embed the health score and are polled every few seconds.
# Probing four sockets on every poll would add seconds of latency whenever a
# service is down, so the score is cached for a little longer than a poll cycle.
SCORE_CACHE_TTL_S = 10


def _service_targets() -> List[tuple]:
    """The stack's services and where this backend expects to reach them.

    Every host and port comes from configuration, so the probe follows the
    backend whether it runs inside the compose network or on the host.
    """
    return [
        ("Cassandra", settings.cassandra_host, settings.cassandra_port),
        ("Kafka", settings.kafka_host, settings.kafka_port),
        ("Presto", settings.presto_host, settings.presto_port),
        ("Spark", settings.spark_ui_host, settings.spark_ui_port),
    ]


def _probe(host: str, port: int) -> str:
    try:
        with socket.create_connection((host, port), timeout=PROBE_TIMEOUT_S):
            return "up"
    except OSError:
        return "down"
    except Exception:
        return "unknown"


_score_lock = threading.Lock()
_cached_score: Optional[Tuple[float, float]] = None  # (expires_at_monotonic, score)


def _probe_services() -> List[ServiceHealth]:
    return [
        ServiceHealth(name=name, status=_probe(host, port), endpoint=f"{host}:{port}")
        for name, host, port in _service_targets()
    ]


def _score(services: List[ServiceHealth]) -> float:
    if not services:
        return 0.0
    return round(sum(1 for s in services if s.status == "up") / len(services), 3)


def platform_health_score() -> float:
    """Fraction of the stack's services accepting connections, cached briefly."""
    global _cached_score
    with _score_lock:
        if _cached_score and _cached_score[0] > time.monotonic():
            return _cached_score[1]
    score = _score(_probe_services())
    with _score_lock:
        _cached_score = (time.monotonic() + SCORE_CACHE_TTL_S, score)
    return score


@router.get("/health", response_model=PlatformHealthResponse)
def get_platform_health() -> PlatformHealthResponse:
    global _cached_score
    services = _probe_services()
    score = _score(services)
    with _score_lock:
        _cached_score = (time.monotonic() + SCORE_CACHE_TTL_S, score)

    total_drones = 0
    if cassandra_client.connected:
        try:
            total_drones = cassandra_client.get_drone_count()
        except Exception as e:
            print(f"[health] drone count failed: {e}")

    return PlatformHealthResponse(
        services=services, total_drones=total_drones, overall_health_score=score
    )


# ──────────────────── Work in flight, and stopping it ────────────────────

# The dashboard's own client per path, for the reconnect control.  Derived from the
# comparison's table so the two cannot come to disagree about what the paths are.
_CLIENTS = {name: client for name, (client, _dialect) in ENGINES.items()}


@router.get("/running", response_model=RunningWorkResponse)
def get_running_work() -> RunningWorkResponse:
    """What the engines are working on, and the comparison holding the lock.

    Each engine is asked directly rather than the dashboard reporting what it
    submitted, so work it knows nothing about is included: a spark-sql session in
    the container, or a presto-cli query, is usually exactly what you want to see
    when the dashboard has gone slow.
    """
    queries: List[RunningQuery] = []
    unreadable = {}

    try:
        for query in presto_client.running_queries():
            queries.append(
                RunningQuery(
                    engine="presto",
                    id=query["id"],
                    state=query["state"],
                    running_s=query["running_s"],
                    sql=query["sql"],
                    submitter=query["source"] or query["user"],
                )
            )
    except Exception as e:
        unreadable["presto"] = str(e)

    try:
        for job in spark_ui.running_jobs():
            queries.append(
                RunningQuery(
                    engine="spark",
                    id=job["id"],
                    state=job["state"],
                    running_s=job["running_s"],
                    sql=job["sql"],
                    tasks_done=job["tasks_done"],
                    tasks_total=job["tasks_total"],
                )
            )
    except Exception as e:
        unreadable["spark"] = str(e)

    # Cassandra has no register of running queries to read, and needs none here: a
    # point read is milliseconds, so anything worth seeing on this page arrived
    # through one of the two above.  Said rather than silently left out.
    unreadable["cassandra"] = "Cassandra keeps no list of running queries to read"

    return RunningWorkResponse(
        comparison=running_comparison(), queries=queries, unreadable=unreadable
    )


@router.post("/running/cancel-comparison", response_model=OperationResult)
def post_cancel_comparison() -> OperationResult:
    """Stop the comparison in flight, so the next one can run."""
    actions = cancel_comparison()
    if not actions:
        return OperationResult(ok=False, actions=["no comparison was running"])
    return OperationResult(actions=actions)


@router.post("/running/kill", response_model=OperationResult)
def post_kill_query(req: KillQueryRequest) -> OperationResult:
    """Cancel one query, named by the handle its own engine gave it."""
    try:
        if req.engine == "presto":
            presto_client.kill_query(req.id)
            return OperationResult(actions=[f"asked Presto to cancel {req.id}"])
        spark_ui.kill_job(req.id)
        return OperationResult(actions=[f"asked Spark to kill job {req.id}"])
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e)) from e


@router.post("/reconnect", response_model=OperationResult)
def reconnect(req: ReconnectRequest) -> OperationResult:
    """Rebuild the dashboard's connection to a path, or to all of them.

    This restarts the client, not the service.  The dashboard is a container beside
    the others with no control over them, which is the right way round for
    something reachable from a browser; restarting a service is a command on the
    host, and the page shows which one.  A reconnect is the useful half anyway: it
    is what clears a session that has gone stale, and it costs no downtime.
    """
    targets = list(_CLIENTS) if req.target == "all" else [req.target]
    actions = []
    reconnected = 0
    for name in targets:
        client = _CLIENTS[name]
        if getattr(client, "busy", False):
            # connect() would queue behind the query rather than replace it, and a
            # control that hangs for a quarter of an hour explains nothing.
            actions.append(f"{name}: busy with a query, so stop that first")
            continue
        try:
            # force=True for Cassandra, whose connect() is otherwise a no-op when it
            # already believes it is connected; the others rebuild unconditionally.
            if name == "cassandra":
                client.connect(force=True)
            else:
                client.connect()
        except Exception as e:
            actions.append(f"{name}: {e}")
            continue
        if client.connected:
            reconnected += 1
            actions.append(f"{name}: reconnected")
        else:
            actions.append(f"{name}: still unreachable")
    return OperationResult(ok=reconnected == len(targets), actions=actions)
