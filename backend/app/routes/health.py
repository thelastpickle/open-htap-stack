"""Platform health routes — a TCP reachability probe per service."""
import socket
import threading
import time
from typing import List, Optional, Tuple

from fastapi import APIRouter

from app.config import settings
from app.db.cassandra_client import cassandra_client
from app.models import PlatformHealthResponse, ServiceHealth

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
