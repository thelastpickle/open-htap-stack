"""Demo routes — the scripted breach scenario and the latency probes."""
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional

from fastapi import APIRouter, HTTPException

from app.db.cassandra_client import cassandra_client
from app.db.presto_client import presto_client
from app.routes.vector import probe_vector

router = APIRouter(prefix="/api/demo", tags=["demo"])

BREACH_RISK_SCORE = 0.97
BREACH_ZONE_ID = "scenario-zone"


@router.post("/trigger-breach-scenario")
def trigger_breach_scenario() -> Dict[str, Any]:
    """Flag a real asset as breaching and write a matching alert.

    Every field written here is real data in Cassandra, so the map, the KPIs and
    the alert feed all pick it up through their normal queries.  If there is no
    fleet to pick from the endpoint says so rather than inventing one.
    """
    if not cassandra_client.connected:
        raise HTTPException(status_code=503, detail="Cassandra unavailable")

    try:
        candidates = cassandra_client.get_drones(limit=50, flying_only=True)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not read the fleet: {e}")
    if not candidates:
        raise HTTPException(
            status_code=409,
            detail="No flying assets yet — wait for the producer to warm up, then retry",
        )

    target = random.choice(candidates)
    entity_id = str(target["entity_id"])
    latitude = float(target.get("latitude") or 0.0)
    longitude = float(target.get("longitude") or 0.0)
    now = datetime.now(timezone.utc)
    alert_id = uuid.uuid1()

    try:
        cassandra_client.execute_query(
            "UPDATE drone_latest_status SET predicted_zone_breach = true, "
            "near_restricted_zone = true, risk_score = %s WHERE entity_id = %s",
            (BREACH_RISK_SCORE, entity_id),
        )
        cassandra_client.execute_query(
            "INSERT INTO alerts_by_bucket (bucket, alert_time, entity_id, alert_id, alert_type, "
            "severity, zone_id, latitude, longitude, altitude_m, message, risk_score) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (
                now.strftime("%Y-%m-%dT%H"),
                now,
                entity_id,
                alert_id,
                "zone_breach_predicted",
                "critical",
                BREACH_ZONE_ID,
                latitude,
                longitude,
                float(target.get("altitude_m") or 0.0),
                f"Scenario: {entity_id} is on a predicted course into restricted airspace",
                BREACH_RISK_SCORE,
            ),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not inject the scenario: {e}")

    return {
        "success": True,
        "scenario": "zone_breach",
        "entity_id": entity_id,
        "latitude": latitude,
        "longitude": longitude,
        "alert_id": str(alert_id),
        "severity": "critical",
        "message": f"{entity_id} flagged for a predicted zone breach; alert written",
    }


# ──────────────────────── Latency probes ────────────────────────


@router.get("/latency")
def get_latency() -> Dict[str, Any]:
    """Measure one representative query per tier.

    Each figure is the round trip this backend actually observed, including the
    network hop.  A tier that cannot answer reports null, which the UI shows as
    an em dash rather than a zero.
    """
    return {
        "cassandra_point_read_ms": _timed(_cassandra_point_read),
        "presto_scan_ms": _timed(_presto_scan),
        "vector_search_ms": _timed(_vector_probe),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def _timed(prepare: Callable[[], Optional[Callable[[], None]]]) -> Optional[float]:
    """Time one query in milliseconds, or return None if it cannot run.

    ``prepare`` does any setup a probe needs and hands back the single call to be
    timed, so setup never lands inside the measurement.
    """
    try:
        query = prepare()
        if query is None:
            return None
        start = time.perf_counter()
        query()
        return round((time.perf_counter() - start) * 1000, 1)
    except Exception:
        return None


_probe_entity_id: Optional[str] = None


def _cassandra_point_read() -> Optional[Callable[[], None]]:
    """A single-partition read — the OLTP path the dashboard claims is fast.

    Choosing the asset is a scan, so it happens here rather than in the timed
    call, which would otherwise report a scan's latency as a point read's.
    """
    global _probe_entity_id
    if not cassandra_client.connected:
        return None
    if _probe_entity_id is None:
        rows = cassandra_client.execute_query("SELECT entity_id FROM drone_latest_status LIMIT 1")
        if not rows:
            return None
        _probe_entity_id = str(rows[0]["entity_id"])

    entity_id = _probe_entity_id

    def read() -> None:
        global _probe_entity_id
        if cassandra_client.get_drone_detail(entity_id) is None:
            # The asset is gone — a fleet resize, say.  Pick another next time.
            _probe_entity_id = None
            raise LookupError(entity_id)

    return read


def _presto_scan() -> Optional[Callable[[], None]]:
    """An aggregate over the same table — the OLAP path, for contrast."""
    if not presto_client.connected:
        return None
    return lambda: presto_client.execute_query(
        "SELECT count(*) AS cnt FROM demo.drone_latest_status"
    )


def _vector_probe() -> Optional[Callable[[], None]]:
    """An ANN lookup against the SAI index; unavailable until rows are indexed."""
    if not cassandra_client.connected:
        return None
    return lambda: cassandra_client.execute_query(
        "SELECT entity_id FROM drone_text_embeddings ORDER BY payload_vector ANN OF %s LIMIT 1",
        (probe_vector(),),
    )
