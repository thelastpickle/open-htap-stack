"""Demo control routes.

The dashboard's Settings page drives the data generator through these endpoints.
The producer polls ``GET /api/settings/demo`` every few seconds and adopts what
it finds, so every control here has a visible effect on the live stack; nothing
on this page is decorative.

State is deliberately in memory only: restarting the backend returns the demo to
the defaults its environment declares.
"""
import threading
from typing import Any, Dict

from fastapi import APIRouter

from app.config import settings as env
from app.db.cassandra_client import cassandra_client
from app.models import DemoSettings, DemoSettingsResponse

router = APIRouter(prefix="/api/settings", tags=["settings"])

_lock = threading.Lock()


def _defaults() -> DemoSettings:
    """The demo's startup state, as configured by the environment."""
    return DemoSettings(
        drones_enabled=min(env.demo_n_entities, env.demo_max_entities),
        events_per_sec=env.demo_events_per_sec,
        outlier_percent=env.demo_outlier_percent,
        paused=False,
    )


_current = _defaults()


@router.get("/demo", response_model=DemoSettingsResponse)
def get_demo_settings() -> DemoSettingsResponse:
    """The settings currently in force.  Polled by the data producer."""
    with _lock:
        return DemoSettingsResponse(settings=_current)


@router.get("/demo/defaults", response_model=DemoSettingsResponse)
def get_default_settings() -> DemoSettingsResponse:
    return DemoSettingsResponse(settings=_defaults(), message="Startup defaults")


@router.post("/demo", response_model=DemoSettingsResponse)
def update_demo_settings(new: DemoSettings) -> DemoSettingsResponse:
    global _current
    clamped = new.model_copy(
        update={"drones_enabled": min(new.drones_enabled, env.demo_max_entities)}
    )
    with _lock:
        _current = clamped
    note = (
        f"Fleet size capped at MAX_ENTITIES ({env.demo_max_entities})"
        if clamped.drones_enabled != new.drones_enabled
        else "Settings updated; the producer picks them up within its poll interval"
    )
    return DemoSettingsResponse(settings=clamped, message=note)


@router.post("/demo/pause", response_model=DemoSettingsResponse)
def toggle_pause() -> DemoSettingsResponse:
    """Stop or resume event generation."""
    global _current
    with _lock:
        _current = _current.model_copy(update={"paused": not _current.paused})
        paused = _current.paused
        state = _current
    return DemoSettingsResponse(
        settings=state, message=f"Data generation {'paused' if paused else 'resumed'}"
    )


@router.post("/demo/cleanup")
def clear_fleet_state() -> Dict[str, Any]:
    """Truncate drone_latest_status.

    Reducing the fleet size leaves the rows of retired assets behind, since
    nothing overwrites them; clearing the table lets the KPIs settle on the new
    size as fresh telemetry arrives.  Event history and the zones are untouched.
    """
    if not cassandra_client.connected:
        return {"success": False, "message": "Cassandra not connected"}
    try:
        cassandra_client.execute_query("TRUNCATE drone_latest_status")
        return {"success": True, "message": "Fleet state cleared; KPIs rebuild as telemetry arrives"}
    except Exception as e:
        return {"success": False, "message": str(e)}
