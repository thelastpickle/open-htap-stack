"""Alerts routes."""
from typing import Optional

from fastapi import APIRouter, Query

from app.db.cassandra_client import cassandra_client
from app.models import AlertRecord, AlertsResponse

router = APIRouter(prefix="/api/alerts", tags=["alerts"])


@router.get("", response_model=AlertsResponse)
def get_alerts(
    severity: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
) -> AlertsResponse:
    """Recent alerts, newest first.

    ``severity`` filters after the read, so ``total_count`` always reports how
    many alerts were found in the window and the UI's per-severity counts stay
    consistent with each other.
    """
    if not cassandra_client.connected:
        return AlertsResponse(alerts=[], total_count=0)
    try:
        rows = cassandra_client.get_alerts(limit=limit)
    except Exception as e:
        print(f"[alerts] query failed: {e}")
        return AlertsResponse(alerts=[], total_count=0)

    alerts = [
        AlertRecord(
            alert_id=str(a.get("alert_id", "")),
            alert_time=str(a.get("alert_time", "")),
            entity_id=str(a.get("entity_id", "")),
            alert_type=str(a.get("alert_type", "")),
            severity=str(a.get("severity", "")),
            zone_id=str(a["zone_id"]) if a.get("zone_id") else None,
            latitude=float(a.get("latitude") or 0.0),
            longitude=float(a.get("longitude") or 0.0),
            altitude_m=float(a.get("altitude_m") or 0.0),
            message=str(a.get("message", "")),
            risk_score=float(a.get("risk_score") or 0.0),
        )
        for a in rows
    ]
    matching = [a for a in alerts if not severity or a.severity == severity]
    return AlertsResponse(alerts=matching, total_count=len(alerts))
