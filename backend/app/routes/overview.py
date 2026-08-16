"""Overview dashboard routes — fleet KPIs and ingestion volume."""
from typing import Any, Dict, List

from fastapi import APIRouter
from fastapi.responses import Response

from app.db.cassandra_client import cassandra_client
from app.models import AlertSummary, IngestionBucket, OverviewKPIs
from app.routes.health import platform_health_score

router = APIRouter(prefix="/api/overview", tags=["overview"])

MAX_HISTORY_HOURS = 48
CSV_HEADER = "time,timestamp,count"


@router.get("/kpis", response_model=OverviewKPIs)
def get_overview_kpis() -> OverviewKPIs:
    return OverviewKPIs(
        **_fetch_kpis(),
        platform_health_score=platform_health_score(),
        latest_alerts=_fetch_latest_alerts(),
    )


@router.get("/ingestion-history")
def get_ingestion_history(hours: int = 8) -> Dict[str, Any]:
    """Ingestion volume in 30-minute buckets over the last N hours."""
    hours = max(1, min(hours, MAX_HISTORY_HOURS))
    if not cassandra_client.connected:
        return {"hours": hours, "buckets": []}
    try:
        buckets = cassandra_client.get_ingestion_history(hours=hours)
        return {"hours": hours, "buckets": [IngestionBucket(**b).model_dump() for b in buckets]}
    except Exception as e:
        print(f"[overview] ingestion-history failed: {e}")
        return {"hours": hours, "buckets": []}


@router.get("/ingestion-history/csv")
def download_ingestion_csv(hours: int = 8) -> Response:
    """The same series as a CSV download."""
    hours = max(1, min(hours, MAX_HISTORY_HOURS))
    lines = [CSV_HEADER]
    if cassandra_client.connected:
        try:
            lines += [
                f"{b['time']},{b['timestamp']},{b['count']}"
                for b in cassandra_client.get_ingestion_history(hours=hours)
            ]
        except Exception as e:
            print(f"[overview] ingestion CSV failed: {e}")
    return Response(
        content="\n".join(lines) + "\n",
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="ingestion_log_{hours}h.csv"'},
    )


@router.post("/resync")
def trigger_resync() -> Dict[str, Any]:
    """Re-probe Cassandra and return fresh KPIs."""
    try:
        cassandra_client.connect(force=not cassandra_client.connected)
        return {"success": True, "message": "Re-sync complete", "kpis": _fetch_kpis()}
    except Exception as e:
        return {"success": False, "message": str(e)}


def _fetch_kpis() -> Dict[str, Any]:
    """Every fleet KPI.  Returns {} when Cassandra is unreachable, which leaves
    the response model's zero defaults in place."""
    if not cassandra_client.connected:
        cassandra_client.connect()
    if not cassandra_client.connected:
        return {}
    try:
        return cassandra_client.get_overview_kpis()
    except Exception as e:
        print(f"[overview] KPI query failed: {e}")
        return {}


def _fetch_latest_alerts(limit: int = 5) -> List[AlertSummary]:
    if not cassandra_client.connected:
        return []
    try:
        return [
            AlertSummary(
                alert_id=str(a.get("alert_id", "")),
                alert_time=str(a.get("alert_time", "")),
                entity_id=str(a.get("entity_id", "")),
                alert_type=str(a.get("alert_type", "")),
                severity=str(a.get("severity", "")),
                message=str(a.get("message", "")),
                risk_score=float(a.get("risk_score") or 0.0),
            )
            for a in cassandra_client.get_alerts(limit=limit)
        ]
    except Exception as e:
        print(f"[overview] latest alerts failed: {e}")
        return []
