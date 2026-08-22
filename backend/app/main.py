"""HTAP Mission Control — the dashboard's API.

One FastAPI app in front of the three engines the stack runs: Cassandra for the
live fleet state, Presto for analytical queries over the same rows, and Spark for
batch analytics.  It also reads those rows itself, with the cqlite reader, which
parses Cassandra's SSTable files in this process.  Every endpoint reports which
engine answered it, because showing that is the point of the demo.
"""
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.db.cassandra_client import cassandra_client
from app.db.cqlite_client import cqlite_client
from app.db.presto_client import presto_client
from app.db.spark_client import spark_bulk_client, spark_client
from app.routes import alerts, demo, health, map, overview, query, settings as settings_routes
from app.routes import vector, zones

ROUTERS = (
    overview.router,
    map.router,
    alerts.router,
    query.router,
    zones.router,
    health.router,
    vector.router,
    settings_routes.router,
    demo.router,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Connect eagerly so the first dashboard poll is not paying for it, but never
    # fatally: the stack's services come up in their own time and every endpoint
    # already reports an engine it cannot reach.
    # The bulk reader is here as well as the connector, on its own connection: it
    # is one of the five paths the comparison offers, so it should report itself
    # reachable before anybody uses it rather than only afterwards, and in a run
    # that starts every path at once it should not be the one still opening a
    # session while the others are already scanning.
    # The cqlite reader comes after Cassandra, and must: it takes each table's
    # CREATE TABLE statement from the driver's schema metadata, so the CQL path has
    # to have connected once before it can register anything.
    for name, client in (
        ("Cassandra", cassandra_client),
        ("Presto", presto_client),
        ("Spark Thrift Server", spark_client),
        ("Spark bulk reader", spark_bulk_client),
        ("cqlite reader", cqlite_client),
    ):
        try:
            client.connect()
        except Exception as e:
            print(f"[startup] {name} unavailable: {e}")
    yield


def create_app() -> FastAPI:
    app = FastAPI(
        title="HTAP Mission Control",
        description=__doc__,
        version="1.0.0",
        lifespan=lifespan,
    )

    origins = ["*"] if settings.allowed_origins == "*" else settings.allowed_origins.split(",")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.get("/api/health", tags=["platform"])
    def liveness():
        """Is the API up?  Engine reachability lives at /api/platform/health."""
        return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}

    for router in ROUTERS:
        app.include_router(router)
    return app


app = create_app()
