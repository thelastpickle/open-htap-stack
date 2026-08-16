"""Query routes — ad-hoc SQL, the three-engine benchmark, and NL → SQL."""
import asyncio
import re
import time
from typing import Any, Dict, Optional

import httpx
from fastapi import APIRouter, HTTPException

from app.config import settings
from app.db.cassandra_client import cassandra_client
from app.db.presto_client import presto_client
from app.db.spark_client import spark_client
from app.models import (
    BenchmarkRequest,
    BenchmarkResponse,
    EngineResult,
    NLQueryRequest,
    NLQueryResponse,
    SQLQueryRequest,
    SQLQueryResult,
)

router = APIRouter(prefix="/api/query", tags=["query"])

# Every engine here is reachable read-write, so the console is restricted to
# single SELECT statements.  Matched on word boundaries: a substring test would
# reject "SELECT created_at" for containing CREATE.
WRITE_KEYWORDS = (
    "INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "TRUNCATE",
    "GRANT", "REVOKE",
)
_WRITE_KEYWORD_RE = re.compile(r"\b(" + "|".join(WRITE_KEYWORDS) + r")\b", re.IGNORECASE)
_ALLOW_FILTERING_RE = re.compile(r"\s*ALLOW\s+FILTERING\s*", re.IGNORECASE)
_LIMIT_RE = re.compile(r"\s+LIMIT\s+\d+\s*$", re.IGNORECASE)

# Tables the console exposes, used to qualify bare names per engine.
DEMO_TABLES = ("drone_latest_status", "drone_events_by_entity", "drone_text_embeddings",
               "alerts_by_bucket", "ingestion_counts", "restricted_zones", "events")
_BARE_TABLE_RE = re.compile(r"(?<![\w.])(" + "|".join(DEMO_TABLES) + r")\b", re.IGNORECASE)
_QUALIFIED_TABLE_RE = re.compile(r"\bdemo\.(" + "|".join(DEMO_TABLES) + r")\b", re.IGNORECASE)


def _validate(sql: str) -> str:
    """Accept one read-only statement, or raise 400."""
    statement = sql.strip().rstrip(";").strip()
    if not statement:
        raise HTTPException(status_code=400, detail="Empty query")
    if ";" in statement:
        raise HTTPException(status_code=400, detail="Only a single statement is allowed")
    if not statement.upper().startswith("SELECT"):
        raise HTTPException(status_code=400, detail="Only SELECT queries are allowed")
    forbidden = _WRITE_KEYWORD_RE.search(statement)
    if forbidden:
        raise HTTPException(
            status_code=400, detail=f"Forbidden keyword in a read-only console: {forbidden.group(1).upper()}"
        )
    return statement


def _strip_limit(sql: str) -> str:
    return _LIMIT_RE.sub("", sql).strip()


def _has_limit(sql: str) -> bool:
    return bool(_LIMIT_RE.search(sql))


def sql_for_cassandra(sql: str, limit: int) -> str:
    """CQL demands ``LIMIT n ALLOW FILTERING`` in that order, and knows no
    keyspace prefix beyond the session's own."""
    statement = _strip_limit(_ALLOW_FILTERING_RE.sub(" ", sql).strip())
    statement = _QUALIFIED_TABLE_RE.sub(r"\1", statement)
    return f"{statement} LIMIT {limit} ALLOW FILTERING"


def sql_for_presto(sql: str, limit: int) -> str:
    """Presto reads Cassandra through its catalog, where tables live in the
    ``demo`` schema.  ALLOW FILTERING is Cassandra-only and must go."""
    statement = _ALLOW_FILTERING_RE.sub(" ", sql).strip()
    statement = _BARE_TABLE_RE.sub(r"demo.\1", statement)
    return statement if _has_limit(statement) else f"{statement} LIMIT {limit}"


def sql_for_spark(sql: str, limit: int) -> str:
    """The Thrift Server registers the Cassandra tables as temp views in the
    session's default database, so names stay unqualified."""
    statement = _ALLOW_FILTERING_RE.sub(" ", sql).strip()
    statement = _QUALIFIED_TABLE_RE.sub(r"\1", statement)
    return statement if _has_limit(statement) else f"{statement} LIMIT {limit}"


ENGINES = {
    "cassandra": (cassandra_client, sql_for_cassandra),
    "presto": (presto_client, sql_for_presto),
    "spark": (spark_client, sql_for_spark),
}


def _run(engine: str, sql: str, limit: int) -> EngineResult:
    """Run one query on one engine, reporting failure in the result rather than
    raising, so a partial outage still renders."""
    client, dialect = ENGINES[engine]
    if not client.connected:
        try:
            client.connect()
        except Exception as e:
            return EngineResult(available=False, error=str(e))
    if not client.connected:
        return EngineResult(available=False, error="Engine not connected")

    statement = dialect(sql, limit)
    try:
        start = time.perf_counter()
        rows = client.execute_query(statement)
        elapsed_ms = round((time.perf_counter() - start) * 1000, 1)
    except Exception as e:
        return EngineResult(sql=statement, error=str(e))

    columns = list(rows[0].keys()) if rows else []
    return EngineResult(
        sql=statement,
        columns=columns,
        rows=[[r.get(c) for c in columns] for r in rows],
        row_count=len(rows),
        query_time_ms=elapsed_ms,
    )


@router.post("/sql", response_model=SQLQueryResult)
def execute_sql(req: SQLQueryRequest) -> SQLQueryResult:
    """Run one SELECT on the chosen engine."""
    if req.engine not in ENGINES:
        raise HTTPException(status_code=400, detail=f"Unknown engine: {req.engine}")
    result = _run(req.engine, _validate(req.sql), req.limit)
    if not result.available:
        raise HTTPException(status_code=503, detail=result.error or "Engine unavailable")
    if result.error:
        raise HTTPException(status_code=400, detail=result.error)
    return SQLQueryResult(
        columns=result.columns,
        rows=result.rows,
        row_count=result.row_count,
        query_time_ms=result.query_time_ms,
        sql=result.sql,
    )


@router.post("/benchmark", response_model=BenchmarkResponse)
def run_benchmark(req: BenchmarkRequest) -> BenchmarkResponse:
    """Run one logical query on all three engines and return all three timings.

    The engines run in sequence so they are not competing for the same host's
    CPU while being timed.  Per-engine failures are reported in the body, so the
    comparison still renders when one engine is down.
    """
    statement = _validate(req.sql)
    return BenchmarkResponse(**{engine: _run(engine, statement, req.limit) for engine in ENGINES})


# ──────────────────────── Natural language → SQL ────────────────────────

# NL questions become Presto SQL: they ask for ordering, ranges and aggregates,
# which are ordinary SQL but not things CQL can answer.  Presto reads the same
# live Cassandra rows, so the answer is still current.
NL_ENGINE = "presto"
NL_SCHEMA = (
    "demo.drone_latest_status(entity_id, event_time, latitude, longitude, altitude_m, "
    "speed_mps, heading_deg, is_flying, temp_internal_c, temp_external_c, "
    "near_restricted_zone, predicted_zone_breach, risk_score)"
)


@router.post("/nl", response_model=NLQueryResponse)
async def nl_query(req: NLQueryRequest) -> NLQueryResponse:
    prompt = req.prompt.strip()
    if not prompt:
        raise HTTPException(status_code=400, detail="Empty prompt")

    sql = None
    if settings.openrouter_api_key:
        sql = await _llm_to_sql(prompt)
    if not sql:
        sql = _patterns_to_sql(prompt.lower())

    render_hint = _render_hint(prompt.lower())
    try:
        statement = _validate(sql)
    except HTTPException as e:
        return NLQueryResponse(generated_sql=sql, error=str(e.detail), render_hint=render_hint)

    # This handler is async for the HTTP call to the translator, so the blocking
    # engine query has to leave the event loop.
    result = await asyncio.to_thread(_run, NL_ENGINE, statement, 100)
    if result.error:
        return NLQueryResponse(generated_sql=result.sql or sql, error=result.error, render_hint=render_hint)
    return NLQueryResponse(
        generated_sql=result.sql or sql,
        engine=NL_ENGINE,
        render_hint=render_hint,
        result=SQLQueryResult(
            columns=result.columns,
            rows=result.rows,
            row_count=result.row_count,
            query_time_ms=result.query_time_ms,
            sql=result.sql,
        ),
    )


async def _llm_to_sql(prompt: str) -> Optional[str]:
    """Translate a question with an OpenAI-compatible chat endpoint.

    Returns None on any failure so the caller falls back to the local patterns;
    the demo must work without an API key.
    """
    system = (
        "You translate questions into a single Presto SQL SELECT statement.\n"
        f"The only table is {NL_SCHEMA}.\n"
        "Return raw SQL only: no prose, no markdown fence, no trailing semicolon."
    )
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {settings.openrouter_api_key}",
                    "Content-Type": "application/json",
                    "X-Title": "HTAP Mission Control",
                },
                json={
                    "model": settings.openrouter_model,
                    "messages": [
                        {"role": "system", "content": system},
                        {"role": "user", "content": prompt},
                    ],
                    "max_tokens": 500,
                    "temperature": 0.0,
                },
            )
            resp.raise_for_status()
            sql = resp.json()["choices"][0]["message"]["content"]
    except Exception as e:
        print(f"[nl] LLM translation failed, using patterns: {e}")
        return None

    sql = re.sub(r"```(?:sql)?", "", sql).strip()
    return sql if sql.upper().startswith("SELECT") else None


_NL_COMPARISONS = re.compile(
    r"(above|over|greater than|more than|below|less than|under|between)\s+"
    r"(-?\d+(?:\.\d+)?)(?:\s*(?:and|to|-)\s*(-?\d+(?:\.\d+)?))?"
)
_NL_MEASURES = (
    (("temperature", "temp", "hot", "overheat"), "temp_internal_c"),
    (("altitude", "height", "high"), "altitude_m"),
    (("speed", "fast", "velocity"), "speed_mps"),
    (("risk",), "risk_score"),
)
_NL_BASE = (
    "SELECT entity_id, event_time, latitude, longitude, altitude_m, speed_mps, "
    "temp_internal_c, risk_score FROM demo.drone_latest_status"
)


def _patterns_to_sql(prompt: str) -> str:
    """A small rule-based translator, so the feature works without an API key."""
    measure = next((col for words, col in _NL_MEASURES if any(w in prompt for w in words)), None)

    if measure:
        match = _NL_COMPARISONS.search(prompt)
        if match:
            operator, first, second = match.group(1), match.group(2), match.group(3)
            if operator == "between" and second:
                return f"{_NL_BASE} WHERE {measure} BETWEEN {first} AND {second} ORDER BY {measure} DESC"
            if operator in ("above", "over", "greater than", "more than"):
                return f"{_NL_BASE} WHERE {measure} > {first} ORDER BY {measure} DESC"
            return f"{_NL_BASE} WHERE {measure} < {first} ORDER BY {measure} ASC"
        return f"{_NL_BASE} ORDER BY {measure} DESC"

    if "breach" in prompt:
        return f"{_NL_BASE} WHERE predicted_zone_breach = true ORDER BY risk_score DESC"
    if "zone" in prompt:
        return f"{_NL_BASE} WHERE near_restricted_zone = true ORDER BY risk_score DESC"
    if "ground" in prompt:
        return f"{_NL_BASE} WHERE is_flying = false"
    if any(w in prompt for w in ("flying", "active", "airborne")):
        return f"{_NL_BASE} WHERE is_flying = true"
    if any(w in prompt for w in ("count", "how many", "total", "stats")):
        return (
            "SELECT count(*) AS assets, "
            "count_if(is_flying) AS flying, "
            "count_if(near_restricted_zone) AS near_zone, "
            "round(avg(speed_mps), 1) AS avg_speed_mps "
            "FROM demo.drone_latest_status"
        )
    return _NL_BASE


def _render_hint(prompt: str) -> str:
    if any(w in prompt for w in ("map", "where", "location", "position")):
        return "map"
    if any(w in prompt for w in ("count", "how many", "total", "stats")):
        return "kpi"
    if any(w in prompt for w in ("trend", "history", "over time")):
        return "chart"
    return "table"


def engine_status() -> Dict[str, bool]:
    """Which engines are currently connected, for the UI's engine selector."""
    return {name: bool(client.connected) for name, (client, _) in ENGINES.items()}


@router.get("/engines")
def get_engines() -> Dict[str, Any]:
    return {"engines": engine_status()}
