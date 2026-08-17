"""Query routes — ad-hoc SQL, the four-path comparison, and NL → SQL."""
import asyncio
import re
import threading
import time
from typing import Any, Dict, List, Optional

import httpx
from fastapi import APIRouter, HTTPException

from app.config import settings
from app.db import spark_ui
from app.db.cassandra_client import cassandra_client
from app.db.presto_client import presto_client
from app.db.spark_client import BULK_VIEW_PREFIX, spark_bulk_client, spark_client
from app.models import (
    BenchmarkRequest,
    BenchmarkResponse,
    ComparisonRun,
    EngineResult,
    NLQueryRequest,
    NLQueryResponse,
    OltpImpact,
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

# Tables the console exposes.  Each engine reaches them under a different name, so
# a statement is rewritten per engine before it is issued.
DEMO_TABLES = ("drone_latest_status", "drone_events_by_entity", "drone_text_embeddings",
               "alerts_by_bucket", "ingestion_counts", "restricted_zones", "events")

# Only a name that follows FROM or JOIN is a table.  Matching the bare word
# anywhere would rewrite anything that happens to share a table's name — a column,
# or an alias: "SELECT count(*) AS events FROM events" would have its alias
# rewritten too, and the engines would disagree about what the result column is
# called.  A comma-separated table list is not handled, and no engine here needs
# one.
_TABLE_REFERENCE_RE = re.compile(
    r"(?P<lead>\b(?:FROM|JOIN)\s+)(?:demo\.)?(?P<table>" + "|".join(DEMO_TABLES) + r")\b",
    re.IGNORECASE,
)


def _rewrite_tables(sql: str, prefix: str = "") -> str:
    """Rewrite every table reference to ``<prefix><table>``, dropping any keyspace."""
    return _TABLE_REFERENCE_RE.sub(lambda m: f"{m.group('lead')}{prefix}{m.group('table')}", sql)


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
    return f"{_rewrite_tables(statement)} LIMIT {limit} ALLOW FILTERING"


def sql_for_presto(sql: str, limit: int) -> str:
    """Presto reads Cassandra through its catalog, where the tables live in the
    ``demo`` schema.  ALLOW FILTERING is Cassandra-only and must go."""
    statement = _rewrite_tables(_ALLOW_FILTERING_RE.sub(" ", sql).strip(), prefix="demo.")
    return statement if _has_limit(statement) else f"{statement} LIMIT {limit}"


def sql_for_spark(sql: str, limit: int) -> str:
    """The connector registers each table as a temp view under its own name in the
    Thrift Server session, so names stay unqualified."""
    statement = _rewrite_tables(_ALLOW_FILTERING_RE.sub(" ", sql).strip())
    return statement if _has_limit(statement) else f"{statement} LIMIT {limit}"


def sql_for_spark_bulk(sql: str, limit: int) -> str:
    """Aim the statement at the bulk reader's views rather than the connector's.

    Both paths are registered in the same Thrift Server session, told apart by the
    view name, so which one answers is decided here.
    """
    statement = _rewrite_tables(_ALLOW_FILTERING_RE.sub(" ", sql).strip(), prefix=BULK_VIEW_PREFIX)
    return statement if _has_limit(statement) else f"{statement} LIMIT {limit}"


# Order matters: it is the order the dashboard shows the engines in, and the order
# the benchmark runs them in.  Cassandra first because it is the transactional
# path the other three are being contrasted with.
ENGINES = {
    "cassandra": (cassandra_client, sql_for_cassandra),
    "presto": (presto_client, sql_for_presto),
    "spark": (spark_client, sql_for_spark),
    "spark_bulk": (spark_bulk_client, sql_for_spark_bulk),
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
    start = time.perf_counter()
    try:
        rows = client.execute_query(statement)
        elapsed_ms = round((time.perf_counter() - start) * 1000, 1)
    except Exception as e:
        # Time the failure too.  A path refused in a millisecond because CQL cannot
        # express the question and a path that gave up after a quarter of an hour
        # are different findings, and without the clock they read alike.
        return EngineResult(
            sql=statement,
            error=str(e),
            query_time_ms=round((time.perf_counter() - start) * 1000, 1),
        )

    columns = list(rows[0].keys()) if rows else []
    return EngineResult(
        sql=statement,
        columns=columns,
        rows=[[r.get(c) for c in columns] for r in rows],
        row_count=len(rows),
        query_time_ms=elapsed_ms,
        # Only the bulk reader offers this, so it is asked for rather than required.
        bytes_scanned=getattr(client, "last_bytes_scanned", None),
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


# ──────────────────────── Comparing the four paths ────────────────────────

# One comparison at a time.  Two overlapping ones would each be timed while the
# other was running, and both sets of numbers would be wrong without saying so.
#
# What is running is recorded beside the lock, not just the fact that something is:
# a browser that gives up on a long run leaves it going here, so "already running"
# with no age, statement or progress reads like a stuck dashboard.  The Health page
# reads this, and can stop it.
_comparison_lock = threading.Lock()
_in_flight: Optional[Dict[str, Any]] = None
# Set by a cancel request, cleared when the run it stopped has returned.  A path
# already working is stopped by taking its connection away; this is what stops the
# paths that have not started yet.
_cancel_requested = threading.Event()

# How often the probe reads one partition while an engine works, and how long the
# baseline window is.  4 reads a second is far below what the dashboard's own
# polling already costs, so the probe does not itself become the noisy neighbour.
PROBE_INTERVAL_S = 0.25
BASELINE_WINDOW_S = 3.0


class _OltpProbe:
    """Read one partition, over and over, and keep the latencies.

    This is how the comparison shows what an analytical query costs the
    transactional path.  Every engine here reads the same single node, so an
    engine that scans the whole history is expected to be felt; the bulk reader
    is the one claiming not to, and this is what tests that claim.
    """

    def __init__(self, entity_id: str):
        self._entity_id = entity_id
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._latencies: List[float] = []
        self._failures = 0

    def __enter__(self) -> "_OltpProbe":
        self._thread = threading.Thread(target=self._loop, name="oltp-probe", daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *_exc: Any) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)

    def _loop(self) -> None:
        while not self._stop.is_set():
            start = time.perf_counter()
            try:
                cassandra_client.get_drone_detail(self._entity_id)
                self._latencies.append((time.perf_counter() - start) * 1000)
            except Exception:
                # A read that never came back is the most interesting outcome of
                # all, so count it rather than letting it end the probe.
                self._failures += 1
            self._stop.wait(PROBE_INTERVAL_S)

    def impact(self) -> OltpImpact:
        samples = sorted(self._latencies)
        if not samples:
            return OltpImpact(failures=self._failures)

        def at(fraction: float) -> float:
            index = min(len(samples) - 1, int(round(fraction * (len(samples) - 1))))
            return round(samples[index], 1)

        return OltpImpact(
            p50_ms=at(0.5),
            p95_ms=at(0.95),
            max_ms=round(samples[-1], 1),
            samples=len(samples),
            failures=self._failures,
        )


def _probe_subject() -> Optional[str]:
    """An asset to point-read, or None if Cassandra cannot be asked for one."""
    try:
        drones = cassandra_client.get_drones(limit=1)
    except Exception:
        return None
    return drones[0]["entity_id"] if drones else None


def _requested_engines(names: Optional[List[str]]) -> List[str]:
    """The paths to compare, in the dashboard's order, or raise 400.

    Ordering comes from ENGINES rather than from the request, so the columns do
    not move about depending on the order they were named in.
    """
    if names is None:
        return list(ENGINES)
    unknown = [name for name in names if name not in ENGINES]
    if unknown:
        raise HTTPException(status_code=400, detail=f"Unknown engine(s): {', '.join(unknown)}")
    chosen = [name for name in ENGINES if name in set(names)]
    if not chosen:
        raise HTTPException(status_code=400, detail="Choose at least one engine to compare")
    return chosen


def _run_sequentially(
    engines: List[str], statement: str, limit: int, subject: Optional[str],
    results: Dict[str, EngineResult],
) -> None:
    """One path at a time, each probed on its own, filling ``results`` as it goes.

    This is the mode whose timings mean what they look like: nothing else the
    dashboard controls is running, so a path's figure is its own cost, and the
    probe beside it is the price that one path charged the transactional path.

    Results are filled in rather than returned so that a run in flight can be
    watched: what is in the dict is what has answered.  A cancelled run stops
    before its next path, and the paths it never reached stay absent.
    """
    for engine in engines:
        if _cancel_requested.is_set():
            return
        if not subject:
            results[engine] = _run(engine, statement, limit)
            continue
        with _OltpProbe(subject) as probe:
            results[engine] = _run(engine, statement, limit)
        results[engine].oltp = probe.impact()


def _run_together(
    engines: List[str], statement: str, limit: int, results: Dict[str, EngineResult]
) -> None:
    """Every path at once, contending on purpose.

    Each path answers more slowly than it would alone, and that is the point: the
    comparison is being used to show interference rather than to avoid it.  Each
    path has its own client and its own connection, including the two Spark paths,
    so they genuinely overlap instead of queueing behind a shared session.

    There is nothing here for a cancel flag to prevent, since every path has
    already started; a cancel stops these by taking their connections away.
    """
    threads = []
    for engine in engines:
        def leg(engine: str = engine) -> None:
            try:
                results[engine] = _run(engine, statement, limit)
            except Exception as e:
                # _run reports failure in its result rather than raising, so this
                # is only reached if it fails outright.  Record it, because a
                # thread that died silently would drop the column altogether and
                # the comparison would look like it was never asked for.
                results[engine] = EngineResult(error=str(e))

        thread = threading.Thread(target=leg, name=f"compare-{engine}")
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()


@router.post("/benchmark", response_model=BenchmarkResponse)
def run_benchmark(req: BenchmarkRequest) -> BenchmarkResponse:
    """Run one logical question down the chosen paths, and report what each cost.

    Sequentially by default: a timing is then of one path rather than of several
    competing for one host, and the single-partition read sampled beside each one
    is the price that path charged the transactional path.  Asked to run in
    parallel, the paths contend deliberately, every figure inflates, and the probe
    becomes one measurement over the whole window, since while the paths overlap
    the cost belongs to all of them and to none in particular.

    Either way the same read is sampled just beforehand as a reference, and
    per-path failures are reported in the body rather than raised, so the
    comparison still renders when a path cannot answer — which for CQL and an
    aggregate is the point of showing it.
    """
    global _in_flight
    statement = _validate(req.sql)
    engines = _requested_engines(req.engines)

    if not _comparison_lock.acquire(blocking=False):
        running = running_comparison()
        age = int(running.running_for_s) if running else 0
        raise HTTPException(
            status_code=409,
            detail=f"A comparison has been running for {age}s.  They run one at a time, "
            "because two at once would each be timed while the other ran; a run whose "
            "browser gave up carries on here until it finishes.  The Health page shows "
            "it, and can stop it.",
        )

    results: Dict[str, EngineResult] = {}
    _cancel_requested.clear()
    _in_flight = {
        "started": time.monotonic(),
        "mode": req.mode,
        "engines": engines,
        "sql": statement,
        "results": results,
        # What each Spark path will submit, worked out here because a cancel has to
        # recognise those jobs among everything else the shared Thrift Server may be
        # running.  The dialects are pure rewrites, so this is what the legs issue.
        "spark_statements": [
            ENGINES[name][1](statement, req.limit)
            for name in engines
            if name in ("spark", "spark_bulk")
        ],
    }
    try:
        subject = _probe_subject()
        baseline = None
        if subject:
            with _OltpProbe(subject) as probe:
                time.sleep(BASELINE_WINDOW_S)
            baseline = probe.impact()

        combined = None
        if req.mode == "parallel":
            if subject:
                with _OltpProbe(subject) as probe:
                    _run_together(engines, statement, req.limit, results)
                combined = probe.impact()
            else:
                _run_together(engines, statement, req.limit, results)
        else:
            _run_sequentially(engines, statement, req.limit, subject, results)

        return BenchmarkResponse(
            mode=req.mode,
            oltp_baseline=baseline,
            oltp_combined=combined,
            cancelled=_cancel_requested.is_set(),
            **results,
        )
    finally:
        _in_flight = None
        _cancel_requested.clear()
        _comparison_lock.release()


def running_comparison() -> Optional[ComparisonRun]:
    """The comparison in flight, or None.  Read by the Health page."""
    run = _in_flight
    if run is None:
        return None
    answered = run["results"]
    return ComparisonRun(
        running_for_s=round(time.monotonic() - run["started"], 1),
        mode=run["mode"],
        engines=run["engines"],
        sql=run["sql"],
        done=[name for name in run["engines"] if name in answered],
    )


def cancel_comparison() -> List[str]:
    """Stop the comparison in flight, and report what that took.

    Three different mechanisms, because the paths are three different kinds of
    client: the paths that have not started are stopped by a flag, a Presto query
    is cancelled by the coordinator, and a Spark statement has its connection taken
    away because PyHive cannot cancel one it is already waiting on.  Cassandra is
    absent from the list on purpose: its legs are single-digit milliseconds, so
    there is never one to stop.

    The run itself ends the moment its paths stop; the HTTP request that started it
    gets an ordinary response marked cancelled, if anything is still listening.
    """
    run = _in_flight
    if run is None:
        return []

    _cancel_requested.set()
    actions = ["stopped the paths that had not started yet"]

    if "presto" in run["engines"]:
        try:
            killed = [
                query["id"]
                for query in presto_client.running_queries()
                if query["user"] == settings.presto_user
            ]
            for query_id in killed:
                presto_client.kill_query(query_id)
            if killed:
                actions.append(f"cancelled {len(killed)} Presto quer(y/ies): {', '.join(killed)}")
        except Exception as e:
            actions.append(f"could not cancel the Presto query: {e}")

    for name, client in (("spark", spark_client), ("spark_bulk", spark_bulk_client)):
        if name in run["engines"]:
            try:
                if client.abort():
                    actions.append(f"took the connection away from {name}")
            except Exception as e:
                actions.append(f"could not stop {name}: {e}")

    if run["spark_statements"]:
        # Both halves are needed.  The abort above stops the dashboard waiting; this
        # stops Spark working, which it otherwise carries on doing for a session that
        # has gone, keeping the cores the next comparison would be timed against.
        try:
            killed = spark_ui.kill_jobs_for(run["spark_statements"])
            if killed:
                actions.append(f"killed {len(killed)} Spark job(s): {', '.join(killed)}")
        except Exception as e:
            actions.append(f"could not kill the Spark job(s): {e}")

    return actions


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
