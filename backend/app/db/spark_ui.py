"""The Spark application UI's REST API — seeing and stopping running work.

The Thrift Server is one long-lived Spark application, so every job it runs is
visible at its application UI, whether the dashboard submitted it or somebody ran
spark-sql in the container.  This goes over HTTP rather than through the Thrift
connection on purpose: a connection busy with a query is exactly the connection
that cannot be asked about it.
"""
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

import httpx

from app.config import settings

# Short, because the Health page polls this: a Spark UI that is slow to answer
# should leave the page saying so rather than holding the whole response up.
TIMEOUT_S = 3.0

# A job description can be a whole statement; the page wants a line, not a plan.
DESCRIPTION_LIMIT = 300


def base_url() -> str:
    return f"http://{settings.spark_ui_host}:{settings.spark_app_ui_port}"


def application_id() -> Optional[str]:
    """The Thrift Server's application, or None if the UI cannot be reached.

    The UI lists one application per JVM and the Thrift Server is the only one
    here, so the first is the right one.
    """
    response = httpx.get(f"{base_url()}/api/v1/applications", timeout=TIMEOUT_S)
    response.raise_for_status()
    applications = response.json()
    return applications[0]["id"] if applications else None


def running_jobs() -> List[Dict[str, Any]]:
    """Jobs currently running in the Thrift Server's application.

    Jobs rather than SQL executions: the jobs endpoint filters server-side, while
    the SQL one returns every execution since start-up with its whole query plan
    attached, which is megabytes to answer a question about the present.
    """
    app = application_id()
    if not app:
        return []
    response = httpx.get(
        f"{base_url()}/api/v1/applications/{app}/jobs",
        params={"status": "running"},
        timeout=TIMEOUT_S,
    )
    response.raise_for_status()
    jobs = []
    for job in response.json():
        # description is the statement the Thrift Server set for the job group;
        # name is the Spark call site, which is all there is for a job submitted
        # any other way.
        text = job.get("description") or job.get("name") or "a Spark job"
        jobs.append(
            {
                "id": str(job.get("jobId")),
                "state": (job.get("status") or "RUNNING").lower(),
                "sql": " ".join(text.split())[:DESCRIPTION_LIMIT],
                "running_s": _age_s(job.get("submissionTime")),
                "tasks_total": job.get("numTasks") or 0,
                "tasks_done": job.get("numCompletedTasks") or 0,
            }
        )
    return jobs


def _age_s(submitted: Optional[str]) -> float:
    """How long ago Spark says the job was submitted, in seconds.

    Spark stamps these as "2026-08-17T12:01:52.041GMT", which is ISO-8601 with a
    zone abbreviation where an offset belongs, so the suffix is swapped before
    parsing.  An unparseable stamp gives 0 rather than hiding the job.
    """
    if not submitted:
        return 0.0
    try:
        moment = datetime.fromisoformat(submitted.replace("GMT", "+00:00"))
    except ValueError:
        return 0.0
    return round(max(0.0, (datetime.now(timezone.utc) - moment).total_seconds()), 1)


def label(statement: str) -> str:
    """A statement as the job list reports it, for matching one against the other."""
    return " ".join(statement.split())[:DESCRIPTION_LIMIT]


def kill_jobs_for(statements: Iterable[str]) -> List[str]:
    """Kill the running jobs that are working on any of these statements.

    Matched by statement rather than killing whatever is running, because the
    Thrift Server is one application shared by everything that connects to it: a
    spark-sql session in the container would otherwise be collateral damage when a
    dashboard comparison is cancelled.

    Killing is necessary and not merely tidy.  Taking a client's connection away
    stops the dashboard waiting, but Spark carries on: HiveServer2 does not notice
    a dropped session promptly, so the job keeps its share of the cores and the
    next comparison would be timed against an orphan.
    """
    wanted = {label(statement) for statement in statements}
    killed = []
    for job in running_jobs():
        if job["sql"] in wanted:
            kill_job(job["id"])
            killed.append(job["id"])
    return killed


def kill_job(job_id: str) -> None:
    """Ask the UI to kill one job, as its own kill link does.

    Depends on spark.ui.killEnabled, which is Spark's default and is left at it.
    The handler answers with a redirect to the jobs page, so any 2xx or 3xx means
    the request was accepted; whether the job dies is then Spark's business, and
    the next poll of running_jobs() is what confirms it.
    """
    response = httpx.post(
        f"{base_url()}/jobs/job/kill/",
        params={"id": job_id},
        timeout=TIMEOUT_S,
        follow_redirects=False,
    )
    if response.status_code >= 400:
        raise RuntimeError(
            f"the Spark UI refused to kill job {job_id} (HTTP {response.status_code}); "
            "spark.ui.killEnabled must be on"
        )
