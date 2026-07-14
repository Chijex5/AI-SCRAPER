from datetime import datetime, timezone
from enum import StrEnum
import asyncio, time

from fastapi import APIRouter, status, Response
from pydantic import BaseModel


class DepStatus(StrEnum):
    ok       = "ok"
    degraded = "degraded"
    down     = "down"


class DepResult(BaseModel):
    status:     DepStatus
    latency_ms: float
    detail:     str | None = None


class SchedulerJob(BaseModel):
    id:       str
    next_run: str | None
    running:  bool


class HealthResponse(BaseModel):
    status:       DepStatus
    version:      str
    timestamp:    str
    db_name:      str
    gemini_keys:  int
    dependencies: dict[str, DepResult]
    scheduler:    dict[str, SchedulerJob]


router = APIRouter(tags=["health"])


# ── Dependency probes ─────────────────────────────────────────────────────────

async def _probe_mongo() -> DepResult:
    """Ping the server and confirm both collections are accessible."""
    from main import db, DB_NAME
    t = time.monotonic()
    try:
        if db.client is None:
            raise RuntimeError("client not initialised")
        await db.client.admin.command("ping")
        # Confirm both collections are reachable
        await asyncio.gather(
            db.client[DB_NAME]["signals"].estimated_document_count(),
            db.client[DB_NAME]["notifications"].estimated_document_count(),
        )
        return DepResult(
            status=DepStatus.ok,
            latency_ms=round((time.monotonic() - t) * 1000, 2),
        )
    except Exception as exc:
        return DepResult(
            status=DepStatus.down,
            latency_ms=round((time.monotonic() - t) * 1000, 2),
            detail=str(exc),
        )


async def _probe_gemini() -> DepResult:
    """Confirm at least one Gemini key is loaded (no network call — avoids quota)."""
    from main import _gemini_clients
    t = time.monotonic()
    n = len(_gemini_clients)
    if n == 0:
        return DepResult(
            status=DepStatus.down,
            latency_ms=round((time.monotonic() - t) * 1000, 2),
            detail="no Gemini API keys configured",
        )
    return DepResult(
        status=DepStatus.ok,
        latency_ms=round((time.monotonic() - t) * 1000, 2),
        detail=f"{n} key{'s' if n != 1 else ''} in pool",
    )


async def _probe_telegram() -> DepResult:
    """
    Telegram connects per pipeline run rather than holding a persistent
    connection, so a live probe would mean opening a throwaway connection on
    every /health call (slow, rate-limit-prone). Instead, derive staleness
    from the most recent source_health entry recorded by record_source_health()
    during the last scrape run.
    """
    from main import db, DB_NAME, get_active_channels

    t = time.monotonic()
    try:
        channels = await get_active_channels()
        if not channels:
            return DepResult(
                status=DepStatus.ok,
                latency_ms=round((time.monotonic() - t) * 1000, 2),
                detail="no channels configured",
            )

        if db.client is None:
            raise RuntimeError("client not initialised")
        col = db.client[DB_NAME]["source_health"]
        last = await col.find_one({"source": "Telegram"}, sort=[("at", -1)])

        if last is None:
            return DepResult(
                status=DepStatus.degraded,
                latency_ms=round((time.monotonic() - t) * 1000, 2),
                detail="no recorded Telegram run yet",
            )

        if last["count"] == 0:
            return DepResult(
                status=DepStatus.degraded,
                latency_ms=round((time.monotonic() - t) * 1000, 2),
                detail=f"last run at {last['at']} returned 0 listings — session may be dead",
            )

        return DepResult(
            status=DepStatus.ok,
            latency_ms=round((time.monotonic() - t) * 1000, 2),
            detail=f"last run at {last['at']}: {last['count']} listings",
        )
    except Exception as exc:
        return DepResult(
            status=DepStatus.down,
            latency_ms=round((time.monotonic() - t) * 1000, 2),
            detail=str(exc),
        )


def _scheduler_jobs() -> dict[str, SchedulerJob]:
    from main import scheduler, scrape_state, notifications_state
    jobs: dict[str, SchedulerJob] = {}
    for job_id, running in [
        ("daily_scrape",         scrape_state["running"]),
        ("weekly_notifications", notifications_state["running"]),
    ]:
        job      = scheduler.get_job(job_id)
        next_run = None
        if job and job.next_run_time:
            next_run = job.next_run_time.isoformat()
        jobs[job_id] = SchedulerJob(
            id=job_id, next_run=next_run, running=running
        )
    return jobs


# ── Routes ────────────────────────────────────────────────────────────────────

@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Full readiness check",
)
async def health_check() -> HealthResponse:
    """Probe MongoDB + Gemini pool in parallel; aggregate overall status."""
    from main import DB_NAME, _gemini_clients

    mongo_result, gemini_result, telegram_result = await asyncio.gather(
        _probe_mongo(),
        _probe_gemini(),
        _probe_telegram(),
    )

    deps = {
        "mongodb":  mongo_result,
        "gemini":   gemini_result,
        "telegram": telegram_result,
    }

    if any(d.status == DepStatus.down     for d in deps.values()):
        overall = DepStatus.down
    elif any(d.status == DepStatus.degraded for d in deps.values()):
        overall = DepStatus.degraded
    else:
        overall = DepStatus.ok

    return HealthResponse(
        status       = overall,
        version      = "2.0.0",
        timestamp    = datetime.now(timezone.utc).isoformat(),
        db_name      = DB_NAME,
        gemini_keys  = len(_gemini_clients),
        dependencies = deps,
        scheduler    = _scheduler_jobs(),
    )

@router.head("/health", include_in_schema=False)
async def health_head():
    return Response(status_code=status.HTTP_200_OK)

@router.get("/livez", status_code=status.HTTP_200_OK, include_in_schema=False)
async def liveness():
    """Kubernetes liveness — just confirms the process is alive."""
    return {"status": "ok"}


@router.get("/readyz", include_in_schema=False)
async def readiness():
    """Kubernetes readiness — returns 503 if MongoDB is down."""
    result = await _probe_mongo()
    if result.status == DepStatus.down:
        from fastapi import HTTPException
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=result.detail or "mongodb is down",
        )