"""
Ingestion trigger endpoints — initiate MRF download and processing jobs.
"""

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel

from app.core.logging import get_logger

router = APIRouter()
log = get_logger(__name__)

# Track simple in-memory job state (use Redis/Celery in prod)
_jobs: dict[str, dict] = {}


class IngestionRequest(BaseModel):
    market_id: str = "va"
    network_ids: list[str] | None = None  # None = all networks in market
    force_refresh: bool = False


class IngestionStatus(BaseModel):
    job_id: str
    status: str   # queued | running | complete | failed
    market_id: str
    files_processed: int
    records_loaded: int
    error: str | None


@router.post("/run", response_model=IngestionStatus, status_code=202)
async def trigger_ingestion(
    request: IngestionRequest,
    background_tasks: BackgroundTasks,
) -> IngestionStatus:
    """
    Trigger a background MRF ingestion job for the specified market.
    Returns a job ID to poll for status.
    """
    import uuid
    job_id = str(uuid.uuid4())

    _jobs[job_id] = {
        "status": "queued",
        "market_id": request.market_id,
        "files_processed": 0,
        "records_loaded": 0,
        "error": None,
    }

    background_tasks.add_task(
        _run_ingestion_job,
        job_id=job_id,
        market_id=request.market_id,
        network_ids=request.network_ids,
        force_refresh=request.force_refresh,
    )

    log.info("Ingestion job queued", job_id=job_id, market=request.market_id)
    return IngestionStatus(job_id=job_id, **_jobs[job_id])


@router.get("/status/{job_id}", response_model=IngestionStatus)
async def get_job_status(job_id: str) -> IngestionStatus:
    if job_id not in _jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    return IngestionStatus(job_id=job_id, **_jobs[job_id])


async def _run_ingestion_job(
    job_id: str,
    market_id: str,
    network_ids: list[str] | None,
    force_refresh: bool,
) -> None:
    """
    Background task: delegates to the packages/ingestion pipeline.
    In MVP this runs in-process; in production, offload to a worker queue.
    """
    import sys
    import os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../../packages/ingestion"))

    try:
        _jobs[job_id]["status"] = "running"
        from pipeline.run import run_market_ingestion  # type: ignore[import]
        stats = await run_market_ingestion(
            market_id=market_id,
            network_ids=network_ids,
            force_refresh=force_refresh,
        )
        _jobs[job_id].update({
            "status": "complete",
            "files_processed": stats.get("files_processed", 0),
            "records_loaded": stats.get("records_loaded", 0),
        })
        log.info("Ingestion complete", job_id=job_id, **stats)
    except Exception as e:
        _jobs[job_id].update({"status": "failed", "error": str(e)})
        log.error("Ingestion failed", job_id=job_id, error=str(e))
