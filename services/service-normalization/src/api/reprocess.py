"""
Reprocessing endpoints for normalization service.

This module provides API endpoints for reprocessing historical data
through the normalization pipeline.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

router = APIRouter()


class ReprocessRequest(BaseModel):
    """Request model for reprocessing."""

    start_time: str
    end_time: str
    market: Optional[str] = None
    data_type: Optional[str] = None
    source_topic: str = "ingestion.*.raw.v1"
    target_topic: str = "normalized.market.ticks.v1"
    batch_size: int = 100
    parallel_workers: int = 4


class ReprocessJob(BaseModel):
    """Reprocessing job model."""

    job_id: str
    status: str  # pending, running, completed, failed, cancelled
    start_time: str
    end_time: str
    market: Optional[str]
    data_type: Optional[str]
    records_processed: int = 0
    records_failed: int = 0
    created_at: datetime
    updated_at: datetime
    error_message: Optional[str] = None


# In-memory job storage (in production, use database)
reprocess_jobs: Dict[str, ReprocessJob] = {}


@router.post("/reprocess")
async def start_reprocessing(request: ReprocessRequest) -> Dict[str, Any]:
    """
    Start a reprocessing job.

    Args:
        request: Reprocessing request

    Returns:
        Dict[str, Any]: Job information
    """
    try:
        # Create reprocessing job
        job_id = str(uuid4())
        job = ReprocessJob(
            job_id=job_id,
            status="pending",
            start_time=request.start_time,
            end_time=request.end_time,
            market=request.market,
            data_type=request.data_type,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc)
        )

        reprocess_jobs[job_id] = job

        # Start reprocessing task asynchronously
        asyncio.create_task(_execute_reprocessing_job(job_id, request))

        return {
            "job_id": job_id,
            "status": "pending",
            "message": "Reprocessing job created and started",
            "request": request.dict()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to start reprocessing: {str(e)}")


@router.get("/reprocess/{job_id}")
async def get_reprocessing_status(job_id: str) -> Dict[str, Any]:
    """
    Get status of a reprocessing job.

    Args:
        job_id: Job identifier

    Returns:
        Dict[str, Any]: Job status
    """
    if job_id not in reprocess_jobs:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

    job = reprocess_jobs[job_id]

    return {
        "job_id": job.job_id,
        "status": job.status,
        "start_time": job.start_time,
        "end_time": job.end_time,
        "market": job.market,
        "data_type": job.data_type,
        "records_processed": job.records_processed,
        "records_failed": job.records_failed,
        "created_at": job.created_at.isoformat(),
        "updated_at": job.updated_at.isoformat(),
        "error_message": job.error_message
    }


@router.get("/reprocess")
async def list_reprocessing_jobs(
    status: Optional[str] = Query(None, description="Status filter"),
    limit: int = Query(50, description="Maximum number of results")
) -> Dict[str, Any]:
    """
    List reprocessing jobs.

    Args:
        status: Status filter
        limit: Maximum number of results

    Returns:
        Dict[str, Any]: List of jobs
    """
    try:
        jobs = list(reprocess_jobs.values())

        # Filter by status
        if status:
            jobs = [j for j in jobs if j.status == status]

        # Sort by created_at descending
        jobs.sort(key=lambda j: j.created_at, reverse=True)

        # Apply limit
        jobs = jobs[:limit]

        return {
            "jobs": [
                {
                    "job_id": j.job_id,
                    "status": j.status,
                    "start_time": j.start_time,
                    "end_time": j.end_time,
                    "market": j.market,
                    "records_processed": j.records_processed,
                    "records_failed": j.records_failed,
                    "created_at": j.created_at.isoformat()
                }
                for j in jobs
            ],
            "count": len(jobs),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list jobs: {str(e)}")


@router.post("/reprocess/{job_id}/cancel")
async def cancel_reprocessing_job(job_id: str) -> Dict[str, Any]:
    """
    Cancel a reprocessing job.

    Args:
        job_id: Job identifier

    Returns:
        Dict[str, Any]: Cancellation result
    """
    if job_id not in reprocess_jobs:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

    job = reprocess_jobs[job_id]

    if job.status not in ["pending", "running"]:
        raise HTTPException(status_code=400, detail=f"Job {job_id} cannot be cancelled (status: {job.status})")

    # Update job status
    job.status = "cancelled"
    job.updated_at = datetime.now(timezone.utc)

    return {
        "job_id": job_id,
        "status": "cancelled",
        "message": "Reprocessing job cancelled"
    }


async def _execute_reprocessing_job(job_id: str, request: ReprocessRequest) -> None:
    """
    Execute a reprocessing job.

    Args:
        job_id: Job identifier
        request: Reprocessing request
    """
    from ..main import normalization_service

    job = reprocess_jobs[job_id]

    try:
        # Update job status
        job.status = "running"
        job.updated_at = datetime.now(timezone.utc)

        # Simulate reprocessing (in production, consume from Kafka and reprocess)
        # This would:
        # 1. Connect to Kafka
        # 2. Seek to offset based on start_time
        # 3. Consume messages until end_time
        # 4. Normalize each message
        # 5. Publish to target topic
        # 6. Track progress

        # For now, just simulate completion
        await asyncio.sleep(5)  # Simulate processing time

        # Update job status
        job.status = "completed"
        job.records_processed = 100  # Simulated
        job.updated_at = datetime.now(timezone.utc)

        logging.info(f"Reprocessing job {job_id} completed successfully")

    except Exception as e:
        # Update job status on failure
        job.status = "failed"
        job.error_message = str(e)
        job.updated_at = datetime.now(timezone.utc)

        logging.error(f"Reprocessing job {job_id} failed: {e}")